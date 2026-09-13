#!/usr/bin/env python
"""一次性重建 adj_factor/all.parquet：用扶摇稀疏事件 dump 重建"单事件比值"因子表。

背景（2026-09-09 中天科技 0813 数据事故）：
- 曾有写入方（stock-sdk hit_worker hfq/none 基线合成）在每个交易日产出 ex_factor
  = close_hfq/close_none（后复权累计比值），并因 sync_adj_factor 的 upsert 合并
  永久残留；前复权 _apply_adj_factor 对其 cum_prod，历史价被指数级压成 ~0。
- 当前 data/adj_factor/all.parquet 几乎全部是 2026-08-03~09-08 的每日残渣，
  真实除权事件历史不在表中。

本脚本从本地扶摇事件 dump 重建稀疏事件因子表（前置收盘价取自本地 kline_daily，
完全离线、确定性），修复后 enriched 全量重算可得正确前复权价。

用法（从 backend/ 目录运行）：
    .venv/bin/python -m scripts.rebuild_adj_factor --dry-run   # 只扫描+统计，不写盘
    .venv/bin/python -m scripts.rebuild_adj_factor             # 备份后重建 all.parquet
可重复执行（幂等）。
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # backend/

from app.parquet import scan_daily_parquet  # noqa: E402
from app.plugins.fuyao.provider import (  # noqa: E402
    _price_limit,
    _ref_price,
    FuyaoProvider,
)

BACKEND = Path(__file__).resolve().parents[1]
# 运行时 DATA_DIR 默认./data 解析到项目根（backend/../data）；这里同样取项目根，
# 不依赖启动环境变量（.env 里的 DATA_DIR=./data 即此路径）。
DATA_DIR = BACKEND.parent / "data"
ADJ_OUT = DATA_DIR / "adj_factor" / "all.parquet"
DAILY_GLOB = str(DATA_DIR / "kline_daily" / "**" / "*.parquet")


def _load_local_closes() -> pl.DataFrame:
    """本地 kline_daily 原始收盘 (symbol, date, close)，一次扫描全体分区。"""
    lf = scan_daily_parquet(DAILY_GLOB)
    df = lf.select(["symbol", "date", "close"]).collect()
    return (
        df.with_columns(
            pl.col("date").cast(pl.Date, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
        )
        .filter(pl.col("close").is_finite() & (pl.col("close") > 0))
        .sort(["symbol", "date"])
    )


def _build_factor_table(events: pl.DataFrame, closes: pl.DataFrame) -> pl.DataFrame:
    """事件 → 单事件比值因子表（与 fuyao get_adj_factors 同语义，前置收盘取自本地）。"""
    close_by_sym: dict[str, dict[date, float]] = {}
    for g in closes.partition_by("symbol"):
        sym = g["symbol"][0]
        close_by_sym[sym] = dict(
            zip(g["date"].to_list(), g["close"].to_list(), strict=True)
        )

    out_rows: list[dict] = []
    skipped_no_close = 0
    skipped_bad_ref = 0
    skipped_limit = 0
    t0 = time.perf_counter()
    syms = sorted(events["symbol"].unique().to_list())

    for i, sym in enumerate(syms):
        evs = (
            events.filter(pl.col("symbol") == sym)
            .sort("ex_date")
            .select("ex_date", "dividend", "bonus", "allot", "allot_price")
        )
        closes_sym = close_by_sym.get(sym)
        if not closes_sym:
            skipped_no_close += evs.height
            continue
        days = sorted(closes_sym)
        for ev in evs.iter_rows(named=True):
            exd: date = ev["ex_date"]
            prev = [d for d in days if d < exd]
            if not prev:
                skipped_no_close += 1
                continue
            p = closes_sym[prev[-1]]
            ref = _ref_price(p, ev["dividend"], ev["bonus"], ev["allot"], ev["allot_price"])
            if ref is None or ref <= 0:
                skipped_bad_ref += 1
                continue
            factor = p / ref
            ex_days = [d for d in days if d >= exd]
            if ex_days:
                ret = closes_sym[ex_days[0]] / (p / factor) - 1.0
                if abs(ret) > _price_limit(sym) + 0.02:
                    skipped_limit += 1
                    continue
            out_rows.append(
                {"symbol": sym, "trade_date": exd, "ex_factor": factor}
            )
        if (i + 1) % 500 == 0:
            print(f"  … {i + 1}/{len(syms)} 标的, {len(out_rows)} 事件因子, "
                  f"{time.perf_counter() - t0:.0f}s")

    out = pl.DataFrame(out_rows, schema={"symbol": pl.Utf8, "trade_date": pl.Date, "ex_factor": pl.Float64})
    print(f"完成: {len(syms)} 标的 → {out.height} 事件行 "
          f"(无前收盘 {skipped_no_close}, 参考价异常 {skipped_bad_ref}, 涨跌停自检剔除 {skipped_limit})")
    return out.sort(["symbol", "trade_date"])


def main() -> None:
    ap = argparse.ArgumentParser(description="重建 adj_factor/all.parquet")
    ap.add_argument("--dry-run", action="store_true", help="只扫描+统计，不写盘")
    args = ap.parse_args()

    provider = FuyaoProvider()
    dump = provider._ensure_dump("adjustment-factors", "adj_factors")

    # 事件清洗完全复用扶摇 _load_adj_events（全零过滤/同日合并/配股无价过滤/ex_date<=今天）
    symset = set(dump["thscode"].unique().to_list())
    events = provider._load_adj_events(symset, None, None)
    print(f"事件 dump: {dump.height} 行 / {len(symset)} 标的 → 清洗后 {events.height} 事件")

    closes = _load_local_closes()
    print(f"本地 kline_daily 收盘: {closes.height} 行, "
          f"{closes['symbol'].n_unique()} 标的, {closes['date'].min()} ~ {closes['date'].max()}")

    factors = _build_factor_table(events, closes)

    # 稀疏性自检：不应存在"相邻因子行日期间隔 ≤3 天"的连续簇
    consec = (
        factors.sort(["symbol", "trade_date"])
        .with_columns(
            pl.col("trade_date").diff().dt.total_days().over("symbol").alias("_gap"),
        )
        .filter(pl.col("_gap").is_not_null() & (pl.col("_gap") <= 3))
        .group_by("symbol").agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)
    )
    if not consec.is_empty():
        bad = consec.sort("n", descending=True)
        sample = bad.head(15)
        print("⚠ 重建后仍有相邻密集因子行(非稀疏)，请检查：")
        print(sample)
    else:
        print("✓ 稀疏性自检通过：无相邻密集因子行")

    if args.dry_run:
        print("--dry-run：未写入。预计写入行数:", factors.height)
        return

    if not ADJ_OUT.exists():
        print(f"未找到现表 {ADJ_OUT}，直接写入新表")
    else:
        bak = ADJ_OUT.with_name("all.parquet.bak-before-rebuild")
        shutil.copy2(ADJ_OUT, bak)
        print(f"已备份旧表 → {bak}")

    tmp = ADJ_OUT.with_name("all.parquet.rebuild.tmp")
    factors.write_parquet(tmp)
    tmp.replace(ADJ_OUT)
    print(f"已写入 {ADJ_OUT}: {factors.height} 行")
    print("下一步：清空 data/kline_daily_enriched/ 并调 rebuild_enriched / run_pipeline 全量重算。")


if __name__ == "__main__":
    main()