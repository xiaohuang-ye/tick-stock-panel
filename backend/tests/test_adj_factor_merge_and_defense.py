"""除权因子合并语义 + 前复权非稀疏防御的回归测试。

背景 (2026-09-09 中天科技 0813 数据事故):
- sync_adj_factor 旧 upsert 合并从不删除窗口内无新事件的行, 导致伪造的
  "每日快照/累计因子"残渣永久残留; _apply_adj_factor 对其 cum_prod 会把
  历史前复权价指数级压成 ~0。
- 修复: _merge_factor_window 按窗口"先删后插"; _apply_adj_factor 识别
  非稀疏(连续交易日密集)序列并跳过其复权, 价保原始。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import polars as pl

from app.indicators.pipeline import _apply_adj_factor
from app.services.kline_sync import _merge_factor_window

W0 = datetime(2026, 8, 1)
W1 = datetime(2026, 9, 8)


def _adj(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={"symbol": pl.Utf8, "trade_date": pl.Date, "ex_factor": pl.Float64},
    )


def test_merge_factor_window_replaces_affected_in_window():
    """受影响 symbol 在窗口内的旧行被剔除后并入新行; 窗口外/未受影响行保留。"""
    existing = _adj([
        # 伪造的每日残渣 (08-03~09-01 连续)
        {"symbol": "600522.SH", "trade_date": date(2026, 8, 3), "ex_factor": 6.2},
        {"symbol": "600522.SH", "trade_date": date(2026, 8, 13), "ex_factor": 6.16},
        {"symbol": "600522.SH", "trade_date": date(2026, 9, 1), "ex_factor": 6.1},
        # 未受影响 symbol 的行 (无新事件) 应原样保留
        {"symbol": "600519.SH", "trade_date": date(2026, 8, 3), "ex_factor": 1.01},
    ])
    new_data = _adj([
        # 提供方本次只返回 600522 的真实稀疏事件
        {"symbol": "600522.SH", "trade_date": date(2026, 8, 19), "ex_factor": 1.03},
    ])
    merged = _merge_factor_window(existing, new_data, ["600522.SH"], W0, W1)

    assert merged["symbol"].n_unique() == 2
    # 600522 只保留本次真实事件, 每日残渣被清出窗口
    sym_rows = merged.filter(pl.col("symbol") == "600522.SH")
    assert sym_rows.height == 1
    assert sym_rows["trade_date"][0] == date(2026, 8, 19)
    # 未受影响 symbol 的窗口内行不被误删
    others = merged.filter(pl.col("symbol") == "600519.SH")
    assert others.height == 1
    assert others["trade_date"][0] == date(2026, 8, 3)


def test_merge_factor_window_none_window_replaces_whole_symbol():
    """start/end 均 None (全量拉取) → 受影响 symbol 全量替换, 未受影响保留。"""
    existing = _adj([
        {"symbol": "600522.SH", "trade_date": date(2025, 12, 3), "ex_factor": 9.9},
        {"symbol": "600522.SH", "trade_date": date(2026, 8, 13), "ex_factor": 6.16},
        {"symbol": "600519.SH", "trade_date": date(2026, 8, 13), "ex_factor": 1.0},
    ])
    new_data = _adj([
        {"symbol": "600522.SH", "trade_date": date(2026, 7, 15), "ex_factor": 1.006},
    ])
    merged = _merge_factor_window(existing, new_data, ["600522.SH"], None, None)

    sym_rows = merged.filter(pl.col("symbol") == "600522.SH")
    assert sym_rows.height == 1
    assert sym_rows["trade_date"][0] == date(2026, 7, 15)
    # 未受影响 symbol 保留
    assert "600519.SH" in merged["symbol"].to_list()
    assert merged["symbol"].n_unique() == 2


def test_merge_factor_window_empty_new_keeps_existing():
    """新数据为空(限流/无事件)时 affected 为空 → 现有表原样保留, 不误删。"""
    existing = _adj([
        {"symbol": "600522.SH", "trade_date": date(2026, 8, 13), "ex_factor": 6.16},
    ])
    merged = _merge_factor_window(existing, pl.DataFrame(), [], W0, W1)
    assert merged.height == existing.height
    assert merged["trade_date"][0] == date(2026, 8, 13)


def _raw(close: float, day: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": ["600522.SH"],
            "date": [date(2026, 8, day)],
            "open": [close],
            "high": [close],
            "low": [close],
            "close": [close],
        }
    )


def test_apply_adj_factor_skips_dense_per_day_factor():
    """连续交易日密集(每日快照)因子序列 → 跳过复权, 价保原始, 不再被压成 ~0。"""
    start = date(2026, 8, 3)
    dense = [
        {"symbol": "600522.SH", "trade_date": start + timedelta(days=i),
         "ex_factor": 6.16}
        for i in range(22)  # 22 个"连续交易日"每日因子
    ]
    raw = _raw(32.9, 13)
    out = _apply_adj_factor(raw, _adj(dense))
    assert out["close"][0] == 32.9  # 未被 cum_prod 压成 ~0


def test_apply_adj_factor_applies_sparse_event():
    """真实稀疏单事件比值 → 正常前复权 (事件日前按因子下调, 事件日及之后 = 原价)。"""
    raw = _raw(11.0, 5)
    factors = _adj([
        {"symbol": "600522.SH", "trade_date": date(2026, 8, 7), "ex_factor": 1.1},
    ])
    # 8-05 在事件(8-07)之前 → 前复权下调 1.1 倍
    out = _apply_adj_factor(raw, factors)
    assert abs(out["close"][0] - 10.0) < 1e-9