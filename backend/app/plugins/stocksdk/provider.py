"""stock-sdk 内置数据源 provider。

Original implementation by @forrany (PR #57), migrated to plugin architecture.
核心抓取/归一化逻辑保留原作者实现, 仅调整 import 路径与注册方式。

通过 bridge.mjs 调真实 stock-sdk 抓 A 股行情, 归一化到项目内部 schema。
方法签名对齐 custom.GenericHTTPProvider(service 分流点按这套签名调用),
因此注入 custom loader 注册表后, 各 service 无需改动即可路由到本 provider。
"""
from __future__ import annotations

import logging
import math
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from datetime import time as dtime

import polars as pl

from app.data_providers.base import AssetType, UpstreamThrottledError
from app.data_providers.normalizer import normalize_adj_factors, normalize_daily
from app.plugins.stocksdk import bridge
from app.tickflow.rate_limits import chunked

logger = logging.getLogger(__name__)

# stock-sdk 支持的数据集(financial 不支持 → 不声明, 自动回退 tickflow)
_DATASETS = ("daily", "adj_factor", "minute", "realtime")

# 每次桥接调用的符号数。桥接内部按 concurrency 并发, 分批仅为进度反馈与超时控制。
_BATCH = 40
# 批内逐 symbol 抓取的并发数(透传给 bridge.mjs)。对东财免费接口的限流风险随
# 此值与 _PARALLEL_BRIDGE 线性上升, 实测大量空结果/失败批次时应下调。
_CONCURRENCY = 12
# 同时在飞的桥接子进程数(每批各自 spawn node, 相互无共享状态)。
_PARALLEL_BRIDGE = 2
# 单批桥接超时(秒)。批大小 40、批内并发 12 后单批最耗时约 2 分钟, 留一倍余量。
_TIMEOUT_DAILY = 360
_TIMEOUT_ADJ = 480
_TIMEOUT_MINUTE = 360
# ex_factor 合理上限。真实除权因子几十年累积也在千级以内; 上游行情错位时会算出
# 1e26 级脏值(实测), 写入 enriched 后会让 polars f64→i64 严格转换崩溃,
# 必须在数据边界丢弃。
_EX_FACTOR_LIMIT = 1e6
# 熔断: 连续 N 批完全空结果(限流的典型形态)即中止剩余批次并抛 UpstreamThrottledError。
# 实测被限流时每批仍要 1~2 分钟, 不熔断会空转近一小时且零数据返回, 还与「无新增」混淆。
_EMPTY_BREAK_CHUNKS = 4
# 熔断仅对窗口含近 N 天的拉取启用: 历史补数据(extend_history 拉早年)窗口里大量
# 当日未上市标的空返回是正常形态, 不能据以判定限流。
_EMPTY_BREAK_RECENT_DAYS = 7

_MINUTE_CANONICAL = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]


def _sane_ex_factor(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    v = float(value)
    return math.isfinite(v) and 0.0 < v <= _EX_FACTOR_LIMIT


def _is_recent_window(end_time: datetime | None) -> bool:
    if end_time is None:
        return True
    return (datetime.now() - end_time).days <= _EMPTY_BREAK_RECENT_DAYS


def _execute_chunks(
    chunks: list[list],
    worker: Callable[[list], list[pl.DataFrame]],
    on_chunk_done: Callable[[int, int], None] | None,
    empty_break_after: int | None = None,
) -> list[pl.DataFrame]:
    """以 _PARALLEL_BRIDGE 并发执行各批桥接调用, 返回全部非空帧。

    每批是独立 node 子进程, 批间无共享状态; 进度按「已完成批数」单调回调。
    某批抛出(含任务取消经回调传播)时置停止标记: 未开始的批次直接跳过,
    已在飞批次等待自然结束(与原串行行为一致)。
    empty_break_after: 连续 N 批完全空结果(限流形态)时置停止标记, 等在飞
    批次结束后抛 UpstreamThrottledError; None = 不启用。
    """
    total = len(chunks)
    frames: list[pl.DataFrame] = []
    if not total:
        return frames
    state = {"done": 0, "empty_streak": 0, "broken_streak": 0}
    lock = threading.Lock()
    cancelled = threading.Event()

    def _run_one(chunk) -> list[pl.DataFrame]:
        if cancelled.is_set():
            return []
        part = worker(chunk)
        # 计数与回调同锁, 保证多批并行时进度回调仍单调递增
        with lock:
            state["done"] += 1
            if part:
                state["empty_streak"] = 0
            elif empty_break_after:
                state["empty_streak"] += 1
                if state["empty_streak"] >= empty_break_after:
                    state["broken_streak"] = state["empty_streak"]
                    cancelled.set()
            if on_chunk_done:
                on_chunk_done(state["done"], total)
        return part

    max_workers = max(1, min(_PARALLEL_BRIDGE, total))
    pool = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = [pool.submit(_run_one, chunk) for chunk in chunks]
        for fut in as_completed(futures):
            try:
                frames.extend(fut.result())
            except BaseException:
                cancelled.set()
                raise
    finally:
        pool.shutdown(wait=True)
    if state["broken_streak"]:
        raise UpstreamThrottledError(
            f"连续 {state['broken_streak']} 批空结果(上游限流形态), 中止剩余 {total - state['done']} 批"
        )
    return frames


@dataclass
class _StockSDKConfig:
    """轻量 config shim, 让 custom loader 的 list_sources/provider_has_dataset 能识别本 provider。"""

    name: str = "stocksdk"
    display_name: str = "stock-sdk（免费行情）"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def _yyyymmdd(dt: datetime | None) -> str | None:
    return dt.strftime("%Y%m%d") if dt else None


class StockSDKProvider:
    """内置 stock-sdk 数据源。"""

    name = "stocksdk"
    builtin = True
    # 分钟历史深度能力(可选声明, 未声明视为深历史): stock-sdk 免费分时接口
    # 只保留最近 5 个交易日的 1 分钟数据, 分时档位/默认值据此收窄。
    minute_history_days = 5

    def __init__(self) -> None:
        self.config = _StockSDKConfig()

    def close(self) -> None:  # loader.load_all 会对每个 provider 调 close
        pass

    # ---- daily ----
    def get_daily(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",  # noqa: ARG002
        on_chunk_done=None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        logger.info("stock-sdk daily 拉取开始(%d symbols)", len(symbols))

        def worker(chunk: list[str]) -> list[pl.DataFrame]:
            job = {
                "op": "daily",
                "symbols": chunk,
                "adjust": "none",
                "start": _yyyymmdd(start_time),
                "end": _yyyymmdd(end_time),
                "concurrency": _CONCURRENCY,
            }
            try:
                result = bridge.run_job(job, timeout=_TIMEOUT_DAILY)
            except bridge.StockSDKBridgeError as e:
                logger.warning("stock-sdk daily 拉取失败(%d symbols): %s", len(chunk), e)
                return []
            frames: list[pl.DataFrame] = []
            for sym, rows in (result.get("rows") or {}).items():
                if not rows:
                    continue
                df = normalize_daily(rows, default_symbol=sym, source=self.name)
                if not df.is_empty():
                    frames.append(df)
            return frames

        frames = _execute_chunks(chunked(symbols, _BATCH), worker, on_chunk_done)
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    # ---- adj_factor ----
    def get_adj_factors(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: str = "stock",  # noqa: ARG002
        on_chunk_done=None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        # 熔断仅对含近几天的窗口启用(历史补数据窗口的空返回是正常形态)
        breaker = _EMPTY_BREAK_CHUNKS if _is_recent_window(end_time) else None

        def worker(chunk: list[str]) -> list[pl.DataFrame]:
            job = {
                "op": "adj",
                "symbols": chunk,
                "start": _yyyymmdd(start_time),
                "end": _yyyymmdd(end_time),
                "concurrency": _CONCURRENCY,
            }
            try:
                result = bridge.run_job(job, timeout=_TIMEOUT_ADJ)
            except bridge.StockSDKBridgeError as e:
                logger.warning("stock-sdk adj 拉取失败(%d symbols): %s", len(chunk), e)
                return []
            flat: list[dict] = []
            for rows in (result.get("rows") or {}).values():
                flat.extend(rows or [])
            if not flat:
                return []
            sane = [r for r in flat if _sane_ex_factor(r.get("ex_factor"))]
            if len(sane) < len(flat):
                logger.warning(
                    "stock-sdk adj 丢弃 %d 条异常除权因子(非有限/<=0/>%.0e), 防止污染 enriched",
                    len(flat) - len(sane), _EX_FACTOR_LIMIT,
                )
            if not sane:
                return []
            df = normalize_adj_factors(sane, source=self.name)
            return [df] if not df.is_empty() else []

        frames = _execute_chunks(chunked(symbols, _BATCH), worker, on_chunk_done,
                                 empty_break_after=breaker)
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    # ---- minute ----
    def get_minute(
        self,
        symbols: list[str],
        start_time: datetime | None,
        end_time: datetime | None,
        asset_type: AssetType = "stock",  # noqa: ARG002
        freq: str = "1m",
        on_chunk_done: Callable[[int, int], None] | None = None,
    ) -> pl.DataFrame:
        if not symbols:
            return pl.DataFrame()
        period = "".join(ch for ch in str(freq) if ch.isdigit()) or "1"
        logger.info("stock-sdk minute 拉取开始(%d symbols, period=%s)", len(symbols), period)

        # 上游区间查询的分钟 open 为日级常量(伪值), 单日查询才给最新交易日真实
        # 分钟 open → 末尾 3 个自然日逐日单拉(跳过周末, 覆盖周五收盘后场景),
        # 其余历史段仍走单个区间任务控制桥接成本。伪 open 由 _null_degenerate_opens
        # 在 _minute_df 内置 null。
        windows: list[tuple[datetime | None, datetime | None]] = []
        if (
            start_time is not None
            and end_time is not None
            and end_time.date() > start_time.date()
        ):
            tail_start = end_time.date() - timedelta(days=3)
            if tail_start > start_time.date():
                head_end = datetime.combine(tail_start - timedelta(days=1), dtime.max)
                windows.append((start_time, head_end))
            else:
                tail_start = start_time.date()
            day = tail_start
            while day <= end_time.date():
                if day.weekday() < 5:
                    windows.append((
                        datetime.combine(day, dtime.min),
                        datetime.combine(day, dtime.max),
                    ))
                day += timedelta(days=1)
        else:
            windows.append((start_time, end_time))

        # 窗口 × 批次展开成任务列表, 由 _execute_chunks 并行跑, 进度按
        # 已完成任务数单调回调(与原串行的 step/total 口径一致)。
        tasks = [
            (win_start, win_end, chunk)
            for win_start, win_end in windows
            for chunk in chunked(symbols, _BATCH)
        ]

        def worker(task) -> list[pl.DataFrame]:
            win_start, win_end, chunk = task
            job = {
                "op": "minute",
                "symbols": chunk,
                "period": period,
                "start": _yyyymmdd(win_start),
                "end": _yyyymmdd(win_end),
                "concurrency": _CONCURRENCY,
            }
            try:
                result = bridge.run_job(job, timeout=_TIMEOUT_MINUTE)
            except bridge.StockSDKBridgeError as e:
                logger.warning("stock-sdk minute 拉取失败(%d symbols): %s", len(chunk), e)
                return []
            out: list[pl.DataFrame] = []
            for sym, rows in (result.get("rows") or {}).items():
                df = self._minute_df(rows, sym)
                if not df.is_empty():
                    out.append(df)
            return out

        frames = _execute_chunks(tasks, worker, on_chunk_done)
        # 末窗口(最新一日)的真实 open 与首窗口可能重叠同日(时区/边界), keep="last"
        # 由上层 _write_minute_partition 的 unique 处理; 这里仅拼接。
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    @staticmethod
    def _null_degenerate_opens(df: pl.DataFrame) -> pl.DataFrame:
        """把"日级常量"的伪分钟 open 置 null。

        stock-sdk 上游对历史日的分钟 open 只给全天常量(如涨跌停价/日开),
        并非真实分钟开盘价; 只有最新交易日在单日查询下给真实值。伪 open 入库
        会让分钟K的 close-vs-open 口径全偏(如分时量恒红), 故按日检测:
        同日 rows>10 且 open 唯一值<=3 而 close 唯一值>10 → open 判定非分钟级,
        置 null(fail-closed, 不伪造 prev_close 替代)。
        """
        if df.is_empty() or "open" not in df.columns or "datetime" not in df.columns:
            return df
        stats = df.group_by(pl.col("datetime").dt.date()).agg(
            pl.len().alias("n"),
            pl.col("open").n_unique().alias("uo"),
            pl.col("close").n_unique().alias("uc"),
        )
        fake_dates = stats.filter(
            (pl.col("n") > 10) & (pl.col("uo") <= 3) & (pl.col("uc") > 10)
        )["datetime"]
        if fake_dates.is_empty():
            return df
        logger.warning(
            "stock-sdk minute open 为日级常量, 置 null: %s %s",
            df["symbol"][0] if "symbol" in df.columns else "?", fake_dates.to_list(),
        )
        return df.with_columns(
            pl.when(pl.col("datetime").dt.date().is_in(fake_dates))
            .then(None)
            .otherwise(pl.col("open"))
            .alias("open")
        )

    @staticmethod
    def _minute_df(rows: list[dict], symbol: str) -> pl.DataFrame:
        if not rows:
            return pl.DataFrame()
        df = pl.DataFrame(rows)
        # bridge 分钟行含 timestamp(ms, UTC 基准)。A 股分时按北京时间墙钟展示,
        # 故转 Asia/Shanghai 后去掉时区得到 naive 北京时间(如 09:35)。
        if "timestamp" in df.columns:
            df = df.with_columns(
                pl.from_epoch(pl.col("timestamp").cast(pl.Int64), time_unit="ms")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone("Asia/Shanghai")
                .dt.replace_time_zone(None)
                .cast(pl.Datetime("us"))
                .alias("datetime")
            )
        elif "date" in df.columns:
            df = df.with_columns(
                pl.col("date").str.to_datetime("%Y-%m-%d %H:%M", strict=False).alias("datetime")
            )
        df = df.with_columns(pl.lit(symbol).alias("symbol"))
        for col in ("open", "high", "low", "close", "volume", "amount"):
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
        df = StockSDKProvider._null_degenerate_opens(df)
        keep = [c for c in _MINUTE_CANONICAL if c in df.columns]
        return df.select(keep) if "datetime" in keep else pl.DataFrame()

    # ---- realtime (全市场快照) ----
    def get_realtime(self) -> list[dict]:
        logger.info("stock-sdk realtime 拉取开始(全市场快照)")
        try:
            result = bridge.run_job({"op": "realtime"}, timeout=120)
        except bridge.StockSDKBridgeError as e:
            logger.warning("stock-sdk realtime 拉取失败: %s", e)
            return []
        rows = result.get("rows") or []
        normalized: list[dict] = []
        for row in rows:
            item = dict(row)
            # stock-sdk 的 changePercent 是百分数值(-1.15 = -1.15%);
            # provider 入口契约统一使用小数制(-0.0115 = -1.15%)。
            if item.get("change_pct") is not None:
                item["change_pct"] = float(item["change_pct"]) / 100
            # stock-sdk 全量实时行情的 amount 单位为万元;内部日K统一使用元。
            if item.get("amount") is not None:
                item["amount"] = float(item["amount"]) * 10_000
            normalized.append(item)
        return normalized

    # ---- instruments (标的维表) ----
    def get_instruments(self, asset_type: str = "stock") -> list[dict]:
        """返回 tickflow Instrument 形状的行(symbol/name/code/exchange/region/type + ext),

        供 instrument_sync._flatten_instruments 复用同一 flatten 路径, 列结构与 tickflow 一致。
        当前覆盖 A 股股票。
        """
        if asset_type != "stock":
            return []
        try:
            result = bridge.run_job({"op": "instruments"}, timeout=120)
        except bridge.StockSDKBridgeError as e:
            logger.warning("stock-sdk instruments 拉取失败: %s", e)
            return []
        return result.get("rows") or []

    # ---- 测试(设置页试拉) ----
    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        symbols = symbols or ["600519.SH"]
        if dataset == "daily":
            df = self.get_daily(symbols, None, None)
            return _preview("daily", df)
        if dataset == "adj_factor":
            df = self.get_adj_factors(symbols, None, None)
            return _preview("adj_factor", df)
        if dataset == "minute":
            df = self.get_minute(symbols, None, None)
            return _preview("minute", df)
        if dataset == "realtime":
            rows = self.get_realtime()
            head = rows[:5]
            return {
                "provider": self.name,
                "dataset": "realtime",
                "rows": len(rows),
                "columns": list(head[0].keys()) if head else [],
                "preview": head,
            }
        raise ValueError(f"stock-sdk 不支持数据集: {dataset}")


def _preview(dataset: str, df: pl.DataFrame) -> dict:
    return {
        "provider": "stocksdk",
        "dataset": dataset,
        "rows": df.height,
        "columns": df.columns,
        "preview": df.head(5).to_dicts() if not df.is_empty() else [],
    }