"""zzshare 免费 A 股量化数据插件 — 财务数据提供方。

接入方式: Python 型插件 (runtime: python), 依赖 zzshare 包 (pip install zzshare)。
数据源为 zzshare API (https://api.zizizaizai.com), 默认 anonymous token 免费可用,
无需注册申请; 可选设置环境变量 ZZSHARE_TOKEN 提升配额(SDK 自动读取)。

本插件只声明 financial 数据集 → provider_has_dataset("zzshare", "financial") = True,
financial_sync 自动绕过 TickFlow FINANCIAL 能力门控, 改调本 provider 的 get_financials。
未声明的数据集(日K/分钟/实时)仍然回退 TickFlow。

表映射(内部表 → zzshare 表, 路径见 zzshare DataApi.SHORTCUTS):
  metrics       → indicator       财务指标
  income        → income          利润表
  balance_sheet → balance         资产负债表
  cash_flow     → cash_flow       现金流量表
  shares        → (无对应表)       返回空, 上游正常记录 0 行

返回行统一携带 code(=内部 symbol) / statDate(=period_end) / pubDate(=announce_date),
满足 financial_sync 的 (symbol, period_end) 去重与公告日口径。

因子列对齐(metrics 须满足 backtest/fundamentals.load_fundamental_snapshot 的 7 列,
缺 1 列整个财务快照失效):
  - zzshare indicator 提供 roe / gross_profit_margin / net_profit_margin /
    inc_revenue_year_on_year / inc_net_profit_year_on_year → 对齐到
    roe / gross_margin / net_margin / revenue_yoy / net_income_yoy;
  - bps(每股净资产) 与 debt_to_asset_ratio(资产负债率) zzshare 不提供 → 显式补 null
    列保证快照校验通过; 这两个下游因子(市净率/资产负债率)数据全 null, 自动剔除。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

import polars as pl

logger = logging.getLogger(__name__)

# 只声明真实提供的数据集; 其余数据集(日K/分钟/实时) provider_has_dataset = False → 回退 tickflow
_DATASETS = ("financial",)

# 内部财务表 → zzshare 表名; shares 无对应实现
_ZZSHARE_TABLES = {
    "metrics": "indicator",
    "income": "income",
    "balance_sheet": "balance",
    "cash_flow": "cash_flow",
}

# 通用列名对齐 (zzshare → 内部契约)
_RENAME = {"code": "symbol", "statDate": "period_end", "pubDate": "announce_date"}

# metrics(indicator) 额外字段名对齐到回测因子列
_METRICS_RENAME = {
    "gross_profit_margin": "gross_margin",
    "net_profit_margin": "net_margin",
    "inc_revenue_year_on_year": "revenue_yoy",
    "inc_net_profit_year_on_year": "net_income_yoy",
}

# 回测财务快照要求齐全的因子列 (backtest/fundamentals.py load_fundamental_snapshot)
_FACTOR_COLUMNS = (
    "bps",
    "roe",
    "gross_margin",
    "net_margin",
    "revenue_yoy",
    "net_income_yoy",
    "debt_to_asset_ratio",
)

# 免费档限流保守值: 首次回填逐股拉历史时的调用间隔(秒)
_HISTORY_SLEEP_SEC = 0.12
# finance_latest 单次 codes 数量上限(保守)
_LATEST_BATCH = 50


def availability() -> tuple[bool, str]:
    """loader 启动自检: zzshare 包已安装才注册为可切换数据源。不抛异常。"""
    try:
        import zzshare  # noqa: F401
    except ImportError:
        return False, "未安装 zzshare, 运行: pip install zzshare"
    return True, "ok"


class ZzshareProvider:
    """zzshare 财务数据源。仅财务表走 zzshare, 其余数据集自动回退 TickFlow。"""

    name = "zzshare"
    builtin = True

    def __init__(self) -> None:
        self.config = _ZzshareConfig()
        self._api = None  # 惰性创建 zzshare.pro_api() (anonymous / ZZSHARE_TOKEN)

    def close(self) -> None:  # loader.load_all 重建注册表时调用
        self._api = None

    # ---- 设置页试拉 ----
    def test_dataset(self, dataset: str, symbols: list[str] | None = None) -> dict:
        if dataset != "financial":
            return {
                "provider": self.name,
                "dataset": dataset,
                "rows": 0,
                "error": f"{self.name} 插件未接入该数据集(自动回退 TickFlow)",
            }
        try:
            rows = self._get_api().query(
                "v3/fundamentals/indicator/latest",
                {"codes": ",".join(symbols or ["000001.SZ"])},
            )
            out = self._to_frame(rows, "indicator")
        except Exception as e:
            return {"provider": self.name, "dataset": dataset, "rows": 0, "error": str(e)}
        return {
            "provider": self.name,
            "dataset": dataset,
            "rows": len(out),
            "columns": list(out.columns),
            "preview": out.head(5).to_dicts() if not out.is_empty() else [],
        }

    # ---- 财务数据 ----
    def get_financials(
        self, table: str, symbols: list[str], latest_only: bool = True
    ) -> pl.DataFrame:
        """按内部财务表契约拉取并规范化。

        - latest_only=True: finance_latest 批量(每批少量请求, 增量同步用);
        - latest_only=False: finance_stock 逐股全历史(一次性回填用, 免费档限流垫间隔)。
        """
        zz_table = _ZZSHARE_TABLES.get(table)
        if zz_table is None or not symbols:
            return pl.DataFrame()  # shares 无对应 zzshare 表: 空表, 不报错
        api = self._get_api()

        frames: list[pl.DataFrame] = []
        if latest_only:
            for codes in _chunk(symbols, _LATEST_BATCH):
                df = self._fetch_latest(api, zz_table, codes)
                if not df.is_empty():
                    frames.append(df)
        else:
            for symbol in symbols:
                df = self._fetch_history(api, zz_table, symbol)
                if not df.is_empty():
                    frames.append(df)
                if _HISTORY_SLEEP_SEC:
                    time.sleep(_HISTORY_SLEEP_SEC)
        return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    def _fetch_latest(self, api, zz_table: str, codes: list[str]) -> pl.DataFrame:
        try:
            rows = api.query(f"v3/fundamentals/{zz_table}/latest", {"codes": ",".join(codes)})
        except Exception as e:
            logger.warning("zzshare %s/latest (%d) 拉取失败: %s", zz_table, len(codes), e)
            return pl.DataFrame()
        return self._to_frame(rows, zz_table)

    def _fetch_history(self, api, zz_table: str, symbol: str) -> pl.DataFrame:
        try:
            rows = api.query(f"v3/fundamentals/{zz_table}/stock/{symbol}", {})
        except Exception as e:
            logger.warning("zzshare %s/stock %s 拉取失败: %s", zz_table, symbol, e)
            return pl.DataFrame()
        return self._to_frame(rows, zz_table)

    def _to_frame(self, rows, zz_table: str) -> pl.DataFrame:
        """zzshare 原始行(list[dict]) → 内部规范化 polars 表。空返回空表。"""
        if not rows:
            return pl.DataFrame()
        frame = pl.DataFrame(rows).rename(_RENAME)
        if zz_table == "indicator":
            frame = frame.rename(_METRICS_RENAME)
        if "symbol" not in frame.columns:
            return pl.DataFrame()
        head = [c for c in ("symbol", "period_end", "announce_date") if c in frame.columns]
        rest = [c for c in frame.columns if c not in head]
        frame = frame.select(head + rest)
        frame = _cast_dates(frame)
        if zz_table == "indicator":
            frame = _align_factor_columns(frame)
        return frame

    def _get_api(self):
        if self._api is None:
            import zzshare

            # token 留空 → SDK 回退 ZZSHARE_TOKEN 环境变量或 anonymous(免费可用)
            self._api = zzshare.pro_api()
        return self._api


@dataclass
class _ZzshareConfig:
    """轻量 config shim, 让 custom loader 的 provider_has_dataset 识别本 provider。"""

    name: str = "zzshare"
    display_name: str = "zzshare"
    datasets: dict = field(default_factory=lambda: dict.fromkeys(_DATASETS))
    path: None = None
    builtin: bool = True


def _chunk(items: list[str], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _cast_dates(frame: pl.DataFrame) -> pl.DataFrame:
    """period_end / announce_date 统一为字符串(pubDate 罕见缺失时保持 null)。"""
    for col in ("period_end", "announce_date"):
        if col in frame.columns:
            frame = frame.with_columns(pl.col(col).cast(pl.Utf8, strict=False))
    return frame


def _align_factor_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """补齐回测财务因子 7 列并统一 Float64 (缺失的 bps/资产负债率补 null)。"""
    missing = [col for col in _FACTOR_COLUMNS if col not in frame.columns]
    if missing:
        frame = frame.with_columns([pl.lit(None, dtype=pl.Float64).alias(col) for col in missing])
    for col in _FACTOR_COLUMNS:
        frame = frame.with_columns(pl.col(col).cast(pl.Float64, strict=False))
    return frame
