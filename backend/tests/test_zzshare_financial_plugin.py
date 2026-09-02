"""zzshare 插件财务 provider 单测。

不依赖真实网络: 用假 api (query 记录调用并返回固定行) 注入 provider._api。
"""

from __future__ import annotations

from app.plugins.zzshare import provider as zz


def _indicator_row(code="000001.SZ", stat="2026-06-30", pub="2026-08-15") -> dict:
    return {
        "code": code,
        "statDate": stat,
        "pubDate": pub,
        "roe": 2.05,
        "gross_profit_margin": 40.1,
        "net_profit_margin": 31.62,
        "inc_revenue_year_on_year": -0.94,
        "inc_net_profit_year_on_year": 3.7,
        "eps": 0.5758,
        "adjusted_profit": 11145000000.0,
    }


def _income_row(code="000001.SZ", stat="2026-06-30", pub="2026-08-15") -> dict:
    return {
        "code": code,
        "statDate": stat,
        "pubDate": pub,
        "operating_revenue": 35340000000.0,
        "net_profit": 11173000000.0,
    }


class _FakeApi:
    """记录 query 调用, 按路径返回固定行列。"""

    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.paths: list[tuple[str, dict]] = []
        self.latest_codes: list[str] = []
        self.stock_codes: list[str] = []

    def query(self, path: str, params: dict | None = None) -> list[dict]:
        params = params or {}
        self.paths.append((path, params))
        if "/latest" in path:
            self.latest_codes.append(params.get("codes", ""))
            return self.rows
        # finance_stock: 返回行须归属请求的个股
        code = path.rsplit("/", 1)[-1]
        self.stock_codes.append(code)
        return [dict(row, code=code) for row in self.rows]


def _provider(rows: list[dict]) -> zz.ZzshareProvider:
    p = zz.ZzshareProvider()
    p._api = _FakeApi(rows)
    return p


def test_metrics_latest_maps_factor_columns():
    p = _provider([_indicator_row()])

    df = p.get_financials("metrics", ["000001.SZ"], latest_only=True)

    assert df["symbol"].to_list() == ["000001.SZ"]
    assert df["period_end"].to_list() == ["2026-06-30"]
    assert df["announce_date"].to_list() == ["2026-08-15"]
    # zzshare indicator 字段对齐到回测因子列
    assert df["roe"].to_list() == [2.05]
    assert df["gross_margin"].to_list() == [40.1]
    assert df["net_margin"].to_list() == [31.62]
    assert df["revenue_yoy"].to_list() == [-0.94]
    assert df["net_income_yoy"].to_list() == [3.7]
    # zzshare 不提供的因子列补齐为 null (保证快照校验通过)
    assert df["bps"].is_null().all()
    assert df["debt_to_asset_ratio"].is_null().all()
    assert {"symbol", "announce_date"} | set(zz._FACTOR_COLUMNS) <= set(df.columns)


def test_income_latest_batches_codes():
    p = _provider([_income_row(), _income_row("600519.SH")])

    df = p.get_financials("income", ["000001.SZ", "600519.SH"], latest_only=True)

    assert p._api.paths == [("v3/fundamentals/income/latest", {"codes": "000001.SZ,600519.SH"})]
    assert df["symbol"].to_list() == ["000001.SZ", "600519.SH"]
    assert df["period_end"].to_list() == ["2026-06-30", "2026-06-30"]
    assert df["operating_revenue"].to_list() == [35340000000.0, 35340000000.0]
    assert {"symbol", "period_end", "announce_date", "operating_revenue", "net_profit"} <= set(
        df.columns
    )


def test_latest_batches_by_chunk(monkeypatch):
    monkeypatch.setattr(zz, "_LATEST_BATCH", 2)
    p = _provider([_income_row()])

    p.get_financials("income", ["000001.SZ", "600519.SH", "000002.SZ"], latest_only=True)

    assert p._api.latest_codes == ["000001.SZ,600519.SH", "000002.SZ"]


def test_history_uses_per_symbol_stock(monkeypatch):
    monkeypatch.setattr(zz, "_HISTORY_SLEEP_SEC", 0)  # 加速单测
    p = _provider([_income_row()])

    df = p.get_financials("income", ["000001.SZ", "600519.SH"], latest_only=False)

    assert [path for path, _ in p._api.paths] == [
        "v3/fundamentals/income/stock/000001.SZ",
        "v3/fundamentals/income/stock/600519.SH",
    ]
    assert p._api.stock_codes == ["000001.SZ", "600519.SH"]
    assert df["symbol"].to_list() == ["000001.SZ", "600519.SH"]


def test_shares_table_returns_empty():
    p = zz.ZzshareProvider()

    df = p.get_financials("shares", ["000001.SZ"], latest_only=False)

    assert df.is_empty()
    assert p._api is None  # 未触发对 zzshare 的任何请求


def test_empty_rows_returns_empty():
    p = _provider([])

    df = p.get_financials("income", ["000001.SZ"], latest_only=True)

    assert df.is_empty()


def test_test_dataset_financial_preview():
    p = _provider([_indicator_row()])

    res = p.test_dataset("financial")

    assert res["provider"] == "zzshare"
    assert res["dataset"] == "financial"
    assert res["rows"] == 1
    assert "roe" in res["columns"]
    assert res["preview"][0]["symbol"] == "000001.SZ"


def test_test_dataset_unsupported_returns_error():
    p = zz.ZzshareProvider()

    res = p.test_dataset("daily")

    assert res["rows"] == 0
    assert "error" in res


def test_availability_returns_tuple():
    ok, reason = zz.availability()
    assert isinstance(ok, bool)
    assert isinstance(reason, str)
