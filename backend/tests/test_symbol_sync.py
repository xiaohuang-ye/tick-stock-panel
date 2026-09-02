"""按股票同步 (run_symbols_sync) 与代码规范化测试。"""
import pytest

from app.jobs import daily_pipeline
from app.tickflow.capabilities import Cap, CapabilityLimits, CapabilitySet
from app.tickflow.repository import DataStore, KlineRepository

SYMBOLS = ["600519.SH", "000001.SZ"]


@pytest.fixture()
def repo(tmp_path):
    return KlineRepository(DataStore(tmp_path))


def _capset(*caps: Cap) -> CapabilitySet:
    return CapabilitySet({cap: CapabilityLimits() for cap in caps})


@pytest.fixture()
def _patch_pipeline_io(monkeypatch, tmp_path):
    """隔离 run_symbols_sync 的副作用: 数据源函数与视图/缓存刷新全部换桩。"""
    calls: dict = {}

    def fake_daily(symbols, repo, capset, count=None, start_date=None, end_date=None,
                   on_chunk_done=None):
        calls["daily"] = {"symbols": symbols, "start": start_date, "end": end_date}
        return 12

    def fake_adj(symbols, repo, capset, start_time=None, end_time=None,
                 on_chunk_done=None, asset_type="stock"):
        calls["adj"] = {"symbols": symbols, "start": start_time}
        return 3, list(symbols)

    def fake_run_pipeline(*args, **kwargs):
        calls["enriched"] = kwargs
        return 48

    monkeypatch.setattr(daily_pipeline.kline_sync, "sync_and_persist_daily_batch", fake_daily)
    monkeypatch.setattr(daily_pipeline.kline_sync, "sync_adj_factor", fake_adj)
    monkeypatch.setattr(daily_pipeline, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(daily_pipeline, "_refresh_single_view", lambda *a, **k: None)
    monkeypatch.setattr(daily_pipeline, "_refresh_views", lambda *a, **k: None)
    monkeypatch.setattr(daily_pipeline, "_invalidate", lambda *a, **k: None)
    monkeypatch.setattr(daily_pipeline._prefs, "get_adj_factor_provider", lambda: "tickflow")
    monkeypatch.setattr(daily_pipeline._prefs, "get_daily_data_provider", lambda: "tickflow")
    return calls


def test_run_symbols_sync_passes_symbols_and_window(repo, _patch_pipeline_io):
    """日K/除权收到指定 symbols 与 [today-days, today] 窗口; enriched 按个股局部重算。"""
    from datetime import date

    calls = _patch_pipeline_io
    stages: list[tuple[str, int]] = []
    result = daily_pipeline.run_symbols_sync(
        repo, _capset(Cap.KLINE_DAILY_BATCH, Cap.ADJ_FACTOR), SYMBOLS,
        days=15, on_progress=lambda stage, pct, msg, **k: stages.append((stage, pct)),
    )

    assert calls["daily"]["symbols"] == SYMBOLS
    assert calls["daily"]["end"].date() == date.today()
    assert (calls["daily"]["end"].date() - calls["daily"]["start"].date()).days == 15
    assert calls["adj"]["symbols"] == SYMBOLS
    assert calls["enriched"]["symbols"] == SYMBOLS
    assert calls["enriched"]["new_dates_only"] is True

    assert result == {
        "symbols": 2,
        "daily_rows": 12,
        "adj_factor_rows": 3,
        "adj_factor_symbols": 2,
        "enriched_rows": 48,
        "skipped_stages": [],
    }
    assert stages[0] == ("sync_daily", 5)
    assert stages[-1] == ("done", 100)


def test_run_symbols_sync_skips_adj_without_capability(repo, _patch_pipeline_io, monkeypatch):
    """无 ADJ_FACTOR 能力 → 跳过除权阶段并计入 skipped_stages, 不中断后续 enriched。"""
    calls = _patch_pipeline_io

    def _unexpected(*a, **k):
        raise AssertionError("sync_adj_factor should not be called")

    monkeypatch.setattr(daily_pipeline.kline_sync, "sync_adj_factor", _unexpected)

    result = daily_pipeline.run_symbols_sync(
        repo, _capset(Cap.KLINE_DAILY_BATCH), SYMBOLS, days=10,
    )
    assert result["adj_factor_rows"] == 0
    assert result["skipped_stages"] == ["sync_adj"]
    assert calls["enriched"]["symbols"] == SYMBOLS


def test_run_symbols_sync_empty_symbols_raises(repo, _patch_pipeline_io):
    with pytest.raises(ValueError):
        daily_pipeline.run_symbols_sync(repo, _capset(Cap.KLINE_DAILY_BATCH), [])


def test_normalize_symbol_inputs():
    """6 位裸代码按首位规则补后缀; 显式后缀/前缀优先; 去重保序; 非法输入进 bad 列表。"""
    from app.api.kline import _normalize_symbol_inputs

    ok, bad = _normalize_symbol_inputs([
        "600519",          # 沪市裸代码
        "000001",          # 深市裸代码
        "sh600519",        # 前缀
        "600519.SH",       # 已带后缀 (与前两项去重合并)
        "sz300750",        # 创业板
        "920001",          # 北交所 92 开头
        "430047",          # 北交所 4 开头
        "510300",          # 沪市 ETF 5 开头
        "abc",             # 无 6 位数字
        "",                # 空串忽略
    ])
    assert ok == [
        "600519.SH",
        "000001.SZ",
        "300750.SZ",
        "920001.BJ",
        "430047.BJ",
        "510300.SH",
    ]
    assert bad == ["abc"]
