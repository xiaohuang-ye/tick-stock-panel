"""StockSDKProvider 归一化与桥接契约测试。

不依赖真实 node / 网络: mock bridge.run_job 返回样例 payload, 只验证 Python 侧的
归一化、除权因子合成对齐、符号回显、空结果处理与注册接线。
"""
from __future__ import annotations

import datetime as dt
import json
import shutil
import subprocess
import threading
import time

import polars as pl
import pytest

from app.plugins.stocksdk import bridge
from app.plugins.stocksdk import provider as sp
from app.plugins.stocksdk.provider import StockSDKProvider


def _patch_run_job(monkeypatch, mapping):
    """mapping: op -> payload dict(将作为 run_job 返回值)。"""

    def fake(job, timeout=None):
        return mapping[job["op"]]

    monkeypatch.setattr(sp.bridge, "run_job", fake)


def test_get_daily_normalizes_and_echoes_symbol(monkeypatch):
    _patch_run_job(monkeypatch, {
        "daily": {"ok": True, "op": "daily", "rows": {
            "600519.SH": [
                {"date": "2026-01-05", "open": 1385.0, "high": 1431.9, "low": 1385.0,
                 "close": 1426.0, "volume": 70949, "amount": 1.0e10, "code": "600519"},
                {"date": "2026-01-06", "open": 1432.5, "high": 1437.0, "low": 1416.5,
                 "close": 1428.0, "volume": 39586, "amount": 5.6e9, "code": "600519"},
            ],
        }},
    })
    df = StockSDKProvider().get_daily(["600519.SH"], dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 15))
    assert df.columns == ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
    assert df.height == 2
    assert df["symbol"].unique().to_list() == ["600519.SH"]
    assert df.schema["date"] == pl.Date
    assert df.schema["close"] == pl.Float64


def test_get_adj_factors_from_bridge_ratio(monkeypatch):
    # 桥接内部已算好 ex_factor = close_hfq/close_none, 这里验证 Python 侧归一化。
    _patch_run_job(monkeypatch, {
        "adj": {"ok": True, "op": "adj", "rows": {
            "600519.SH": [
                {"symbol": "600519.SH", "trade_date": "2020-01-02", "ex_factor": 5.29},
                {"symbol": "600519.SH", "trade_date": "2020-01-03", "ex_factor": 5.30},
            ],
        }},
    })
    df = StockSDKProvider().get_adj_factors(["600519.SH"], None, None)
    assert df.columns == ["symbol", "trade_date", "ex_factor"]
    assert df.height == 2
    assert df.schema["trade_date"] == pl.Date
    assert abs(df["ex_factor"][0] - 5.29) < 1e-9


def test_get_minute_datetime_is_beijing_wall_clock(monkeypatch):
    # timestamp 1779327300000 = 2026-05-21 01:35 UTC = 09:35 Asia/Shanghai
    _patch_run_job(monkeypatch, {
        "minute": {"ok": True, "op": "minute", "rows": {
            "600519.SH": [
                {"date": "2026-05-21 09:35", "open": 1284.9, "high": 1289.1, "low": 1283.9,
                 "close": 1286.7, "volume": 2740, "amount": 3.6e8, "timestamp": 1779327300000},
            ],
        }},
    })
    df = StockSDKProvider().get_minute(["600519.SH"], None, None)
    assert set(df.columns) == {"symbol", "datetime", "open", "high", "low", "close", "volume", "amount"}
    assert df.height == 1
    ts = df["datetime"][0]
    assert (ts.hour, ts.minute) == (9, 35)
    assert df["symbol"][0] == "600519.SH"


def test_get_realtime_normalizes_units_without_mutating_bridge_rows(monkeypatch):
    rows = [{"symbol": "600519.SH", "name": "贵州茅台", "last_price": 1200.0,
             "prev_close": 1194.0, "open": 1186.0, "high": 1203.0, "low": 1180.0,
             "volume": 16325, "amount": 159095, "change_pct": -1.15,
             "timestamp": 1787193740000}]
    _patch_run_job(monkeypatch, {"realtime": {"ok": True, "op": "realtime", "rows": rows}})
    out = StockSDKProvider().get_realtime()
    assert abs(out[0]["change_pct"] - (-0.0115)) < 1e-12
    assert out[0]["amount"] == 1_590_950_000
    assert out[0]["timestamp"] == 1787193740000
    assert rows[0]["change_pct"] == -1.15
    assert rows[0]["amount"] == 159095
    required = {"symbol", "last_price", "prev_close", "open", "high", "low", "volume"}
    assert required <= set(out[0].keys())


def test_get_instruments_flatten_compatible(monkeypatch):
    rows = [{"symbol": "600519.SH", "name": "贵州茅台", "code": "600519", "exchange": "SH",
             "region": "CN", "type": "stock", "total_shares": 1, "float_shares": 1,
             "limit_up": 1.0, "limit_down": 1.0}]
    _patch_run_job(monkeypatch, {"instruments": {"ok": True, "op": "instruments", "rows": rows}})
    out = StockSDKProvider().get_instruments("stock")
    assert out[0]["symbol"] == "600519.SH"
    assert out[0]["exchange"] == "SH"
    # 非 stock 资产暂不覆盖
    assert StockSDKProvider().get_instruments("etf") == []


def test_empty_symbols_returns_empty():
    p = StockSDKProvider()
    assert p.get_daily([], None, None).is_empty()
    assert p.get_adj_factors([], None, None).is_empty()
    assert p.get_minute([], None, None).is_empty()


def test_bridge_error_degrades_to_empty(monkeypatch):
    def boom(job, timeout=None):
        raise sp.bridge.StockSDKBridgeError("node missing")

    monkeypatch.setattr(sp.bridge, "run_job", boom)
    assert StockSDKProvider().get_daily(["600519.SH"], None, None).is_empty()
    assert StockSDKProvider().get_realtime() == []
    assert StockSDKProvider().get_instruments("stock") == []


def test_bridge_uses_utf8_error_tolerant_subprocess(monkeypatch):
    calls = []

    class Result:
        returncode = 0
        stdout = json.dumps({"ok": True, "op": "ping"})
        stderr = ""

    monkeypatch.setattr(bridge, "_node_bin", lambda: "node")

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))
        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert bridge.run_job({"op": "ping"})["ok"] is True
    kwargs = calls[0][1]
    assert kwargs["encoding"] == "utf-8"
    assert kwargs["errors"] == "replace"


def test_bridge_mjs_resolves_local_sdk_and_maps_realtime_timestamp(tmp_path):
    if shutil.which("node") is None:
        raise AssertionError("node is required for stock-sdk bridge path regression test")

    bridge_path = tmp_path / "bridge.mjs"
    shutil.copyfile(bridge._BRIDGE_MJS, bridge_path)

    pkg_dir = tmp_path / "node_modules" / "stock-sdk"
    pkg_dir.mkdir(parents=True)
    (pkg_dir / "package.json").write_text(
        json.dumps({"name": "stock-sdk", "type": "module", "main": "index.js"}),
        encoding="utf-8",
    )
    (pkg_dir / "index.js").write_text(
        """export class StockSDK {
  static version = 'fake-local'
  constructor() {
    this.batch = { cn: async () => [{
      code: '600519', marketId: '1', name: '贵州茅台', price: 1200,
      prevClose: 1194, open: 1186, high: 1203, low: 1180,
      volume: 16325, amount: 159095, changePercent: 0.5,
      timestamp: 1787193740000
    }] }
  }
}
""",
        encoding="utf-8",
    )

    proc = subprocess.run(
        ["node", str(bridge_path)],
        input=json.dumps({"op": "ping"}),
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=20,
    )

    assert proc.returncode == 0
    result = json.loads(proc.stdout)
    assert result == {"ok": True, "op": "ping", "version": "fake-local"}
    realtime_proc = subprocess.run(
        ["node", str(bridge_path)],
        input=json.dumps({"op": "realtime"}),
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=20,
    )
    assert realtime_proc.returncode == 0
    row = json.loads(realtime_proc.stdout)["rows"][0]
    assert row["timestamp"] == 1787193740000


def test_plugin_discovered_in_loader():
    """插件被发现并记录状态 (即使依赖没装, 不可用)。"""
    from app.data_providers import custom as cs

    plugins = {p["name"]: p for p in cs.list_plugins()}
    assert "stocksdk" in plugins
    assert plugins["stocksdk"]["runtime"] == "node"
    assert "daily" in plugins["stocksdk"]["datasets"]
    assert "realtime" in plugins["stocksdk"]["datasets"]
    assert "financial" not in plugins["stocksdk"]["datasets"]
    assert cs.is_builtin("stocksdk")
    # 内置源不出现在用户自定义源列表
    assert "stocksdk" not in [s["name"] for s in cs.list_sources()]


def test_plugin_registered_when_available(monkeypatch):
    """依赖可用时, 插件注册进 _PROVIDERS 并可路由。"""
    from app.data_providers import custom as cs
    from app.data_providers.custom import loader

    # mock availability 返回 (True, "ok")
    monkeypatch.setattr(loader, "_call_check", lambda ref: (True, "ok"))
    monkeypatch.setattr(loader, "_load_entry", _load_stocksdk_entry)
    loader._load_builtin_plugins()

    assert "stocksdk" in cs.names()
    assert cs.is_custom_provider("stocksdk")
    assert cs.provider_has_dataset("stocksdk", "daily")
    assert cs.provider_has_dataset("stocksdk", "realtime")
    assert not cs.provider_has_dataset("stocksdk", "financial")


def _load_stocksdk_entry(entry_ref: str):
    """测试用: 无条件加载 stocksdk provider 类 (跳过 check)。"""
    if "StockSDKProvider" in entry_ref:
        from app.plugins.stocksdk.provider import StockSDKProvider
        return StockSDKProvider
    if "availability" in entry_ref:
        from app.plugins.stocksdk.bridge import availability
        return availability
    raise ValueError(f"unknown entry: {entry_ref}")


def test_builtin_not_editable():
    from app.data_providers import custom as cs

    assert cs.get_config_dict("stocksdk") is None
    for fn in (lambda: cs.save_config("stocksdk", {}), lambda: cs.delete_config("stocksdk")):
        try:
            fn()
            raise AssertionError("expected ValueError for builtin")
        except ValueError:
            pass


# ---------- 分钟 open 数据卫生 (stock-sdk 上游区间模式给日级常量伪 open) ----------

def _sdk_rows(day: str, opens, closes):
    """构造 bridge 返回形状的分钟行 (timestamp 为北京墙钟对应 UTC 毫秒)。"""
    from datetime import UTC, datetime, timedelta
    rows = []
    for i, (o, c) in enumerate(zip(opens, closes, strict=True)):
        dt = datetime.fromisoformat(f"{day} 09:30:00") + timedelta(minutes=i)
        ts = int(dt.replace(tzinfo=UTC).timestamp() * 1000) - 8 * 3600_000
        rows.append({"timestamp": ts, "open": o, "high": max(o, c), "low": min(o, c),
                     "close": c, "volume": 100 + i, "amount": 1000.0 + i})
    return rows


def test_minute_degenerate_open_nulled():
    """历史日 open 为全天常量(uniq=1)而 close 多值 → open 置 null。"""
    n = 30
    rows = _sdk_rows("2026-08-27", [8.0] * n, [10 + i * 0.01 for i in range(n)])
    df = StockSDKProvider._minute_df(rows, "600664.SH")
    assert df.height == n
    assert df["open"].null_count() == n          # 伪 open 全部置 null
    assert df["close"].null_count() == 0         # close/high/low 保留


def test_minute_real_open_kept():
    """真实分钟 open (多唯一值) 原样保留。"""
    n = 30
    rows = _sdk_rows("2026-08-28", [10 + i * 0.01 for i in range(n)], [10.05 + i * 0.01 for i in range(n)])
    df = StockSDKProvider._minute_df(rows, "600664.SH")
    assert df["open"].null_count() == 0
    assert df["open"].n_unique() == n


def test_minute_short_day_open_kept():
    """短交易日 (rows<=10, 如半日/首日少量bar) 不误杀。"""
    rows = _sdk_rows("2026-08-26", [8.0] * 6, [8.0 + i * 0.1 for i in range(6)])
    df = StockSDKProvider._minute_df(rows, "600664.SH")
    assert df["open"].null_count() == 0


def test_get_minute_splits_tail_into_single_day_jobs(monkeypatch):
    """多日区间 → 末尾 3 自然日(跳过周末)逐日单拉 (保住最新交易日真实 open)。"""
    from datetime import datetime

    jobs = []

    def fake_run_job(job, timeout=None):
        jobs.append(job)
        return {"ok": True, "op": "minute", "rows": {}}

    monkeypatch.setattr(sp.bridge, "run_job", fake_run_job)
    p = StockSDKProvider()
    p.get_minute(["600519.SH"], datetime(2026, 8, 20), datetime(2026, 8, 29, 23, 0))

    # 末尾 4 个自然日 (08-26..08-29) 逐日单拉, 周六 08-29 跳过;
    # 前段 = [08-20, 08-25] 一个区间任务
    spans = [(j["start"], j["end"]) for j in jobs]
    for day in ("20260826", "20260827", "20260828"):
        assert (day, day) in spans
    assert ("20260829", "20260829") not in spans          # 周六不单拉
    assert ("20260820", "20260825") in spans


def test_get_minute_single_day_no_split(monkeypatch):
    """单日区间不拆分, 保持一个任务。"""
    from datetime import datetime

    jobs = []

    def fake_run_job(job, timeout=None):
        jobs.append(job)
        return {"ok": True, "op": "minute", "rows": {}}

    monkeypatch.setattr(sp.bridge, "run_job", fake_run_job)
    StockSDKProvider().get_minute(["600519.SH"], datetime(2026, 8, 28), datetime(2026, 8, 28, 15, 0))
    assert len(jobs) == 1
    assert jobs[0]["start"] == "20260828"


# ===== 并行桥 + 熔断 + 脏值守卫 (upstream 简版上的本地增强重套管) =====


def _daily_payload(job):
    rows = {
        s: [{"date": "2026-01-05", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
             "volume": 1, "amount": 1.0, "code": s.split(".")[0]}]
        for s in job["symbols"]
    }
    return {"ok": True, "op": "daily", "rows": rows}


def test_get_daily_runs_two_bridges_in_parallel(monkeypatch):
    """两个批次的桥接子进程应同时在飞, 且 concurrency 透传给 bridge.mjs。"""
    syms = [f"{600000 + i}.SH" for i in range(sp._BATCH * 2)]
    state = {"active": 0, "max_active": 0}
    lock = threading.Lock()
    seen_concurrency = []

    def fake(job, timeout=None):
        with lock:
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            seen_concurrency.append(job.get("concurrency"))
        time.sleep(0.2)
        with lock:
            state["active"] -= 1
        return _daily_payload(job)

    monkeypatch.setattr(sp.bridge, "run_job", fake)
    df = StockSDKProvider().get_daily(syms, None, None)

    assert df.height == sp._BATCH * 2
    assert state["max_active"] == sp._PARALLEL_BRIDGE
    assert seen_concurrency == [sp._CONCURRENCY] * 2


def test_on_chunk_done_counts_each_chunk_once(monkeypatch):
    syms = [f"{600000 + i}.SH" for i in range(sp._BATCH * 3)]
    calls = []

    def fake(job, timeout=None):
        time.sleep(0.02)
        return _daily_payload(job)

    monkeypatch.setattr(sp.bridge, "run_job", fake)
    StockSDKProvider().get_daily(
        syms, None, None, on_chunk_done=lambda i, n: calls.append((i, n)),
    )

    total = sp._BATCH * 3 // sp._BATCH
    assert sorted(calls) == [(i, total) for i in range(1, total + 1)]
    # 并行下进度必须单调递增, 不能出现 2 先于 1 的乱序回调
    assert calls == sorted(calls)


def test_chunk_failure_does_not_kill_other_chunks(monkeypatch):
    """单批桥接失败只跳过该批, 其余批次照常落库。"""
    syms = [f"{600000 + i}.SH" for i in range(sp._BATCH * 2)]

    def fake(job, timeout=None):
        if "600000.SH" in job["symbols"]:
            raise sp.bridge.StockSDKBridgeError("boom")
        return _daily_payload(job)

    monkeypatch.setattr(sp.bridge, "run_job", fake)
    df = StockSDKProvider().get_daily(syms, None, None)

    assert df.height == sp._BATCH
    assert "600000.SH" not in df["symbol"].unique().to_list()


def test_adj_always_uses_op_adj_sparse_events(monkeypatch):
    """除权一律走 op:adj 稀疏事件拉取, 不再用日K none 基线 + hfq 合成每日因子。

    曾有的 hit_worker 基线合成产出的 ex_factor=close_hfq/close_none 是"后复权累计
    比值"(每个交易日一行), 与事件表契约(单事件比值)不符, cum_prod 会把历史价压成
    ~0 (2026-09 中天科技 0813 事故)。移除后即便同日历窗口先调过 get_daily,
    除权阶段也必须发 op:adj, 不得发 hfq/adjust 或复用基线搞每日合成。
    """
    calls = []

    def fake(job, timeout=None):
        calls.append(job)
        if job["op"] == "daily":
            return {"ok": True, "op": "daily", "rows": {
                "600519.SH": [
                    {"date": "2026-01-05", "open": 1.0, "high": 1.0, "low": 1.0, "close": 10.0,
                     "volume": 1, "amount": 1.0, "code": "600519"},
                    {"date": "2026-01-06", "open": 1.0, "high": 1.0, "low": 1.0, "close": 11.0,
                     "volume": 1, "amount": 1.0, "code": "600519"},
                ],
            }}
        if job["op"] == "adj":
            return {"ok": True, "op": "adj", "rows": {
                "600519.SH": [
                    {"symbol": "600519.SH", "trade_date": "2026-01-06", "ex_factor": 1.1},
                ],
            }}
        return {"ok": True, "op": job["op"], "rows": {}}

    monkeypatch.setattr(sp.bridge, "run_job", fake)
    p = StockSDKProvider()
    w0, w1 = dt.datetime(2026, 1, 1), dt.datetime(2026, 1, 15)

    p.get_daily(["600519.SH"], w0, w1)
    df = p.get_adj_factors(["600519.SH"], w0, w1)

    # 除权阶段只走 op:adj 事件拉取, 绝不发 hfq/adjust 基线合成
    assert [c["op"] for c in calls[1:]] == ["adj"]
    assert df["trade_date"].to_list() == [dt.date(2026, 1, 6)]
    assert abs(df["ex_factor"][0] - 1.1) < 1e-9


def test_adj_factors_drop_insane_ex_factor(monkeypatch):
    """1e26 级脏因子(实测会导致 enriched strict_cast 崩溃)必须在 provider 边界丢弃。"""
    _patch_run_job(monkeypatch, {
        "adj": {"ok": True, "op": "adj", "rows": {
            "600519.SH": [
                {"symbol": "600519.SH", "trade_date": "2020-01-02", "ex_factor": 5.29},
                {"symbol": "600519.SH", "trade_date": "2020-01-03", "ex_factor": 1.9515e26},
                {"symbol": "600519.SH", "trade_date": "2020-01-04", "ex_factor": 0},
                {"symbol": "600519.SH", "trade_date": "2020-01-05", "ex_factor": None},
                {"symbol": "600519.SH", "trade_date": "2020-01-06", "ex_factor": -1.0},
            ],
        }},
    })
    df = StockSDKProvider().get_adj_factors(["600519.SH"], None, None)

    assert df.height == 1
    assert df["trade_date"][0] == dt.date(2020, 1, 2)
    assert abs(df["ex_factor"][0] - 5.29) < 1e-9


def test_adj_breaks_after_consecutive_empty_chunks(monkeypatch):
    """连续空批(限流形态)应熔断中止, 而不是空转完全部批次。"""
    calls = []

    def fake(job, timeout=None):
        calls.append(job)
        return {"ok": True, "op": job["op"], "rows": {}}

    monkeypatch.setattr(sp.bridge, "run_job", fake)
    syms = [f"{600000 + i}.SH" for i in range(sp._BATCH * 20)]  # 20 批全空

    with pytest.raises(sp.UpstreamThrottledError):
        StockSDKProvider().get_adj_factors(
            syms, dt.datetime(2026, 1, 1), dt.datetime.now(),
        )

    # 熔断后未开始的批次不再发起桥接调用 (在飞批次自然结束, 至多 _PARALLEL_BRIDGE 条)
    assert len(calls) <= sp._EMPTY_BREAK_CHUNKS + sp._PARALLEL_BRIDGE


def test_adj_breaker_not_armed_for_history_window(monkeypatch):
    """历史补数据窗口不熔断: 当年未上市标的的空返回是正常形态, 不可误杀。"""
    calls = []

    def fake(job, timeout=None):
        calls.append(job)
        return {"ok": True, "op": job["op"], "rows": {}}

    monkeypatch.setattr(sp.bridge, "run_job", fake)
    syms = [f"{600000 + i}.SH" for i in range(sp._BATCH * 5)]
    old_end = dt.datetime(2015, 1, 1)

    df = StockSDKProvider().get_adj_factors(
        syms, old_end - dt.timedelta(days=365), old_end,
    )

    assert df.is_empty()
    assert len(calls) == 5  # 全部批次照常执行
