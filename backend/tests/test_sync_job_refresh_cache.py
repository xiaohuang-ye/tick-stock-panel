"""同步 job 终态必须刷新 Polars 内存快照 (repo.refresh_cache)。

背景: 读路径(K线/自选/看板/选股/监控)优先使用启动时加载的内存快照,
同步落盘后若不调 refresh_cache(), 页面会一直返回旧数据, 直到重启进程。
本文件锁定各同步入口的契约:
  - 成功 / 取消 / 异常三种终态都要刷新(部分落盘的数据同样要可见);
  - 刷新发生在 quote_service.paused() 上下文内 —— refresh 期间的 enriched
    写入会 bump generation, 令 refresh_cache 半途放弃, 内存快照停留旧数据;
  - 阻塞式单股同步(/sync、/sync_batch)写盘后至少失效数据画像缓存。
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.services.pipeline_jobs import JobCancelledError, JobStore, release_run_slot


class _RepoStub:
    def __init__(self, data_dir: Path) -> None:
        self.store = SimpleNamespace(data_dir=data_dir)
        self.calls: list[str] = []

    def refresh_cache(self) -> None:
        self.calls.append("refresh_cache")


class _QuoteServiceStub:
    def __init__(self) -> None:
        self.paused_entered = 0

    @contextmanager
    def paused(self):
        self.paused_entered += 1
        yield


class _CapsStub:
    def has(self, _cap) -> bool:
        return True


@pytest.fixture(autouse=True)
def _free_run_slot():
    """执行槽是模块级单例, 前后各强制释放一次, 隔离测试。"""
    release_run_slot(None)
    yield
    release_run_slot(None)


def _wait_for(predicate, timeout_s: float = 10.0) -> None:
    """job 在 TestClient 的 loop 线程上经 asyncio.create_task 异步执行, 轮询等待。"""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("condition not met before timeout")


def _client(repo: _RepoStub, qs: _QuoteServiceStub, router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.state.repo = repo
    app.state.capabilities = _CapsStub()
    app.state.quote_service = qs
    return TestClient(app)


# ── /api/pipeline/run (立即同步) ─────────────────────────────────────────

@pytest.mark.parametrize("outcome", ["ok", "cancelled", "raise"])
def test_pipeline_run_refreshes_cache_on_every_terminal_state(
    monkeypatch, tmp_path, outcome
):
    from app.api import pipeline as pipeline_api
    from app.jobs import daily_pipeline

    repo = _RepoStub(tmp_path)
    qs = _QuoteServiceStub()
    store = JobStore(store_dir=tmp_path / "jobs")
    monkeypatch.setattr(pipeline_api, "job_store", store)

    def fake_run_now(_repo, _capset, on_progress=None):
        if outcome == "cancelled":
            raise JobCancelledError("test-job")
        if outcome == "raise":
            raise RuntimeError("boom")
        return {"ok": True}

    monkeypatch.setattr(daily_pipeline, "run_now", fake_run_now)

    with _client(repo, qs, pipeline_api.router) as client:
        resp = client.post("/api/pipeline/run")
        assert resp.status_code == 200
        job_id = resp.json()["job_id"]
        _wait_for(lambda: "refresh_cache" in repo.calls)
        assert qs.paused_entered == 1
        if outcome == "ok":
            assert store.get(job_id)["status"] == "succeeded"
        elif outcome == "raise":
            assert store.get(job_id)["status"] == "failed"


# ── /api/kline/sync_symbols (按股票同步) ─────────────────────────────────

@pytest.mark.parametrize("outcome", ["ok", "cancelled", "raise"])
def test_sync_symbols_refreshes_cache_on_every_terminal_state(
    monkeypatch, tmp_path, outcome
):
    from app.api import kline as kline_api
    from app.jobs import daily_pipeline
    from app.services import pipeline_jobs

    repo = _RepoStub(tmp_path)
    qs = _QuoteServiceStub()
    store = JobStore(store_dir=tmp_path / "jobs")
    monkeypatch.setattr(pipeline_jobs, "job_store", store)

    def fake_symbols_sync(_repo, _capset, symbols, days=30, on_progress=None):
        if outcome == "cancelled":
            raise JobCancelledError("test-job")
        if outcome == "raise":
            raise RuntimeError("boom")
        return {"symbols": len(symbols)}

    monkeypatch.setattr(daily_pipeline, "run_symbols_sync", fake_symbols_sync)

    with _client(repo, qs, kline_api.router) as client:
        resp = client.post(
            "/api/kline/sync_symbols",
            json={"symbols": ["600519"], "days": 30},
        )
        assert resp.status_code == 200
        _wait_for(lambda: "refresh_cache" in repo.calls)
        assert qs.paused_entered == 1


# ── /api/kline/extend_history / repair_daily / rebuild_enriched ─────────

def test_extend_history_refreshes_cache(monkeypatch, tmp_path):
    from app.api import kline as kline_api
    from app.services import extend_history, pipeline_jobs

    repo = _RepoStub(tmp_path)
    qs = _QuoteServiceStub()
    store = JobStore(store_dir=tmp_path / "jobs")
    monkeypatch.setattr(pipeline_jobs, "job_store", store)
    monkeypatch.setattr(
        extend_history, "run_extend_history",
        lambda _repo, _capset, value, unit, on_progress=None: {"extended_days": 30},
    )

    with _client(repo, qs, kline_api.router) as client:
        resp = client.post("/api/kline/extend_history", json={"value": 1, "unit": "month"})
        assert resp.status_code == 200
        _wait_for(lambda: "refresh_cache" in repo.calls)
        assert qs.paused_entered == 1


def test_repair_daily_refreshes_cache(monkeypatch, tmp_path):
    from app.api import kline as kline_api
    from app.services import pipeline_jobs, repair_daily

    repo = _RepoStub(tmp_path)
    qs = _QuoteServiceStub()
    store = JobStore(store_dir=tmp_path / "jobs")
    monkeypatch.setattr(pipeline_jobs, "job_store", store)
    monkeypatch.setattr(
        repair_daily, "run_repair_daily",
        lambda _repo, _capset, start_date, on_progress=None: {"repaired": True},
    )

    with _client(repo, qs, kline_api.router) as client:
        resp = client.post("/api/kline/repair_daily", json={"start_date": "2026-08-01"})
        assert resp.status_code == 200
        _wait_for(lambda: "refresh_cache" in repo.calls)
        assert qs.paused_entered == 1


def test_rebuild_enriched_refreshes_cache(monkeypatch, tmp_path):
    from app.api import kline as kline_api
    from app.indicators import pipeline as indicators_pipeline
    from app.services import pipeline_jobs

    repo = _RepoStub(tmp_path)
    qs = _QuoteServiceStub()
    store = JobStore(store_dir=tmp_path / "jobs")
    monkeypatch.setattr(pipeline_jobs, "job_store", store)
    monkeypatch.setattr(
        indicators_pipeline, "run_pipeline", lambda on_batch_done=None: 123,
    )

    with _client(repo, qs, kline_api.router) as client:
        resp = client.post("/api/kline/rebuild_enriched")
        assert resp.status_code == 200
        _wait_for(lambda: "refresh_cache" in repo.calls)
        assert qs.paused_entered == 1
        job_id = resp.json()["job_id"]
        _wait_for(lambda: store.get(job_id)["status"] == "succeeded")


# ── /api/kline/sync (阻塞式单股同步, 无 job) ────────────────────────────

def test_sync_symbol_invalidates_storage_cache(monkeypatch, tmp_path):
    from app.api import data as data_api
    from app.api import kline as kline_api
    from app.services import kline_sync

    repo = _RepoStub(tmp_path)
    monkeypatch.setattr(
        kline_sync, "sync_and_persist_daily_batch",
        lambda symbols, _repo, _capset, count=None: 5,
    )
    invalidated: list = []
    monkeypatch.setattr(
        data_api, "invalidate_storage_cache", lambda: invalidated.append("invalidate"),
    )

    with _client(repo, _QuoteServiceStub(), kline_api.router) as client:
        resp = client.post("/api/kline/sync?symbol=600519.SH&days=30")
        assert resp.status_code == 200
        assert resp.json() == {"symbol": "600519.SH", "rows_written": 5}
    assert invalidated == ["invalidate"]
