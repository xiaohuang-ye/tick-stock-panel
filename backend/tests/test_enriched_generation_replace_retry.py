"""enriched 发布的 os.replace 瞬时占用重试测试 (Windows WinError 5)。"""
import polars as pl
import pytest

from app import enriched_generation as eg


def test_replace_with_retry_succeeds_after_transient_errors(monkeypatch, tmp_path):
    """前两次 PermissionError 后第三次成功 → 重试生效, 退避递增。"""
    calls = {"n": 0}
    sleeps: list[float] = []

    def flaky_replace(src, dst):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise PermissionError(5, "拒绝访问。")

    monkeypatch.setattr(eg.os, "replace", flaky_replace)
    monkeypatch.setattr(eg.time, "sleep", sleeps.append)

    eg._replace_with_retry(tmp_path / "a", tmp_path / "b")

    assert calls["n"] == 3
    assert sleeps == [0.15, 0.3]


def test_replace_with_retry_raises_after_exhaustion(monkeypatch, tmp_path):
    """持续占用 → 重试耗尽 (5 次) 后抛出原始 PermissionError。"""
    calls = {"n": 0}

    def always_locked(src, dst):
        calls["n"] += 1
        raise PermissionError(5, "拒绝访问。")

    monkeypatch.setattr(eg.os, "replace", always_locked)
    monkeypatch.setattr(eg.time, "sleep", lambda _s: None)

    with pytest.raises(PermissionError):
        eg._replace_with_retry(tmp_path / "a", tmp_path / "b")
    assert calls["n"] == eg._REPLACE_RETRY_ATTEMPTS


def test_write_parquet_retries_when_destination_locked(tmp_path, monkeypatch):
    """write_parquet 在目标分区被瞬时占用时重试并成功落盘, marker 状态一致。"""
    real_replace = eg.os.replace
    calls = {"n": 0}

    def flaky_replace(src, dst):
        if str(dst).endswith("part.parquet"):
            calls["n"] += 1
            if calls["n"] == 1:
                raise PermissionError(5, "拒绝访问。")
        real_replace(src, dst)

    monkeypatch.setattr(eg.os, "replace", flaky_replace)
    monkeypatch.setattr(eg.time, "sleep", lambda _s: None)

    pub = eg.EnrichedPublication(tmp_path, "stock")
    df = pl.DataFrame({"symbol": ["600519.SH"], "close": [1.0]})
    out = tmp_path / "kline_daily_enriched" / "date=2025-08-27" / "part.parquet"

    pub.begin()
    pub.write_parquet(df, out)
    generation = pub.commit()

    assert calls["n"] == 2
    assert generation
    assert out.exists()
    assert pl.read_parquet(out).height == 1
    assert not eg.enriched_publication_incomplete(tmp_path, "stock")
