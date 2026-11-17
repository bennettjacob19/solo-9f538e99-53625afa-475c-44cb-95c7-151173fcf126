"""In-process unit tests for the shared capacity-seat subsystem.

These drive ``app.acquire_seat`` / ``app.release_seat`` directly against a
temporary SQLite file.  They cover the total and per-level seat limits,
token-fenced releases (a late result cannot free a newer seat), reaping of
seats abandoned by dead workers, and the read-only breaker peek that keeps
capacity rejections away from probe election.
"""

from __future__ import annotations

import importlib
import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture()
def gw(tmp_path, monkeypatch):
    monkeypatch.setenv("GATEWAY_DB", str(tmp_path / "breaker.db"))
    monkeypatch.setenv("REQUEST_TIMEOUT", "1.0")
    monkeypatch.setenv("COOLDOWN", "2.0")
    monkeypatch.setenv("FAILURE_THRESHOLD", "3")
    monkeypatch.setenv("GATEWAY_MAX_INFLIGHT", "2")
    monkeypatch.setenv("GATEWAY_NORMAL_MAX_INFLIGHT", "1")
    monkeypatch.setenv("UPSTREAM_URL", "http://127.0.0.1:1")
    import app as app_module

    importlib.reload(app_module)
    app_module.init_db()
    yield app_module


def test_normal_queries_get_one_seat_then_busy(gw):
    first = gw.acquire_seat("normal", "a")
    assert isinstance(first, gw.Seat)
    second = gw.acquire_seat("normal", "a")
    assert isinstance(second, gw.CapacityBusy)
    assert second.reason == "normal_capacity"
    assert second.inflight == 1 and second.normal_inflight == 1

    snap = gw.capacity_snapshot()
    assert snap["inflight"] == 1
    assert snap["normal_inflight"] == 1
    assert snap["busy_rejected"] == 1
    assert snap["peak_normal"] == 1


def test_emergency_uses_reserved_seat(gw):
    normal = gw.acquire_seat("normal", "a")
    assert isinstance(normal, gw.Seat)
    emergency = gw.acquire_seat("emergency", "rollback")
    assert isinstance(emergency, gw.Seat), "reserved seat must stay available"
    assert emergency.inflight == 2 and emergency.normal_inflight == 1

    # Both seats are now taken; even emergency traffic must be refused.
    overflow = gw.acquire_seat("emergency", "rollback2")
    assert isinstance(overflow, gw.CapacityBusy)
    assert overflow.reason == "total_capacity"


def test_two_emergency_queries_fill_both_seats(gw):
    one = gw.acquire_seat("emergency", "r1")
    two = gw.acquire_seat("emergency", "r2")
    assert isinstance(one, gw.Seat) and isinstance(two, gw.Seat)
    busy = gw.acquire_seat("normal", "d1")
    assert isinstance(busy, gw.CapacityBusy) and busy.reason == "total_capacity"


def test_release_frees_seat_and_late_token_is_a_noop(gw):
    seat = gw.acquire_seat("normal", "a")
    gw.release_seat(seat.token)
    assert gw.capacity_snapshot()["inflight"] == 0

    # A second request now takes the seat.  Releasing the old token again must
    # not free the seat the newer request is holding.
    newer = gw.acquire_seat("normal", "a")
    gw.release_seat(seat.token)  # late result from the first request
    snap = gw.capacity_snapshot()
    assert snap["inflight"] == 1
    gw.release_seat(newer.token)
    assert gw.capacity_snapshot()["inflight"] == 0


def test_dead_worker_seat_is_reaped(gw):
    seat = gw.acquire_seat("emergency", "a")
    # Forge a second seat owned by a PID that cannot exist.
    with gw.write_tx() as conn:
        conn.execute(
            "INSERT INTO capacity_seats "
            "(token, level, artifact, pid, instance_id, acquired_at) "
            "VALUES ('dead-token', 'normal', 'b', 999999, 'other', ?)",
            (time.time(),),
        )
    assert gw.capacity_snapshot()["inflight"] == 2

    # The next acquisition reaps the dead seat first, then admits normally.
    admitted = gw.acquire_seat("normal", "c")
    assert isinstance(admitted, gw.Seat)
    remaining = gw.capacity_snapshot()
    assert remaining["inflight"] == 2  # original live seat + new seat
    tokens = {s["artifact"] for s in remaining["seats"]}
    assert "b" not in tokens and "a" in tokens and "c" in tokens
    gw.release_seat(seat.token)
    gw.release_seat(admitted.token)


def test_lease_expiry_reaps_abandoned_seat(gw, monkeypatch):
    # A seat owned by another (still-live) process whose lease has expired:
    # liveness alone does not save it, the lease backstop reaps it anyway.
    with gw.write_tx() as conn:
        conn.execute(
            "INSERT INTO capacity_seats "
            "(token, level, artifact, pid, instance_id, acquired_at) "
            "VALUES ('stale-token', 'normal', 'a', ?, 'other-instance', ?)",
            (os.getpid(), time.time() - gw.SEAT_LEASE - 1),
        )
    admitted = gw.acquire_seat("normal", "d")
    assert isinstance(admitted, gw.Seat)
    snap = gw.capacity_snapshot()
    assert {s["artifact"] for s in snap["seats"]} == {"d"}
    gw.release_seat(admitted.token)


def test_peak_counters_track_maximum(gw):
    s1 = gw.acquire_seat("normal", "a")
    s2 = gw.acquire_seat("emergency", "b")
    assert isinstance(s1, gw.Seat) and isinstance(s2, gw.Seat)
    gw.release_seat(s1.token)
    gw.release_seat(s2.token)
    snap = gw.capacity_snapshot()
    assert snap["peak_inflight"] == 2
    assert snap["peak_normal"] == 1
    assert snap["admitted"] == 2 and snap["busy_rejected"] == 0


def test_reset_clears_seats_and_counters(gw):
    gw.acquire_seat("normal", "a")
    gw.acquire_seat("emergency", "b")
    gw._reset_capacity()
    snap = gw.capacity_snapshot()
    assert snap["inflight"] == 0
    assert snap["admitted"] == snap["busy_rejected"] == 0
    assert snap["peak_inflight"] == snap["peak_normal"] == 0


def test_peek_admission_rejects_open_breaker_without_write(gw):
    fail = gw.Outcome("failure", "boom")
    for _ in range(3):
        adm = gw.admit("a")
        gw.record_outcome("a", adm, fail)
    peek = gw.peek_admission("a")
    assert peek is not None and not peek.allowed
    assert peek.state == gw.OPEN
    # No seat taken means no probe election: the breaker row is untouched.
    row = gw.connect().execute(
        "SELECT state FROM breakers WHERE artifact = 'a'"
    ).fetchone()
    assert row["state"] == gw.OPEN


def test_peek_allows_closed_artifact(gw):
    peek = gw.peek_admission("never-seen")
    assert peek is not None and peek.allowed and peek.state == gw.CLOSED
