"""Self-tests for the circuit-breaker gateway.

Run against a live deployment (the Docker Compose stack or a locally started
pair of uvicorn processes):

    GATEWAY=http://127.0.0.1:8000 python -m pytest -q tests/test_breaker.py

The tests use only the gateway's public control proxy (``/api/upstream-*``),
so they work whether the example upstream is reachable directly or only from
inside the Compose network.  Each test resets breakers and upstream counters
first.  The persistence test additionally needs ``GATEWAY_RESTART_CMD`` (a
shell command restarting the gateway) and is skipped otherwise.
"""

from __future__ import annotations

import concurrent.futures
import os
import subprocess
import time

import httpx
import pytest

GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:8000").rstrip("/")
RESTART_CMD = os.environ.get("GATEWAY_RESTART_CMD", "")

ART = "example-app.json"
CLIENT = httpx.Client(timeout=15.0)


def reset() -> None:
    CLIENT.post(f"{GATEWAY}/api/reset")
    CLIENT.post(f"{GATEWAY}/api/upstream-stats/reset")
    set_mode("normal")
    CLIENT.post(f"{GATEWAY}/api/upstream-script", json={"responses": []})


def set_mode(mode: str, delay: float | None = None) -> None:
    payload: dict[str, object] = {"mode": mode}
    if delay is not None:
        payload["delay"] = delay
    r = CLIENT.post(f"{GATEWAY}/api/upstream-mode", json=payload)
    assert r.status_code == 200, r.text


def script(responses: list[dict]) -> None:
    r = CLIENT.post(f"{GATEWAY}/api/upstream-script", json={"responses": responses})
    assert r.status_code == 200, r.text


def meta(artifact: str = ART, level: str = "normal") -> httpx.Response:
    return CLIENT.get(f"{GATEWAY}/api/meta/{artifact}?level={level}")


def is_busy(resp: httpx.Response) -> bool:
    return resp.status_code == 503 and resp.json().get("error") == "capacity_busy"


def meta_admitted(artifact: str = ART, level: str = "normal", tries: int = 30) -> httpx.Response:
    """Like ``meta`` but retries the immediate capacity-busy answer.

    The gateway only lets 2 queries through at a time (1 ordinary), so bursty
    tests must wait their turn.  Circuit-open 503s and other responses are
    returned unchanged.
    """
    for attempt in range(tries):
        resp = meta(artifact, level=level)
        if not is_busy(resp):
            return resp
        time.sleep(0.15)
    return resp


def breaker(artifact: str = ART) -> dict:
    data = CLIENT.get(f"{GATEWAY}/api/breakers").json()
    return next(b for b in data["breakers"] if b["artifact"] == artifact)


def gateway_hits() -> int:
    return CLIENT.get(f"{GATEWAY}/api/breakers").json()["gateway_upstream"]["hits"]


def upstream_hits() -> int:
    return CLIENT.get(f"{GATEWAY}/api/upstream-stats").json()["hits"]


@pytest.fixture(autouse=True)
def _reset():
    reset()
    yield


def test_healthy_lookup_returns_metadata_and_is_spread_across_workers():
    pids: set[int] = set()

    def one(_: int) -> dict:
        with httpx.Client(timeout=10.0, headers={"Connection": "close"}) as client:
            r = client.get(f"{GATEWAY}/api/meta/{ART}?level=normal")
            # Only one ordinary query holds a seat at a time; wait our turn.
            for _ in range(30):
                if not (r.status_code == 503 and r.json().get("error") == "capacity_busy"):
                    break
                time.sleep(0.15)
                r = client.get(f"{GATEWAY}/api/meta/{ART}?level=normal")
        assert r.status_code == 200, r.text
        return r.json()

    # Concurrent requests are what the OS load-balances across the two
    # SO_REUSEPORT workers; sequential keep-alive requests may pin to one.
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        for body in pool.map(one, range(12)):
            assert body["upstream"] == "success"
            assert body["metadata"]["metadata"]["name"] == "example-app"
            pids.add(body["pid"])
    assert len(pids) == 2, f"expected both uvicorn workers to serve, got {pids}"


def test_three_consecutive_failures_open_circuit_and_block_upstream():
    set_mode("503")
    codes = [meta().status_code for _ in range(3)]
    assert codes == [502, 502, 502]
    assert breaker()["state"] == "open"
    assert breaker()["failures"] == 3

    hits_before = upstream_hits()
    for _ in range(5):
        r = meta()
        assert r.status_code == 503
        assert r.json()["error"] == "circuit_open"
    assert upstream_hits() == hits_before, "open circuit must not contact upstream"


def test_success_resets_consecutive_failure_count():
    set_mode("503")
    assert meta().status_code == 502
    assert meta().status_code == 502
    set_mode("normal")
    assert meta().status_code == 200
    assert breaker()["failures"] == 0
    set_mode("503")
    assert meta().status_code == 502
    assert meta().status_code == 502
    assert breaker()["state"] == "closed", "two failures after reset must not trip"
    assert meta().status_code == 502
    assert breaker()["state"] == "open"


def test_connect_failure_counts_as_failure():
    # Connect errors are covered deterministically in the unit tests
    # (tests/test_unit.py) by pointing the gateway at a closed port.
    pytest.skip("covered by tests/test_unit.py::test_connect_error_and_timeout_classification")


def test_timeout_is_a_failure_and_bounded():
    set_mode("delay", 3.0)
    started = time.time()
    r = meta()
    elapsed = time.time() - started
    assert r.status_code == 502
    assert 0.9 <= elapsed < 2.0, f"call must be bounded to ~1s, took {elapsed:.2f}s"
    assert "Timeout" in r.json()["detail"] or "timeout" in r.json()["detail"].lower()


def test_cooldown_admits_exactly_one_probe():
    set_mode("503")
    for _ in range(3):
        meta_admitted()
    assert breaker()["state"] == "open"
    time.sleep(2.1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(lambda _: meta(level="emergency"), range(10)))

    roles = [r.json().get("role") for r in results if r.json().get("role")]
    circuit_rejected = [
        r for r in results
        if r.status_code == 503 and r.json().get("error") == "circuit_open"
    ]
    busy = [r for r in results if is_busy(r)]
    assert roles.count("probe") == 1, f"exactly one probe expected, roles={roles}"
    assert len(circuit_rejected) + len(busy) == 9


def test_probe_failure_reopens_and_cooldown_restarts():
    set_mode("503")
    for _ in range(3):
        meta()
    time.sleep(2.1)
    probe = meta()
    assert probe.status_code == 502 and probe.json()["role"] == "probe"
    b = breaker()
    assert b["state"] == "open"
    assert b["cooldown_remaining"] > 1.5, "failed probe must restart the 2s cooldown"
    assert meta().status_code == 503


def test_probe_success_closes_circuit():
    set_mode("503")
    for _ in range(3):
        meta()
    time.sleep(2.1)
    set_mode("normal")
    probe = meta()
    assert probe.status_code == 200 and probe.json()["role"] == "probe"
    assert probe.json()["breaker_effect"] == "closed"
    assert breaker()["state"] == "closed"
    assert meta().status_code == 200


def test_slow_success_started_before_trip_cannot_recover():
    """The in-flight success must be ignored, the breaker stays open."""
    script(
        [
            {"status": 200, "delay": 0.7},  # in flight when the trip happens
            {"status": 503},
            {"status": 503},
            {"status": 503},
        ]
    )
    # Emergency level lets both concurrent requests hold seats (the ordinary
    # pool only admits one at a time); the breaker semantics under test are
    # independent of query level.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        slow = pool.submit(meta, ART, "emergency")
        time.sleep(0.15)
        fast = [meta_admitted(ART, "emergency") for _ in range(3)]
        slow_resp = slow.result()
    assert fast[-1].json()["breaker_effect"] == "opened"
    assert slow_resp.status_code == 200
    assert slow_resp.json()["breaker_effect"] == "stale_success_ignored"
    assert breaker()["state"] == "open", "late pre-trip success must not close the breaker"


def test_slow_queries_do_not_block_each_other():
    # Within the shared 2-seat cap, two slow queries must overlap rather than
    # queue; extra burst traffic is answered immediately with capacity_busy.
    set_mode("delay", 0.7)
    started = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: meta(level="emergency"), range(4)))
    elapsed = time.time() - started
    statuses = sorted(r.status_code for r in results)
    assert 503 in statuses, "overflow traffic must get an immediate busy answer"
    admitted = [r for r in results if not is_busy(r)]
    assert len(admitted) == 2, f"only two seats exist, got {len(admitted)} admitted"
    assert all(r.status_code == 200 for r in admitted)
    assert elapsed < 1.6, f"the two admitted 0.7s queries should overlap (took {elapsed:.2f}s)"
    assert all(
        is_busy(r) for r in results if r.status_code == 503
    ), "every 503 here must be a capacity rejection, not a breaker rejection"


def test_ordinary_burst_admits_one_and_rejects_rest_without_touching_upstream():
    set_mode("delay", 0.8)
    hits_before = upstream_hits()
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: meta(level="normal"), range(6)))
    busy = [r for r in results if is_busy(r)]
    admitted = [r for r in results if not is_busy(r)]
    assert len(admitted) == 1, "ordinary queries share a single seat"
    assert len(busy) == 5
    body = busy[0].json()
    assert body["error"] == "capacity_busy" and body["reason"] == "normal_capacity"
    # Busy answers are local: the directory is never contacted for them.
    assert upstream_hits() == hits_before + 1
    snap = CLIENT.get(f"{GATEWAY}/api/capacity").json()
    assert snap["inflight"] == 0, "the admitted request released its seat on timeout"
    assert snap["busy_rejected"] >= 5
    assert snap["peak_normal"] == 1


def test_emergency_keeps_reserved_seat_under_ordinary_flood():
    set_mode("delay", 0.8)
    cap = f"{GATEWAY}/api/capacity"

    def normal_hold() -> httpx.Response:
        return meta(level="normal")

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        holder = pool.submit(normal_hold)
        # Wait until the ordinary seat is visibly held.
        for _ in range(30):
            snap = CLIENT.get(cap).json()
            if snap["normal_inflight"] == 1:
                break
            time.sleep(0.05)
        emergency = pool.submit(meta, "rollback-artifact.json", "emergency")
        # The overflow is fired while both other requests are still in flight.
        time.sleep(0.1)
        overflow_normal = pool.submit(meta, level="normal")
        hold_resp = holder.result()
        em_resp = emergency.result()
        overflow_resp = overflow_normal.result()

    assert not is_busy(hold_resp), "the ordinary request holds the ordinary seat"
    assert not is_busy(em_resp), "emergency rollback must use the reserved seat"
    assert em_resp.json().get("level") == "emergency"
    assert is_busy(overflow_resp)
    # With the reserved seat also taken the total cap is hit; if the emergency
    # request had already finished it is the ordinary cap instead.  Either way
    # the overflow is an immediate, local busy answer.
    assert overflow_resp.json()["reason"] in {"normal_capacity", "total_capacity"}


def test_capacity_busy_never_counts_towards_breaker_nor_hits():
    set_mode("503")
    # Three *admitted* failures open the breaker; busy rejections in between
    # must neither add failures nor contact the upstream.
    for _ in range(3):
        r = meta_admitted()
        assert r.status_code == 502
    assert breaker()["state"] == "open"
    hits_before = upstream_hits()
    for _ in range(5):
        r = meta(level="normal")
        assert r.status_code == 503
        assert r.json()["error"] == "circuit_open"
    assert upstream_hits() == hits_before
    # Emergency traffic is breaker-rejected as well while OPEN, and still
    # consumes no seat (so the recovery probe later has both seats available).
    r = meta(level="emergency")
    assert r.json()["error"] == "circuit_open"
    assert CLIENT.get(f"{GATEWAY}/api/capacity").json()["inflight"] == 0


def test_busy_rejection_does_not_become_recovery_probe():
    set_mode("503")
    for _ in range(3):
        meta_admitted()
    assert breaker()["state"] == "open"
    time.sleep(2.1)
    set_mode("normal")
    # Flood at the exact cooldown boundary.  Exactly one request may become
    # the probe; busy answers cannot pin the half-open slot.
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: meta(level="emergency"), range(8)))
    roles = [r.json().get("role") for r in results if r.json().get("role")]
    assert roles.count("probe") == 1, f"one probe only, got {roles}"
    assert breaker()["state"] == "closed"


def test_persisted_state_survives_restart():
    if not RESTART_CMD:
        pytest.skip("set GATEWAY_RESTART_CMD to test persistence across restart")
    set_mode("503")
    for _ in range(3):
        meta()
    assert breaker()["state"] == "open"
    subprocess.run(RESTART_CMD, shell=True, check=True)
    for _ in range(60):
        try:
            if CLIENT.get(f"{GATEWAY}/healthz", timeout=1).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    b = breaker()
    assert b["state"] == "open"
    assert b["failures"] == 3
    hits_before = upstream_hits()
    assert meta().status_code == 503
    assert upstream_hits() == hits_before
