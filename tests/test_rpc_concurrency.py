import json
import threading
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from conftest import module


class AdvancingClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.now

    def sleep(self, seconds):
        with self.lock:
            self.sleeps.append(seconds)
            self.now += seconds


def success(request):
    payload = json.loads(request.content)
    if isinstance(payload, list):
        return httpx.Response(200, json=[{"id": item["id"], "result": item["method"]} for item in payload])
    return httpx.Response(200, json={"id": payload["id"], "result": payload["method"]})


def test_http_overlap_unique_ids_and_cap():
    barrier = threading.Barrier(3, timeout=3)
    identifiers = []
    active = 0
    peak = 0
    lock = threading.Lock()

    def handler(request):
        nonlocal active, peak
        with lock:
            identifiers.append(json.loads(request.content)["id"])
            active += 1
            peak = max(peak, active)
        barrier.wait()
        with lock:
            active -= 1
        return success(request)

    clock = AdvancingClock()
    client = module("rpc").RpcClient("https://node.invalid", qps=1000, concurrency=3,
                                     transport=httpx.MockTransport(handler), clock=clock, sleep=clock.sleep)
    try:
        with ThreadPoolExecutor(max_workers=6) as workers:
            futures = [workers.submit(client.call, "method", []) for _ in range(6)]
            assert [future.result(timeout=5) for future in futures] == ["method"] * 6
        assert len(set(identifiers)) == 6
        assert peak == 3
        stats = client.snapshot()
        assert stats["peak_in_flight"] == 3
        assert stats["in_flight"] == 0
        assert stats["rpc_elements"] == stats["http_attempts"] == 6
        assert stats["retries"] == stats["http_429"] == 0
        assert stats["rpc_elements_per_second"] == pytest.approx(1000)
    finally:
        client.close()


def test_shared_pacing_mixed_batches_partial_retries_and_no_idle_credit():
    clock = AdvancingClock()
    dispatches = []

    def handler(request):
        payload = json.loads(request.content)
        dispatches.append((clock(), payload))
        if len(dispatches) == 2:
            return httpx.Response(200, json=[{"id": payload[0]["id"], "result": "first"},
                                            {"id": payload[1]["id"], "error": {"code": -32005, "message": "busy"}}])
        return success(request)

    client = module("rpc").RpcClient("https://node.invalid", qps=2, concurrency=2, batch_size=10,
                                     transport=httpx.MockTransport(handler), clock=clock, sleep=clock.sleep)
    try:
        assert client.call("single", []) == "single"
        assert client.batch([("first", []), ("retry", [])]) == ["first", "retry"]
        clock.sleep(100)
        previous = clock()
        assert client.call("idle", []) == "idle"
        assert [entry[0] for entry in dispatches[:3]] == [0.5, 1.5, 3.0]
        assert dispatches[-1][0] == previous + 0.5
        assert len(dispatches[2][1]) == 1
        stats = client.snapshot()
        assert stats["rpc_elements"] == 5
        assert stats["http_attempts"] == 4
        assert stats["retries"] == 1
        assert set(stats) == {"rpc_elements", "http_attempts", "retries", "http_429", "in_flight", "peak_in_flight", "rpc_elements_per_second"}
    finally:
        client.close()


def test_terminal_429_publishes_cooldown_for_new_requests():
    clock = AdvancingClock()
    dispatches = []

    def handler(request):
        dispatches.append(clock())
        if len(dispatches) == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return success(request)

    client = module("rpc").RpcClient("https://private:SECRET@node.invalid/key", qps=1, retries=0,
                                     transport=httpx.MockTransport(handler), clock=clock, sleep=clock.sleep)
    try:
        with pytest.raises(module("rpc").RpcError, match="exhausted"):
            client.call("first", [])
        assert client.call("second", []) == "second"
        assert dispatches == [1, 9]
        stats = client.snapshot()
        assert stats["http_429"] == 1
        assert stats["http_attempts"] == stats["rpc_elements"] == 2
        assert stats["retries"] == 0
        assert "SECRET" not in str(stats)
    finally:
        client.close()


def test_waiter_rechecks_cooldown_extended_by_another_in_flight_request():
    clock = AdvancingClock()
    first_entered = threading.Event()
    second_entered = threading.Event()
    waiter_sleeping = threading.Event()
    release_waiter = threading.Event()
    dispatches = {}
    main_thread = threading.get_ident()

    def sleep(seconds):
        if threading.get_ident() == main_thread and second_entered.is_set():
            waiter_sleeping.set()
            assert release_waiter.wait(3)
        clock.sleep(seconds)

    def handler(request):
        method = json.loads(request.content)["method"]
        dispatches[method] = clock()
        if method == "first":
            first_entered.set()
            assert second_entered.wait(3)
            return httpx.Response(429, headers={"Retry-After": "5"})
        if method == "second":
            second_entered.set()
            assert waiter_sleeping.wait(3)
            return httpx.Response(429, headers={"Retry-After": "20"})
        return success(request)

    client = module("rpc").RpcClient("https://node.invalid", qps=1, retries=0, concurrency=3,
                                     transport=httpx.MockTransport(handler), clock=clock, sleep=sleep)
    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            first = workers.submit(client.call, "first", [])
            assert first_entered.wait(3)
            second = workers.submit(client.call, "second", [])
            second.add_done_callback(lambda future: release_waiter.set())
            assert second_entered.wait(3)
            assert client.call("waiting", []) == "waiting"
            for future in (first, second):
                with pytest.raises(module("rpc").RpcError):
                    future.result(timeout=3)
        assert dispatches["waiting"] >= dispatches["second"] + 21
        assert client.snapshot()["http_429"] == 2
    finally:
        client.close()


@pytest.mark.parametrize("value", ["600", "inf", "nan"])
def test_retry_after_oversized_is_terminal_and_counted(value):
    clock = AdvancingClock()
    client = module("rpc").RpcClient("https://node.invalid", retries=2, clock=clock, sleep=clock.sleep,
                                     transport=httpx.MockTransport(lambda request: httpx.Response(429, headers={"Retry-After": value})))
    try:
        with pytest.raises(module("rpc").RpcError, match="bounded"):
            client.call("method", [])
        assert client.snapshot()["http_attempts"] == client.snapshot()["http_429"] == 1
        assert client.snapshot()["retries"] == 0
        assert clock() == 1
    finally:
        client.close()


@pytest.mark.parametrize("value", ["invalid", "-5"])
def test_bad_retry_after_uses_bounded_backoff(value):
    clock = AdvancingClock()
    attempts = []

    def handler(request):
        attempts.append(clock())
        return httpx.Response(429, headers={"Retry-After": value}) if len(attempts) == 1 else success(request)

    client = module("rpc").RpcClient("https://node.invalid", retries=1, clock=clock, sleep=clock.sleep,
                                     transport=httpx.MockTransport(handler))
    try:
        assert client.call("method", []) == "method"
        assert attempts == [1, 3]
        assert client.snapshot()["retries"] == 1
    finally:
        client.close()


@pytest.mark.parametrize("concurrency", [0, 129, True, 1.5, "2"])
def test_invalid_concurrency_rejected(concurrency):
    with pytest.raises(ValueError, match="settings"):
        module("rpc").RpcClient("https://node.invalid", concurrency=concurrency)


@pytest.mark.parametrize("qps", [float("nan"), float("inf"), -float("inf"), 0, -1])
def test_nonfinite_or_nonpositive_qps_rejected(qps):
    with pytest.raises(ValueError, match="settings"):
        module("rpc").RpcClient("https://node.invalid", qps=qps)


def test_oversized_cooldown_stops_other_requests_without_unused_attempt_counts():
    clock = AdvancingClock()
    attempts = []

    def handler(request):
        attempts.append(request)
        return httpx.Response(429, headers={"Retry-After": "600"}) if len(attempts) == 1 else success(request)

    client = module("rpc").RpcClient("https://node.invalid", clock=clock, sleep=clock.sleep,
                                     transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(module("rpc").RpcError, match="bounded"):
            client.call("first", [])
        with pytest.raises(module("rpc").RpcError, match="bounded"):
            client.call("second", [])
        assert len(attempts) == 1
        assert client.snapshot()["http_attempts"] == client.snapshot()["rpc_elements"] == 1
    finally:
        client.close()


def test_threads_share_element_schedule_and_reserved_work_is_not_counted():
    condition = threading.Condition()
    now = 0.0
    sleepers = {}
    batch_entered = threading.Event()
    release_batch = threading.Event()
    dispatches = []

    def clock():
        with condition:
            return now

    def sleep(seconds):
        with condition:
            identifier = threading.get_ident()
            deadline = now + seconds
            sleepers[identifier] = deadline
            condition.notify_all()
            assert condition.wait_for(lambda: now >= deadline, timeout=3)
            del sleepers[identifier]

    def handler(request):
        payload = json.loads(request.content)
        dispatches.append((clock(), 2 if isinstance(payload, list) else 1))
        if isinstance(payload, list):
            batch_entered.set()
            assert release_batch.wait(3)
        else:
            release_batch.set()
        return success(request)

    client = module("rpc").RpcClient("https://node.invalid", qps=2, concurrency=2, batch_size=2,
                                     transport=httpx.MockTransport(handler), clock=clock, sleep=sleep)
    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            batch = workers.submit(client.batch, [("first", []), ("second", [])])
            with condition:
                assert condition.wait_for(lambda: len(sleepers) == 1, timeout=3)
            single = workers.submit(client.call, "single", [])
            with condition:
                assert condition.wait_for(lambda: len(sleepers) == 2, timeout=3)
                assert sorted(sleepers.values()) == [1, 1.5]
            assert client.snapshot()["rpc_elements"] == client.snapshot()["http_attempts"] == 0
            with condition:
                now = 1
                condition.notify_all()
            assert batch_entered.wait(3)
            assert client.snapshot()["rpc_elements"] == 2
            with condition:
                now = 1.5
                condition.notify_all()
            assert single.result(timeout=3) == "single"
            assert batch.result(timeout=3) == ["first", "second"]
        assert dispatches == [(1, 2), (1.5, 1)]
        assert client.snapshot()["rpc_elements"] == 3
        assert client.snapshot()["http_attempts"] == client.snapshot()["peak_in_flight"] == 2
    finally:
        release_batch.set()
        client.close()


@pytest.mark.parametrize(("qps", "element_counts"), [(1, (1, 1, 1)), (2, (2, 1, 3))])
def test_delayed_wakeups_repace_actual_admissions(qps, element_counts):
    condition = threading.Condition()
    now = 0.0
    sleepers = {}
    dispatches = []
    stopping = threading.Event()

    def clock():
        with condition:
            return now

    def sleep(seconds):
        with condition:
            identifier = threading.get_ident()
            deadline = now + seconds
            sleepers[identifier] = deadline
            condition.notify_all()
            assert condition.wait_for(lambda: now >= deadline or stopping.is_set(), timeout=3)
            del sleepers[identifier]
            if stopping.is_set():
                raise RuntimeError("test stopped")

    def handler(request):
        payload = json.loads(request.content)
        with condition:
            dispatches.append((now, len(payload) if isinstance(payload, list) else 1))
            condition.notify_all()
        return success(request)

    client = module("rpc").RpcClient("https://node.invalid", qps=qps, concurrency=3, batch_size=3,
                                     transport=httpx.MockTransport(handler), clock=clock, sleep=sleep)
    try:
        with ThreadPoolExecutor(max_workers=3) as workers:
            try:
                futures = []
                for index, elements in enumerate(element_counts):
                    calls = [(f"request_{index}_{element}", []) for element in range(elements)]
                    futures.append(workers.submit(client.call, *calls[0]) if elements == 1
                                   else workers.submit(client.batch, calls))
                    with condition:
                        assert condition.wait_for(lambda: len(sleepers) == index + 1, timeout=3)
                with condition:
                    assert sorted(sleepers.values()) == [sum(element_counts[:count]) / qps
                                                          for count in range(1, 4)]
                    now = 100.0
                    condition.notify_all()
                previous_admissions = 0
                for _ in range(9):
                    with condition:
                        assert condition.wait_for(
                            lambda: len(dispatches) + sum(deadline > now for deadline in sleepers.values()) == 3,
                            timeout=3)
                        assert previous_admissions <= len(dispatches) <= previous_admissions + 1
                        previous_admissions = len(dispatches)
                        if len(dispatches) == 3:
                            break
                        now = min(sleepers.values())
                        condition.notify_all()
                assert len(dispatches) == 3
                for future in futures:
                    future.result(timeout=3)
            finally:
                stopping.set()
                with condition:
                    condition.notify_all()
        assert dispatches[0][0] == 100.0
        for previous, current in zip(dispatches, dispatches[1:]):
            assert current[0] == pytest.approx(previous[0] + current[1] / qps)
        assert client.snapshot()["rpc_elements"] == sum(element_counts)
        assert client.snapshot()["http_attempts"] == 3
    finally:
        client.close()


def test_close_waits_for_active_request_and_rejects_new_work():
    clock = AdvancingClock()
    entered = threading.Event()
    release = threading.Event()
    transport_closed = threading.Event()

    class Transport(httpx.MockTransport):
        def close(self):
            transport_closed.set()

    def handler(request):
        entered.set()
        assert release.wait(3)
        assert not transport_closed.is_set()
        return success(request)

    client = module("rpc").RpcClient("https://node.invalid", clock=clock, sleep=clock.sleep,
                                     transport=Transport(handler))
    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            active = workers.submit(client.call, "method", [])
            assert entered.wait(3)
            closing = workers.submit(client.close)
            with client.lifecycle:
                assert client.lifecycle.wait_for(lambda: client.closing, timeout=3)
            assert not transport_closed.is_set()
            with pytest.raises(module("rpc").RpcError, match="closed"):
                client.call("new", [])
            release.set()
            assert active.result(timeout=3) == "method"
            closing.result(timeout=3)
        assert transport_closed.is_set()
        assert client.snapshot()["http_attempts"] == 1
    finally:
        release.set()
        client.close()
