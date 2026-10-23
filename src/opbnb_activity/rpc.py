import logging
import threading
import time
import math
from contextlib import contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

logger = logging.getLogger(__name__)


class RpcError(RuntimeError):
    def __init__(self, kind, method="request", code=None):
        self.kind, self.method, self.code = kind, method, code
        super().__init__(f"RPC {method}: {kind}" + (f" (code {code})" if isinstance(code, int) else ""))


class RpcProtocolError(RpcError):
    def __init__(self, reason):
        super().__init__("protocol: " + reason)


def error_kind(code, message):
    message = str(message).lower()
    if code == -32601:
        return "unsupported"
    if any(token in message for token in ("missing trie", "pruned", "historical state", "header not found")):
        return "pruned"
    if code in (-32005, -32002) or any(token in message for token in ("rate limit", "too many requests", "temporarily", "timeout", "busy")):
        return "transient"
    return "permanent"


class RpcClient:
    def __init__(self, endpoint, *, qps=1.0, timeout=30, retries=3, batch_size=1, transport=None, sleep=time.sleep, concurrency=1, clock=time.monotonic):
        import httpx
        if not math.isfinite(qps) or qps <= 0 or timeout <= 0 or retries < 0 or not 1 <= batch_size <= 100 or type(concurrency) is not int or not 1 <= concurrency <= 128:
            raise ValueError("Invalid RPC settings")
        try:
            parsed = httpx.URL(endpoint)
            if parsed.scheme not in ("http", "https") or not parsed.host:
                raise httpx.InvalidURL("invalid endpoint")
        except httpx.InvalidURL:
            raise RpcError("invalid endpoint") from None
        self.httpx = httpx
        self.client = httpx.Client(transport=transport, timeout=timeout, follow_redirects=False, trust_env=False,
                                   limits=httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency))
        self.endpoint, self.qps, self.retries = endpoint, qps, retries
        self.batch_size, self.sleep = batch_size, sleep
        self.lock = threading.Lock()
        self.next_id = 1
        self.clock = clock
        self.concurrency = concurrency
        self.permits = threading.BoundedSemaphore(concurrency)
        self.state_lock = threading.Lock()
        self.started_at = clock()
        self.next_dispatch = self.started_at
        self.last_admission = self.started_at
        self.cooldown_until = self.started_at
        self.cooldown_version = 0
        self.wait_budget_exceeded = False
        self.stats = dict(rpc_elements=0, http_attempts=0, retries=0, http_429=0, in_flight=0, peak_in_flight=0)
        self.lifecycle = threading.Condition()
        self.active_operations = 0
        self.closing = False
        self.closed = False

    def close(self):
        with self.lifecycle:
            if self.closing:
                self.lifecycle.wait_for(lambda: self.closed)
                return
            self.closing = True
            self.lifecycle.notify_all()
            self.lifecycle.wait_for(lambda: self.active_operations == 0)
        try:
            self.client.close()
        finally:
            with self.lifecycle:
                self.closed = True
                self.lifecycle.notify_all()

    @contextmanager
    def _operation(self):
        with self.lifecycle:
            if self.closing:
                raise RpcError("client closed")
            self.active_operations += 1
        try:
            yield
        finally:
            with self.lifecycle:
                self.active_operations -= 1
                self.lifecycle.notify_all()

    def snapshot(self):
        with self.state_lock:
            stats = dict(self.stats)
            elapsed = max(0.0, self.clock() - self.started_at)
            stats["rpc_elements_per_second"] = stats["rpc_elements"] / elapsed if elapsed else 0.0
            return stats

    def _begin_attempt(self, elements, retry):
        with self.state_lock:
            if self.wait_budget_exceeded:
                raise RpcError("retry-after exceeds bounded wait; stopped without an early retry")
            self.next_dispatch = max(self.clock(), self.next_dispatch, self.cooldown_until) + elements / self.qps
            ready_at = self.next_dispatch
            version = self.cooldown_version
        while True:
            with self.state_lock:
                if self.wait_budget_exceeded:
                    raise RpcError("retry-after exceeds bounded wait; stopped without an early retry")
                now = self.clock()
                if version != self.cooldown_version:
                    self.next_dispatch = max(now, self.next_dispatch, self.cooldown_until) + elements / self.qps
                    ready_at = self.next_dispatch
                    version = self.cooldown_version
                delay = max(ready_at, self.last_admission + elements / self.qps) - now
                if delay <= 0:
                    self.last_admission = now
                    self.stats["rpc_elements"] += elements
                    self.stats["http_attempts"] += 1
                    self.stats["retries"] += int(retry)
                    self.stats["in_flight"] += 1
                    self.stats["peak_in_flight"] = max(self.stats["peak_in_flight"], self.stats["in_flight"])
                    return
            self.sleep(delay)

    def _publish_cooldown(self, delay):
        with self.state_lock:
            until = self.clock() + delay
            if until > self.cooldown_until:
                self.cooldown_until = until
                self.next_dispatch = max(self.next_dispatch, until)
                self.cooldown_version += 1

    def _post(self, pending, use_batch, attempt):
        with self.permits:
            self._begin_attempt(len(pending), retry=attempt > 0)
            try:
                response = self.client.post(self.endpoint, json=pending if use_batch else pending[0])
                if response.status_code == 429:
                    with self.state_lock:
                        self.stats["http_429"] += 1
                    try:
                        delay = max(self._retry_after(response), min(2**attempt, 30))
                    except RpcError:
                        with self.state_lock:
                            self.wait_budget_exceeded = True
                        raise
                    self._publish_cooldown(delay)
                return response
            finally:
                with self.state_lock:
                    self.stats["in_flight"] -= 1

    def call(self, method, params):
        return self._execute([(method, params)], use_batch=False)[0]

    def batch(self, calls):
        output = []
        for offset in range(0, len(calls), self.batch_size):
            output.extend(self._execute(calls[offset:offset + self.batch_size], use_batch=True))
        return output

    def probe_batch(self):
        return self._execute([("eth_chainId", []), ("eth_chainId", [])], use_batch=True)

    def _retry_after(self, response):
        value = response.headers.get("Retry-After", "0")
        try:
            delay = float(value)
        except ValueError:
            try:
                target = parsedate_to_datetime(value)
                delay = (target - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                return 0.0
        if not math.isfinite(delay) or delay > 300:
            raise RpcError("retry-after exceeds bounded wait; stopped without an early retry")
        return max(0.0, delay)

    def _execute(self, calls, use_batch):
        with self._operation():
            items = []
            with self.lock:
                for method, params in calls:
                    items.append({"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params})
                    self.next_id += 1
            pending = list(items)
            results = {}
            for attempt in range(self.retries + 1):
                try:
                    response = self._post(pending, use_batch, attempt)
                except self.httpx.TransportError:
                    if attempt == self.retries:
                        raise RpcError("transport failure") from None
                    delay = min(2**attempt, 30)
                    self._log_retry(pending, attempt, "transport", delay)
                    self.sleep(delay)
                    continue
                if response.status_code in (429, 500, 502, 503, 504):
                    if attempt == self.retries:
                        raise RpcError("transient HTTP exhausted")
                    delay = max(self._retry_after(response), min(2**attempt, 30))
                    self._log_retry(pending, attempt, f"HTTP_{response.status_code}", delay)
                    self.sleep(delay)
                    continue
                if response.status_code != 200:
                    raise RpcError("HTTP rejected", code=response.status_code)
                try:
                    answers = response.json()
                except ValueError:
                    raise RpcProtocolError("invalid JSON") from None
                answers = answers if isinstance(answers, list) else [answers]
                expected = {item["id"] for item in pending}
                seen = [answer.get("id") for answer in answers if isinstance(answer, dict)]
                if len(seen) != len(answers) or any(type(identifier) is not int for identifier in seen) or len(set(seen)) != len(seen) or set(seen) != expected:
                    raise RpcProtocolError("missing, duplicate, or unexpected IDs")
                by_id = {answer["id"]: answer for answer in answers}
                retry = []
                for item in pending:
                    answer = by_id[item["id"]]
                    if "error" in answer:
                        error = answer["error"]
                        if not isinstance(error, dict):
                            raise RpcProtocolError("malformed error envelope")
                        kind = error_kind(error.get("code"), error.get("message"))
                        if kind == "transient" and attempt < self.retries:
                            retry.append(item)
                        else:
                            raise RpcError(kind, item["method"], error.get("code"))
                    elif "result" in answer:
                        results[item["id"]] = answer["result"]
                    else:
                        raise RpcProtocolError("missing result/error")
                if not retry:
                    return [results[item["id"]] for item in items]
                pending = retry
                delay = min(2**attempt, 30)
                self._log_retry(pending, attempt, "transient", delay)
                self.sleep(delay)
            raise RpcError("retry exhausted")

    def _log_retry(self, pending, attempt, reason, delay):
        methods = ",".join(dict.fromkeys(item["method"] for item in pending))
        logger.warning("RPC retry: method=%s attempt=%d/%d reason=%s delay_seconds=%.1f",
                       methods, attempt + 1, self.retries + 1, reason, delay)
