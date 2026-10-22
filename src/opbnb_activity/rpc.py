import threading
import time
import math
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime


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
    def __init__(self, endpoint, *, qps=1.0, timeout=30, retries=3, batch_size=1, transport=None, sleep=time.sleep):
        import httpx
        if qps <= 0 or timeout <= 0 or retries < 0 or not 1 <= batch_size <= 100:
            raise ValueError("Invalid RPC settings")
        try:
            parsed = httpx.URL(endpoint)
            if parsed.scheme not in ("http", "https") or not parsed.host:
                raise httpx.InvalidURL("invalid endpoint")
        except httpx.InvalidURL:
            raise RpcError("invalid endpoint") from None
        self.httpx = httpx
        self.client = httpx.Client(transport=transport, timeout=timeout, follow_redirects=False, trust_env=False)
        self.endpoint, self.qps, self.retries = endpoint, qps, retries
        self.batch_size, self.sleep = batch_size, sleep
        self.lock = threading.Lock()
        self.next_id = 1

    def close(self):
        self.client.close()

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
        with self.lock:
            items = []
            for method, params in calls:
                items.append({"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params})
                self.next_id += 1
            pending = list(items)
            results = {}
            for attempt in range(self.retries + 1):
                self.sleep(len(pending) / self.qps)
                try:
                    response = self.client.post(self.endpoint, json=pending if use_batch else pending[0])
                except self.httpx.TransportError:
                    if attempt == self.retries:
                        raise RpcError("transport failure") from None
                    self.sleep(min(2**attempt, 30))
                    continue
                if response.status_code in (429, 500, 502, 503, 504):
                    if attempt == self.retries:
                        raise RpcError("transient HTTP exhausted")
                    self.sleep(max(self._retry_after(response), min(2**attempt, 30)))
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
                self.sleep(min(2**attempt, 30))
            raise RpcError("retry exhausted")
