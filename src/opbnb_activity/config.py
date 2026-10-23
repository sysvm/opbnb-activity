import hashlib
import json
import math
import re
import tomllib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

SYSTEM_DEPOSITOR = "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001"
SYSTEM_RECIPIENT = "0x4200000000000000000000000000000000000015"


@dataclass
class Config:
    dataset: Path = Path("data")
    run_id: str = ""
    start: str = "2026-07-22"
    end: str = "2026-10-22"
    timezone: str = "Asia/Shanghai"
    qps: float = 1.0
    concurrency: int = 1
    batch_size: int = 1
    blocks_per_partition: int = 500
    timeout_seconds: float = 30.0
    retries: int = 3
    labels: str | None = None
    assets: dict = field(default_factory=dict)
    system_recipients: list[str] = field(default_factory=lambda: [SYSTEM_RECIPIENT])
    system_depositor: str = SYSTEM_DEPOSITOR

    def __post_init__(self):
        self.dataset = Path(self.dataset).expanduser().resolve()
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", self.run_id):
            raise ValueError("Explicit safe run_id required (letters, numbers, underscore, dot, dash)")
        if self.start_utc >= self.end_utc:
            raise ValueError("start must precede end")
        if type(self.concurrency) is not int or not 1 <= self.concurrency <= 128:
            raise ValueError("Invalid bounded RPC settings")
        if not math.isfinite(self.qps) or self.qps <= 0 or not 1 <= self.batch_size <= 100 or not 1 <= self.blocks_per_partition <= 5000 or self.retries < 0 or self.timeout_seconds <= 0:
            raise ValueError("Invalid bounded RPC settings")
        self.system_recipients = [value.lower() for value in self.system_recipients]
        self.system_depositor = self.system_depositor.lower()
        for asset in self.assets.values():
            if not isinstance(asset.get("decimals"), int) or not 0 <= asset["decimals"] <= 255:
                raise ValueError("Asset decimals must be an integer in [0,255]")

    def utc(self, value):
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=ZoneInfo(self.timezone))
        return parsed.astimezone(timezone.utc)

    @property
    def start_utc(self):
        return self.utc(self.start)

    @property
    def end_utc(self):
        return self.utc(self.end)

    @property
    def identity(self):
        settings = asdict(self)
        for key in ("dataset", "run_id", "qps", "concurrency", "batch_size", "timeout_seconds", "retries", "labels", "assets"):
            settings.pop(key)
        settings.update(schema_version=1, chain_id=204)
        return hashlib.sha256(json.dumps(settings, sort_keys=True).encode()).hexdigest()

    def serializable(self):
        settings = asdict(self)
        settings["dataset"] = str(self.dataset)
        return settings


def load(path, **overrides):
    settings = {}
    if path:
        with Path(path).open("rb") as handle:
            settings = tomllib.load(handle)
        for key in ("dataset", "labels"):
            if settings.get(key) and not Path(settings[key]).is_absolute():
                settings[key] = str(Path(path).resolve().parent / settings[key])
    settings.update({key: value for key, value in overrides.items() if value is not None})
    return Config(**settings)
