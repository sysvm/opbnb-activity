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
ANALYSIS_SETTINGS = ("analysis_memory_limit_mb", "analysis_threads", "analysis_temp_directory")


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
    blocks_per_checkpoint: int = 500
    timeout_seconds: float = 30.0
    retries: int = 3
    analysis_memory_limit_mb: int = 8192
    analysis_threads: int = 2
    analysis_temp_directory: str | None = None
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
        if type(self.blocks_per_partition) is not int or not 1 <= self.blocks_per_partition <= 100000:
            raise ValueError("blocks_per_partition must be an integer in [1,100000]")
        if type(self.blocks_per_checkpoint) is not int or not 1 <= self.blocks_per_checkpoint <= 5000:
            raise ValueError("blocks_per_checkpoint must be an integer in [1,5000]")
        if (type(self.analysis_memory_limit_mb) is not int or not 64 <= self.analysis_memory_limit_mb <= 1048576
                or type(self.analysis_threads) is not int or not 1 <= self.analysis_threads <= 128
                or (self.analysis_temp_directory is not None and
                    (not isinstance(self.analysis_temp_directory, str) or not self.analysis_temp_directory.strip()))):
            raise ValueError("Invalid analysis resource settings")
        if not math.isfinite(self.qps) or self.qps <= 0 or not 1 <= self.batch_size <= 100 or self.retries < 0 or self.timeout_seconds <= 0:
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
        for key in ("dataset", "run_id", "qps", "concurrency", "batch_size", "timeout_seconds", "retries", "labels", "assets", "blocks_per_checkpoint", *ANALYSIS_SETTINGS):
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
        for key in ("dataset", "labels", "analysis_temp_directory"):
            if key == "analysis_temp_directory" and settings.get(key) is not None and (
                    not isinstance(settings[key], str) or not settings[key].strip()):
                raise ValueError("Invalid analysis resource settings")
            if settings.get(key) and not Path(settings[key]).is_absolute():
                settings[key] = str(Path(path).resolve().parent / settings[key])
    settings.update({key: value for key, value in overrides.items() if value is not None})
    return Config(**settings)
