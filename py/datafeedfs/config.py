"""Configuration, read from a TOML file. See config.example.toml."""
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class RemoteConfig:
    ssh_host: str                      # user@host; empty means the pcap files are on the local machine
    pcap_path: str                     # path of one day's pcap with {yyyy} {mm} {dd} placeholders
    ssh_options: list[str] = field(default_factory=lambda: ["-o", "BatchMode=yes", "-o", "ServerAliveInterval=30"])


@dataclass
class ParserConfig:
    command: list[str]                 # run once per pass with the pcap on stdin, see parsers/parse_nasdaq_itch.sh
    env: dict[str, str] = field(default_factory=dict)


@dataclass
class ExtractConfig:
    batch_window_seconds: float = 3.0  # requests for the same day within this window share one pass over the pcap
    max_parallel: int = 2              # passes over different days running at once
    on_miss: str = "block"             # "block": open() waits for the extraction; "async": open() fails with EAGAIN and extracts in the background
    whole_day: bool = False            # a miss extracts every symbol of that day in the same pass: the pcap is read once per day


@dataclass
class Config:
    remote: RemoteConfig
    parser: ParserConfig
    cache_dir: Path
    extract: ExtractConfig = field(default_factory=ExtractConfig)
    days_ttl_seconds: float = 600.0

    def pcap_for(self, day: str) -> str:
        yyyy, mm, dd = day.split("-")
        return self.remote.pcap_path.format(yyyy=yyyy, mm=mm, dd=dd)

    def day_glob(self) -> str:
        return self.remote.pcap_path.format(yyyy="[0-9][0-9][0-9][0-9]", mm="[0-9][0-9]", dd="[0-9][0-9]")

    def day_regex(self) -> re.Pattern:
        pattern = re.escape(self.remote.pcap_path)
        for name, digits in (("yyyy", 4), ("mm", 2), ("dd", 2)):
            pattern = pattern.replace(re.escape("{" + name + "}"), f"(?P<{name}>[0-9]{{{digits}}})")
        return re.compile(pattern + "$")


def load(path: str | Path) -> Config:
    path = Path(path).expanduser()
    raw = tomllib.loads(path.read_text())
    base = path.parent

    def resolve(token: str) -> str:  # relative script paths are relative to the config file
        candidate = (base / token).expanduser()
        return str(candidate) if not Path(token).is_absolute() and candidate.exists() else token

    command = raw["parser"]["command"]
    command = command.split() if isinstance(command, str) else list(command)
    return Config(
        remote=RemoteConfig(**raw["remote"]),
        parser=ParserConfig(command=[resolve(command[0])] + command[1:], env={key: str(value) for key, value in raw["parser"].get("env", {}).items()}),
        cache_dir=Path(raw.get("cache", {}).get("dir", "~/.cache/datafeedfs")).expanduser(),
        extract=ExtractConfig(**raw.get("extract", {})),
        days_ttl_seconds=float(raw.get("days_ttl_seconds", 600)),
    )
