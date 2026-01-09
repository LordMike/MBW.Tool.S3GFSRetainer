from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

DecisionTuple = Tuple[str, str, str]  # (key, decision, tag)


@dataclass(frozen=True)
class RetentionPolicy:
    keep_hourly: int = 0
    keep_daily: int = 7
    keep_weekly: int = 4
    keep_monthly: int = 12


# Single regex for timestamps in filenames.
# Update this to match your real HA backup naming.
#
# Current expectation: ...YYYY-MM-DDTHHMMSSZ... or ...YYYY-MM-DDTHH:MM:SSZ...
#
# Groups required: y m d H M S
FILENAME_TS_RE = re.compile(
    r"(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})"
    r"[Tt]"
    r"(?P<H>\d{2})[:_]?(?P<M>\d{2})[:_]?(?P<S>\d{2})"
    r"Z"
)


def parse_timestamp_from_key(key: str) -> Optional[datetime]:
    """
    Parse a UTC timestamp from the object key using FILENAME_TS_RE.
    Returns timezone-aware UTC datetime if match; otherwise None.
    """
    m = FILENAME_TS_RE.search(key)
    if not m:
        return None
    try:
        return datetime(
            int(m.group("y")),
            int(m.group("m")),
            int(m.group("d")),
            int(m.group("H")),
            int(m.group("M")),
            int(m.group("S")),
            tzinfo=timezone.utc,
        )
    except ValueError:
        return None


def decide_gfs(keys: Iterable[str], policy: RetentionPolicy) -> List[DecisionTuple]:
    """
    Receives object keys, parses timestamps from names, and returns decisions:
      (key, "keep"/"remove", tag)

    Tags:
      hourly/daily/weekly/monthly/unparsed/none

    Conservative behavior:
      - unparsed timestamps => keep (tag=unparsed)
    """

    parsed: List[Tuple[str, Optional[datetime]]] = [(k, parse_timestamp_from_key(k)) for k in keys]

    # Keep unparsed objects for safety
    unparsed_keys = {k for k, dt in parsed if dt is None}

    parsed_items: List[Tuple[str, datetime]] = [(k, dt) for k, dt in parsed if dt is not None]
    if not parsed_items:
        return [(k, "keep", "unparsed") for k, _ in parsed]

    # Newest -> oldest
    parsed_items.sort(key=lambda x: x[1], reverse=True)

    keepers: Dict[str, str] = {}  # key -> tag

    def select(bucket_fn, keep_n: int, tag: str) -> None:
        if keep_n <= 0:
            return
        seen = set()
        for k, dt in parsed_items:
            b = bucket_fn(dt)
            if b in seen:
                continue
            seen.add(b)
            if k not in keepers:
                keepers[k] = tag
            if len(seen) >= keep_n:
                break

    def hour_bucket(dt: datetime) -> Tuple[int, int, int, int]:
        return (dt.year, dt.month, dt.day, dt.hour)

    def day_bucket(dt: datetime) -> Tuple[int, int, int]:
        return (dt.year, dt.month, dt.day)

    def iso_week_bucket(dt: datetime) -> Tuple[int, int]:
        iso = dt.isocalendar()
        return (iso.year, iso.week)

    def month_bucket(dt: datetime) -> Tuple[int, int]:
        return (dt.year, dt.month)

    # Priority: most specific first
    select(hour_bucket, policy.keep_hourly, "hourly")
    select(day_bucket, policy.keep_daily, "daily")
    select(iso_week_bucket, policy.keep_weekly, "weekly")
    select(month_bucket, policy.keep_monthly, "monthly")

    # Output list: newest->oldest for parsed items, then unparsed (sorted)
    out: List[DecisionTuple] = []
    for k, _dt in parsed_items:
        if k in keepers:
            out.append((k, "keep", keepers[k]))
        else:
            out.append((k, "remove", "none"))

    for k in sorted(unparsed_keys):
        out.append((k, "keep", "unparsed"))

    return out
