from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import boto3

DecisionTuple = Tuple[str, str, str]  # (key, decision, tag)


@dataclass(frozen=True)
class RetentionPolicy:
    keep_daily: int = 7
    keep_weekly: int = 4
    keep_monthly: int = 12


def parse_timestamp_from_key(
    key: str,
    filename_ts_re: re.Pattern[str],
    timestamp_format: str,
) -> Optional[datetime]:
    """
    Parse a UTC timestamp from the object key using the provided regex.
    Returns timezone-aware UTC datetime if match; otherwise None.
    """
    m = filename_ts_re.search(key)
    if not m:
        return None
    try:
        ts = m.group(1)
        dt = datetime.strptime(ts, timestamp_format)
        return dt.replace(tzinfo=timezone.utc)
    except (ValueError, IndexError):
        return None


def fetch_from_s3(bucket: str, prefix: str = "", region: Optional[str] = None) -> List[str]:
    session = boto3.session.Session(region_name=region) if region else boto3.session.Session()
    s3 = session.client("s3")

    out: List[str] = []
    token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        for item in resp.get("Contents", []):
            out.append(item["Key"])

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return out


def core_logic(
    keys: Iterable[str],
    policy: RetentionPolicy,
    *,
    filename_ts_re: re.Pattern[str],
    timestamp_format: str,
) -> List[DecisionTuple]:
    """
    Receives object keys, parses timestamps from names, and returns decisions:
      (key, "keep"/"remove", tag)

    Tags:
      hourly/daily/weekly/monthly/unparsed/none

    Conservative behavior:
      - unparsed timestamps => keep (tag=unparsed)
    """
    parsed: List[Tuple[str, Optional[datetime]]] = [
        (k, parse_timestamp_from_key(k, filename_ts_re, timestamp_format)) for k in keys
    ]

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

    def day_bucket(dt: datetime) -> Tuple[int, int, int]:
        return (dt.year, dt.month, dt.day)

    def iso_week_bucket(dt: datetime) -> Tuple[int, int]:
        iso = dt.isocalendar()
        return (iso.year, iso.week)

    def month_bucket(dt: datetime) -> Tuple[int, int]:
        return (dt.year, dt.month)

    # Priority: most specific first
    select(day_bucket, policy.keep_daily, "daily")
    select(iso_week_bucket, policy.keep_weekly, "weekly")
    select(month_bucket, policy.keep_monthly, "monthly")

    # Output list: newest->oldest for parsed items, then unparsed (sorted)
    out: List[DecisionTuple] = []
    for k, _dt in parsed_items:
        if k in keepers:
            out.append((k, "keep", keepers[k]))
        else:
            out.append((k, "remove", ""))

    for k in sorted(unparsed_keys):
        out.append((k, "keep", "unparsed"))

    return out


def apply_removal(
    bucket: str,
    decisions: List[DecisionTuple],
    *,
    region: Optional[str] = None,
    min_remaining: int = 5,
    dry_run: bool = True,
) -> dict:
    """
    Applies deletions for entries marked "remove", in the order provided.

    Core logic must supply decisions ordered oldest-first if that is desired.

    Safety:
      - Maintains a running count of remaining objects.
      - Before deleting another object, if remaining <= min_remaining, aborts the loop.

    Returns a dict suitable for logging:
      { "total": int, "deleted": int, "skipped": bool, "reason": str, "deleted_keys": [...] }
    """
    total = len(decisions)
    remaining = total

    if total <= min_remaining:
        return {
            "total": total,
            "deleted": 0,
            "skipped": True,
            "reason": f"Only {total} objects exist (<= {min_remaining}). No deletions performed.",
            "deleted_keys": [],
        }

    # Plan deletions in-order, aborting once we'd hit the safety floor
    keys_to_delete: List[str] = []
    for key, decision, _tag in decisions:
        if decision != "remove":
            continue

        # If we delete this one, remaining decreases by 1.
        next_remaining = remaining - 1
        if next_remaining <= min_remaining:
            break

        keys_to_delete.append(key)
        remaining = next_remaining

    if not keys_to_delete:
        return {
            "total": total,
            "deleted": 0,
            "skipped": False,
            "reason": f"No deletions selected (would hit safety floor of {min_remaining}).",
            "deleted_keys": [],
        }

    if dry_run:
        return {
            "total": total,
            "deleted": len(keys_to_delete),
            "skipped": False,
            "reason": "Dry run; deletions not executed.",
            "deleted_keys": keys_to_delete,
        }

    session = boto3.session.Session(region_name=region) if region else boto3.session.Session()
    s3 = session.client("s3")

    # Batch delete (max 1000 keys per call)
    for i in range(0, len(keys_to_delete), 1000):
        chunk = keys_to_delete[i : i + 1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
        )

    return {
        "total": total,
        "deleted": len(keys_to_delete),
        "skipped": False,
        "reason": "Deletions executed.",
        "deleted_keys": keys_to_delete,
    }


def main() -> dict:
    bucket = os.environ["S3_BUCKET"]
    prefix = os.environ.get("S3_PREFIX", "")
    region = os.environ.get("AWS_REGION")
    regex_value = os.environ.get("S3_GFS_REGEX")
    if not regex_value:
        raise RuntimeError("S3_GFS_REGEX is required and must include a capture group.")
    try:
        filename_ts_re = re.compile(regex_value)
    except re.error as exc:
        raise RuntimeError("S3_GFS_REGEX is invalid.") from exc
    timestamp_format = os.environ.get("S3_GFS_TIMESTAMP_FORMAT", "%Y-%m-%dT%H:%M:%SZ")

    # Defaults are the policy you described; tweak via env if desired.
    policy = RetentionPolicy(
        keep_daily=int(os.environ.get("S3_GFS_KEEP_DAILY", "7")),
        keep_weekly=int(os.environ.get("S3_GFS_KEEP_WEEKLY", "4")),
        keep_monthly=int(os.environ.get("S3_GFS_KEEP_MONTHLY", "12")),
    )

    dry_run = os.environ.get("S3_GFS_DRY_RUN", "true").lower() in ("1", "true", "yes", "y")
    min_remaining = int(os.environ.get("S3_GFS_MIN_REMAINING", "5"))

    keys = fetch_from_s3(bucket=bucket, prefix=prefix, region=region)
    decisions = core_logic(
        keys,
        policy,
        filename_ts_re=filename_ts_re,
        timestamp_format=timestamp_format,
    )

    # Apply deletions
    result = apply_removal(
        bucket=bucket,
        decisions=decisions,
        region=region,
        min_remaining=min_remaining,
        dry_run=dry_run,
    )

    # If you run in Lambda, printing is captured by CloudWatch
    print(
        {
            "bucket": bucket,
            "prefix": prefix,
            "dry_run": dry_run,
            "policy": policy.__dict__,
            "result": result,
        }
    )

    return result


# Lambda handler compatibility
def handler(event, context):
    return main()


if __name__ == "__main__":
    main()
