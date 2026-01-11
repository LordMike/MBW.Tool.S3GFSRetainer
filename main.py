from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from typing import Dict, Iterable, List, Optional, Tuple

import boto3

DecisionTuple = Tuple[str, str, str]  # (key, decision, tag) decision=keep/remove/ignore
logger = logging.getLogger(__name__)


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
    logger.info("Listing objects from s3://%s/%s", bucket, prefix)
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

    logger.info("Listed %d object keys", len(out))
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
      daily/weekly/monthly/unparsed/none

    Conservative behavior:
      - unparsed timestamps => ignore (tag=unparsed)
    """
    parsed_items: List[Tuple[int, str, datetime]] = []
    unparsed_items: List[Tuple[int, str]] = []
    for idx, k in enumerate(keys):
        dt = parse_timestamp_from_key(k, filename_ts_re, timestamp_format)
        if dt is None:
            unparsed_items.append((idx, k))
        else:
            parsed_items.append((idx, k, dt))

    if not parsed_items:
        logger.info("All %d keys ignored (no timestamp match)", len(unparsed_items))
        return [
            (k, "ignore", "unparsed")
            for _idx, k in sorted(unparsed_items, key=lambda x: x[0])
        ]

    groups: Dict[datetime, List[Tuple[int, str]]] = {}
    for idx, k, dt in parsed_items:
        groups.setdefault(dt, []).append((idx, k))

    logger.info(
        "Parsed %d keys into %d timestamp groups (%d unparsed)",
        len(parsed_items),
        len(groups),
        len(unparsed_items),
    )

    # Newest -> oldest for selection
    group_dts_newest = sorted(groups.keys(), reverse=True)
    keepers: Dict[datetime, set] = {}  # timestamp -> tags

    def select(bucket_fn, keep_n: int, tag: str) -> None:
        if keep_n <= 0:
            return
        seen = set()
        for dt in group_dts_newest:
            bucket = bucket_fn(dt)
            if bucket in seen:
                continue
            seen.add(bucket)
            keepers.setdefault(dt, set()).add(tag)
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

    # Output list: oldest->newest for parsed items, then unparsed (original order)
    out: List[DecisionTuple] = []
    group_dts_oldest = sorted(groups.keys())
    tag_order = ("monthly", "weekly", "daily")
    for dt in group_dts_oldest:
        keys_with_idx = sorted(groups[dt], key=lambda x: x[0])
        if dt in keepers:
            tags = keepers[dt]
            tag = ",".join(t for t in tag_order if t in tags)
            decision = "keep"
        else:
            tag = ""
            decision = "remove"
        for _idx, k in keys_with_idx:
            out.append((k, decision, tag))

    for _idx, k in sorted(unparsed_items, key=lambda x: x[0]):
        out.append((k, "ignore", "unparsed"))

    counts = {"keep": 0, "remove": 0, "ignore": 0}
    for _k, decision, _tag in out:
        counts[decision] += 1
    logger.info(
        "Decisions: keep=%d remove=%d ignore=%d",
        counts["keep"],
        counts["remove"],
        counts["ignore"],
    )

    return out


def apply_removal(
    bucket: str,
    decisions: List[DecisionTuple],
    *,
    filename_ts_re: re.Pattern[str],
    timestamp_format: str,
    region: Optional[str] = None,
    min_remaining: int = 5,
    dry_run: bool = True,
) -> dict:
    """
    Applies deletions for entries marked "remove", grouped by timestamp.

    Deletion order is oldest-first by timestamp.

    Safety:
      - Maintains a running count of remaining backup groups (unique timestamps).
      - Before deleting another group, if remaining <= min_remaining, aborts the loop.

    Returns a dict suitable for logging:
      {
        "total": int,
        "total_groups": int,
        "deleted": int,
        "deleted_groups": int,
        "skipped": bool,
        "reason": str,
        "deleted_keys": [...]
      }
    """
    total_objects = len(decisions)
    groups: Dict[datetime, List[str]] = {}
    group_decisions: Dict[datetime, str] = {}

    for key, decision, _tag in decisions:
        if decision == "ignore":
            continue
        dt = parse_timestamp_from_key(key, filename_ts_re, timestamp_format)
        if dt is None:
            raise RuntimeError(f"Expected timestamp for key but none found: {key}")
        if dt not in groups:
            groups[dt] = []
            group_decisions[dt] = decision
        elif group_decisions[dt] != decision:
            raise RuntimeError(f"Inconsistent decisions for timestamp group {dt.isoformat()}")
        groups[dt].append(key)

    total_groups = len(groups)
    remaining_groups = total_groups

    if total_groups <= min_remaining:
        logger.info(
            "Safety floor hit before deletes: total_groups=%d min_remaining=%d",
            total_groups,
            min_remaining,
        )
        return {
            "total": total_objects,
            "total_groups": total_groups,
            "deleted": 0,
            "deleted_groups": 0,
            "skipped": True,
            "reason": (
                f"Only {total_groups} backup groups exist (<= {min_remaining}). "
                "No deletions performed."
            ),
            "deleted_keys": [],
        }

    # Plan deletions in-order, aborting once we'd hit the safety floor
    keys_to_delete: List[str] = []
    deleted_groups = 0
    for dt in sorted(groups.keys()):
        if group_decisions[dt] != "remove":
            continue

        # If we delete this group, remaining decreases by 1.
        next_remaining = remaining_groups - 1
        if next_remaining < min_remaining:
            logger.info(
                "Stopping deletes at min_remaining=%d (remaining_groups=%d)",
                min_remaining,
                remaining_groups,
            )
            break

        keys_to_delete.extend(groups[dt])
        remaining_groups = next_remaining
        deleted_groups += 1

    if not keys_to_delete:
        logger.info(
            "No deletions selected after planning (min_remaining=%d)", min_remaining
        )
        return {
            "total": total_objects,
            "total_groups": total_groups,
            "deleted": 0,
            "deleted_groups": 0,
            "skipped": False,
            "reason": (
                f"No deletions selected (would hit safety floor of {min_remaining} backup "
                "groups)."
            ),
            "deleted_keys": [],
        }

    if dry_run:
        for key in keys_to_delete:
            logger.info("DRY RUN delete s3://%s/%s", bucket, key)
        return {
            "total": total_objects,
            "total_groups": total_groups,
            "deleted": len(keys_to_delete),
            "deleted_groups": deleted_groups,
            "skipped": False,
            "reason": "Dry run; deletions not executed.",
            "deleted_keys": keys_to_delete,
        }

    session = boto3.session.Session(region_name=region) if region else boto3.session.Session()
    s3 = session.client("s3")

    # Batch delete (max 1000 keys per call)
    for i in range(0, len(keys_to_delete), 1000):
        chunk = keys_to_delete[i : i + 1000]
        for key in chunk:
            logger.info("Deleting s3://%s/%s", bucket, key)
        response = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
        )
        errors = response.get("Errors", [])
        if errors:
            raise RuntimeError(f"S3 delete_objects reported errors: {errors}")

    return {
        "total": total_objects,
        "total_groups": total_groups,
        "deleted": len(keys_to_delete),
        "deleted_groups": deleted_groups,
        "skipped": False,
        "reason": "Deletions executed.",
        "deleted_keys": keys_to_delete,
    }


def main() -> dict:
    logging.basicConfig(
        level=os.environ.get("S3_GFS_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger.info("S3 GFS retention run started")
    try:
        bucket = os.environ["S3_BUCKET"]
        prefix = os.environ.get("S3_PREFIX", "")
        region = os.environ.get("AWS_REGION")
        regex_value = os.environ.get("S3_GFS_REGEX")
        if not regex_value:
            raise RuntimeError(
                "S3_GFS_REGEX is required and must contain exactly one capture group."
            )
        filename_ts_re = re.compile(regex_value)
        if filename_ts_re.groups != 1:
            raise RuntimeError("S3_GFS_REGEX must contain exactly one capture group.")
        timestamp_format = os.environ.get(
            "S3_GFS_TIMESTAMP_FORMAT", "%Y-%m-%dT%H:%M:%SZ"
        )

        # Defaults are the policy you described; tweak via env if desired.
        policy = RetentionPolicy(
            keep_daily=int(os.environ.get("S3_GFS_KEEP_DAILY", "7")),
            keep_weekly=int(os.environ.get("S3_GFS_KEEP_WEEKLY", "4")),
            keep_monthly=int(os.environ.get("S3_GFS_KEEP_MONTHLY", "12")),
        )

        dry_run = os.environ.get("S3_GFS_DRY_RUN", "true").lower() in (
            "1",
            "true",
            "yes",
            "y",
        )
        min_remaining = int(os.environ.get("S3_GFS_MIN_REMAINING", "5"))

        logger.info(
            "Config bucket=%s prefix=%s dry_run=%s min_remaining=%d keep_daily=%d keep_weekly=%d keep_monthly=%d",
            bucket,
            prefix,
            dry_run,
            min_remaining,
            policy.keep_daily,
            policy.keep_weekly,
            policy.keep_monthly,
        )

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
            filename_ts_re=filename_ts_re,
            timestamp_format=timestamp_format,
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

        logger.info("S3 GFS retention run completed")
        return result
    except Exception:
        logger.exception("S3 GFS retention run failed")
        raise


# Lambda handler compatibility
def handler(event, context):
    return main()


if __name__ == "__main__":
    main()
