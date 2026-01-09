from __future__ import annotations

from typing import List, Optional

import boto3

from .core import DecisionTuple


def apply_deletions(
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
