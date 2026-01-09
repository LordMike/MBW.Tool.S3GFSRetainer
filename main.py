from __future__ import annotations

import os

from .applier import apply_deletions
from .core import RetentionPolicy, decide_gfs
from .fetcher import list_s3_keys


def main() -> dict:
    bucket = os.environ["S3_BUCKET"]
    prefix = os.environ.get("S3_PREFIX", "")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")

    # Defaults are the policy you described; tweak via env if desired.
    policy = RetentionPolicy(
        keep_hourly=int(os.environ.get("KEEP_HOURLY", "0")),
        keep_daily=int(os.environ.get("KEEP_DAILY", "7")),
        keep_weekly=int(os.environ.get("KEEP_WEEKLY", "4")),
        keep_monthly=int(os.environ.get("KEEP_MONTHLY", "12")),
    )

    dry_run = os.environ.get("DRY_RUN", "true").lower() in ("1", "true", "yes", "y")
    min_remaining = int(os.environ.get("MIN_REMAINING", "5"))

    objects = list_s3_keys(bucket=bucket, prefix=prefix, region=region)
    keys = [o.key for o in objects]

    decisions = decide_gfs(keys, policy)

    # Apply deletions
    result = apply_deletions(
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
