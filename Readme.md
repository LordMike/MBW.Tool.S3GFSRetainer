# S3 GFS Retention

Prune S3 backups using a simple GFS-style policy (daily, weekly, monthly). The
script groups objects by timestamp extracted from the key, so retention and
minimum counts are applied to complete backup groups (for example, a `.tar` plus
its `.metadata.json`).

## How it works

- Every object key is matched against `S3_GFS_REGEX`.
- Keys that do not match are ignored and never deleted.
- Matching keys are grouped by their timestamp and retained by:
  - `S3_GFS_KEEP_DAILY`: newest N unique days
  - `S3_GFS_KEEP_WEEKLY`: newest N ISO weeks
  - `S3_GFS_KEEP_MONTHLY`: newest N months
- Deletions happen oldest-first and stop once `S3_GFS_MIN_REMAINING` groups
  would be violated.
- Any S3 delete error raises an exception and stops the run.

If your bucket has versioning enabled, this script only creates delete markers.
It does not delete historical versions.

## Configuration

These environment variables control the Lambda behavior.

- `S3_BUCKET` (required): S3 bucket name to scan.
- `S3_PREFIX` (optional): Prefix to filter objects.
- `AWS_REGION` (optional): AWS region for the S3 client.
- `AWS_ACCESS_KEY_ID` (optional): AWS access key for boto3 credentials.
- `AWS_SECRET_ACCESS_KEY` (optional): AWS secret key for boto3 credentials.
- `AWS_SESSION_TOKEN` (optional): AWS session token for boto3 credentials.
- `S3_GFS_REGEX` (required): Regex used to capture the timestamp substring in
  a single capture group.
- `S3_GFS_TIMESTAMP_FORMAT` (optional): `strptime` format for the captured
  timestamp (default: `%Y-%m-%dT%H:%M:%SZ`).
- `S3_GFS_KEEP_DAILY` (optional): Daily buckets to keep (default: `7`).
- `S3_GFS_KEEP_WEEKLY` (optional): Weekly buckets to keep (default: `4`).
- `S3_GFS_KEEP_MONTHLY` (optional): Monthly buckets to keep (default: `12`).
- `S3_GFS_DRY_RUN` (optional): If true, do not delete objects (default: `true`).
- `S3_GFS_MIN_REMAINING` (optional): Minimum backup groups to keep
  (default: `5`).

## Example regex

Use any naming scheme you prefer, as long as the timestamp can be captured in a
single regex group and parsed by `strptime`. See Python's format codes:
[strftime/strptime format codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes)

## Home Assistant example

Home Assistant backup names often look like:

```
Automatic_backup_2026.1.0_2026-01-09_04.45_52003641.tar
Automatic_backup_2026.1.0_2026-01-09_04.45_52003641.metadata.json
```

For that format, set:

```
S3_GFS_REGEX=Automatic_backup_\d+\.\d+\.\d+_(\d{4}-\d{2}-\d{2}_\d{2}\.\d{2})_
S3_GFS_TIMESTAMP_FORMAT=%Y-%m-%d_%H.%M
```

## Dependencies

- Python 3.10+.
- `boto3`.

## Run locally

```
python main.py
```

Use environment variables to configure the run. The script prints a summary
dict to stdout and returns it (useful in Lambda).

## Deployment options

This script can run anywhere you can set environment variables and reach S3,
but it is designed to run inside AWS Lambda based on an event trigger.

For Lambda:

- Set the handler to `main.handler`.
- Trigger it with any event source you prefer (for example, an EventBridge
  schedule).
- Ensure the Lambda role has `s3:ListBucket` and `s3:DeleteObject` on the target
  bucket/prefix.

## Tests

```
pytest
```
