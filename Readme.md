# S3 GFS Retention

## Configuration

These environment variables control the Lambda behavior.

- `S3_BUCKET` (required): S3 bucket name to scan.
- `S3_PREFIX` (optional): Prefix to filter objects.
- `AWS_REGION` (optional): AWS region for the S3 client.
- `AWS_ACCESS_KEY_ID` (optional): AWS access key for boto3 credentials.
- `AWS_SECRET_ACCESS_KEY` (optional): AWS secret key for boto3 credentials.
- `AWS_SESSION_TOKEN` (optional): AWS session token for boto3 credentials.
- `S3_GFS_REGEX` (required): Regex used to capture the timestamp substring in the first capture group.
- `S3_GFS_TIMESTAMP_FORMAT` (optional): `strptime` format for the captured timestamp (default: `%Y-%m-%dT%H:%M:%SZ`).
- `S3_GFS_KEEP_DAILY` (optional): Daily buckets to keep (default: `7`).
- `S3_GFS_KEEP_WEEKLY` (optional): Weekly buckets to keep (default: `4`).
- `S3_GFS_KEEP_MONTHLY` (optional): Monthly buckets to keep (default: `12`).
- `S3_GFS_DRY_RUN` (optional): If true, do not delete objects (default: `true`).
- `S3_GFS_MIN_REMAINING` (optional): Minimum objects to keep (default: `5`).
