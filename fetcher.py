from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import boto3


@dataclass(frozen=True)
class S3ObjectInfo:
    key: str


def list_s3_keys(bucket: str, prefix: str = "", region: Optional[str] = None) -> List[S3ObjectInfo]:
    session = boto3.session.Session(region_name=region) if region else boto3.session.Session()
    s3 = session.client("s3")

    out: List[S3ObjectInfo] = []
    token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        for item in resp.get("Contents", []):
            out.append(S3ObjectInfo(key=item["Key"]))

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return out
