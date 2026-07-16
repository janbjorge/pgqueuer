"""Append fresh benchmark results to the yearly NDJSON history files, idempotently."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Protocol

import boto3

from tools.benchmark_data import load_json_dir, load_ndjson_dir, merge_rows, write_yearly_ndjson

DEFAULT_ENDPOINT = "https://fly.storage.tigris.dev"
BUCKET_PREFIX = "benchmark/"


class ObjectStorage(Protocol):
    """The slice of the boto3 S3 client this module uses."""

    def list_objects_v2(self, *, Bucket: str, Prefix: str) -> dict[str, list[dict[str, str]]]: ...

    def download_file(self, Bucket: str, Key: str, Filename: str) -> None: ...

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None: ...


def make_client() -> ObjectStorage:
    client: ObjectStorage = boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3", DEFAULT_ENDPOINT),
        region_name=os.environ.get("AWS_REGION", "auto"),
    )
    return client


def append(data_dir: Path, new_dir: Path) -> list[Path]:
    existing = load_ndjson_dir(data_dir)
    merged = merge_rows(existing, load_json_dir(new_dir))
    written = write_yearly_ndjson(merged, data_dir)
    print(f"Appended {len(merged) - len(existing)} rows, total {len(merged)}.")
    return written


def append_via_bucket(
    client: ObjectStorage,
    bucket: str,
    data_dir: Path,
    new_dir: Path,
) -> None:
    history_dir = data_dir / "benchmark"
    history_dir.mkdir(parents=True, exist_ok=True)
    listing = client.list_objects_v2(Bucket=bucket, Prefix=BUCKET_PREFIX)
    for obj in listing.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".ndjson"):
            client.download_file(bucket, key, str(history_dir / Path(key).name))

    for file in append(data_dir, new_dir):
        client.upload_file(str(file), bucket, f"{BUCKET_PREFIX}{file.name}")
        print(f"Uploaded {BUCKET_PREFIX}{file.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--new-dir", type=Path, required=True)
    parser.add_argument("--bucket", type=str, default=None)
    args = parser.parse_args()
    if args.bucket:
        append_via_bucket(make_client(), args.bucket, args.data_dir, args.new_dir)
    else:
        for file in append(args.data_dir, args.new_dir):
            print(file)


if __name__ == "__main__":
    main()
