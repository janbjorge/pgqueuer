"""Fetch the newest benchmark-history artifact, or bootstrap from the public archive."""

from __future__ import annotations

import argparse
import io
import os
import subprocess
import tempfile
import urllib.request
import zipfile
from http.client import HTTPMessage
from pathlib import Path
from typing import IO, Protocol
from urllib.error import HTTPError

from pydantic import AwareDatetime, BaseModel

from tools.benchmark_data import load_json_dir, merge_rows, write_monthly_ndjson

ARTIFACT_NAME = "benchmark-history"
API_ROOT = "https://api.github.com"
BOOTSTRAP_REPO_URL = "https://github.com/janbjorge/artifacts-storage"
BOOTSTRAP_SUBDIR = "pgqueuer/benchmark"


class Artifact(BaseModel):
    name: str
    expired: bool
    created_at: AwareDatetime
    archive_download_url: str


class ArtifactListing(BaseModel):
    artifacts: list[Artifact]


class HttpGet(Protocol):
    def __call__(self, url: str) -> bytes: ...


class RedirectCapture(urllib.request.HTTPRedirectHandler):
    """Refuse redirects so the 302 surfaces as an HTTPError with the Location header."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> urllib.request.Request | None:
        return None


def github_get(url: str) -> bytes:
    request = urllib.request.Request(url)
    if token := os.environ.get("GITHUB_TOKEN"):
        request.add_header("Authorization", f"Bearer {token}")
    request.add_header("X-GitHub-Api-Version", "2022-11-28")
    # The artifact zip endpoint 302-redirects to a pre-signed blob URL that
    # rejects requests carrying the Authorization header, so follow manually.
    opener = urllib.request.build_opener(RedirectCapture)
    try:
        with opener.open(request) as response:
            return bytes(response.read())
    except HTTPError as error:
        if error.code == 302 and (location := error.headers.get("Location")):
            with urllib.request.urlopen(location) as response:
                return bytes(response.read())
        raise


def newest_artifact(listing: ArtifactListing) -> Artifact | None:
    candidates = [
        artifact
        for artifact in listing.artifacts
        if artifact.name == ARTIFACT_NAME and not artifact.expired
    ]
    return max(candidates, key=lambda artifact: artifact.created_at, default=None)


def extract_history(zip_bytes: bytes, dest: Path) -> list[Path]:
    history_dir = dest / "benchmark"
    history_dir.mkdir(parents=True, exist_ok=True)
    extracted = list[Path]()
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        for member in archive.namelist():
            if member.endswith(".ndjson"):
                target = history_dir / Path(member).name
                target.write_bytes(archive.read(member))
                extracted.append(target)
    return extracted


def convert_archive(archive_dir: Path, dest: Path) -> list[Path]:
    rows = merge_rows([], load_json_dir(archive_dir))
    return write_monthly_ndjson(rows, dest)


def bootstrap(dest: Path) -> list[Path]:
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(
            ["git", "clone", "--depth", "1", BOOTSTRAP_REPO_URL, tmp],
            check=True,
        )
        return convert_archive(Path(tmp) / BOOTSTRAP_SUBDIR, dest)


def fetch(dest: Path, get: HttpGet) -> None:
    repository = os.environ.get("GITHUB_REPOSITORY")
    artifact = None
    if repository:
        listing = ArtifactListing.model_validate_json(
            get(f"{API_ROOT}/repos/{repository}/actions/artifacts?name={ARTIFACT_NAME}")
        )
        artifact = newest_artifact(listing)

    if artifact is not None:
        extracted = extract_history(get(artifact.archive_download_url), dest)
        print(f"Fetched {ARTIFACT_NAME} from {artifact.created_at}: {len(extracted)} files.")
    else:
        written = bootstrap(dest)
        print(f"No {ARTIFACT_NAME} artifact found; bootstrapped {len(written)} files.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dest", type=Path, required=True)
    args = parser.parse_args()
    fetch(args.dest, github_get)


if __name__ == "__main__":
    main()
