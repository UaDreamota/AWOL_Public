"""Download missing court decisions using Playwright.

This script reads an index CSV produced by the pwdownloader_*
utilities and downloads any decision pages that are missing from
the target directory. Each successful or skipped download is
logged to a CSV file for bookkeeping.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path
import re
from typing import Iterable
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PlayTimeout, Error as PlayError


BASE_URL = "https://reyestr.court.gov.ua"


def sanitize_id(value: str) -> str:
    """Return a filesystem-safe identifier."""
    return re.sub(r"[^A-Za-z0-9_-]", "_", value)


def existing_ids(download_dir: Path) -> set[str]:
    """Return sanitized decision IDs already present in *download_dir*."""
    return {p.stem for p in download_dir.glob("*.html")}


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[2]
    default_dir = repo_root / "data" / "awol_court"

    parser = argparse.ArgumentParser(description="Fetch missing court decisions")
    parser.add_argument(
        "--index-csv",
        type=Path,
        default=default_dir / "output.csv",
        help="CSV produced by pwdownloader_* containing decision IDs and links",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=default_dir / "html",
        help="Directory where HTML files are stored",
    )
    parser.add_argument(
        "--headless",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run browser in headless mode (default: True)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retries for network failures",
    )
    return parser.parse_args()


DECISION_ID_FIELDS: Iterable[str] = [
    "№ рішення",
    "decision_id",
    "Decision ID",
    "id",
]
LINK_FIELDS: Iterable[str] = [
    "Посилання",
    "link",
    "Link",
    "url",
]


def get_field(row: dict, choices: Iterable[str]) -> str:
    for key in choices:
        if key in row and row[key]:
            return row[key]
    return ""


def fetch_print_view(page, url: str) -> str:
    """Navigate to *url* and return HTML of its print view if available."""
    page.goto(url, timeout=60_000, wait_until="load")
    link = page.locator("a[href*='print']")
    if link.count() > 0:
        href = link.first.get_attribute("href")
        if href:
            page.goto(urljoin(url, href), timeout=60_000, wait_until="load")
    return page.content()


def main() -> None:
    args = parse_args()
    download_dir: Path = args.download_dir
    download_dir.mkdir(parents=True, exist_ok=True)

    log_path = download_dir / "fetch_log.csv"
    log_exists = log_path.exists()
    log_file = log_path.open("a", newline="", encoding="utf-8")
    log_writer = csv.DictWriter(log_file, fieldnames=["decision_id", "status", "timestamp"])
    if not log_exists:
        log_writer.writeheader()

    have = existing_ids(download_dir)

    with args.index_csv.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=args.headless)
        page = browser.new_page()
        for row in rows:
            decision_id = get_field(row, DECISION_ID_FIELDS)
            link = get_field(row, LINK_FIELDS)
            if not decision_id or not link:
                continue
            if link.startswith("/"):
                link = urljoin(BASE_URL, link)
            sid = sanitize_id(decision_id)
            timestamp = datetime.utcnow().isoformat()
            if sid in have:
                log_writer.writerow({"decision_id": decision_id, "status": "skipped", "timestamp": timestamp})
                continue
            status = "failed"
            for attempt in range(1, args.max_retries + 1):
                try:
                    html = fetch_print_view(page, link)
                    (download_dir / f"{sid}.html").write_text(html, encoding="utf-8")
                    status = "downloaded"
                    have.add(sid)
                    break
                except (PlayTimeout, PlayError, Exception):
                    if attempt == args.max_retries:
                        status = "failed"
            log_writer.writerow({"decision_id": decision_id, "status": status, "timestamp": timestamp})
        browser.close()
    log_file.close()


if __name__ == "__main__":
    main()
