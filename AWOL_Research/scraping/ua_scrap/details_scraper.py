import asyncio
import json
import re
import csv
from pathlib import Path
from collections import deque

import aiofiles
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlayTimeout, Error as PlaywrightError

# ────────────────────────── paths & globals ───────────────────────────────── #
BASE_DIR     = Path(r"D:/Projects/GitRepositories/TheAWOLthing")
INPUT_FILE   = BASE_DIR / "scripts" / "soldiers.json"
OUT_JSON     = BASE_DIR / "data" / "soldat_info.ndjson"
OUT_CSV      = BASE_DIR / "data" / "soldat_info.csv"
CONCURRENCY  = 10      # parallel pages
MAX_ATTEMPTS = 5      # reload attempts before giving up

# Ensure output folder & files exist
OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# Initialize NDJSON file if missing
if not OUT_JSON.exists():
    OUT_JSON.write_text("", encoding="utf-8")

# Initialize CSV file with header if missing
CSV_FIELDS = [
    "detail_url", "name", "birth_date", "death_date", "burial_date",
    "birth_settlement", "birth_community", "birth_district", "birth_oblast",
    "death_settlement", "death_community", "death_district", "death_oblast",
    "rank", "military_unit", "sources", "is_missing"
]
if not OUT_CSV.exists() or OUT_CSV.stat().st_size == 0:
    with open(OUT_CSV, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()

write_lock = asyncio.Lock()
last_urls = deque(maxlen=10)

# ───────────────────────────────── helpers ─────────────────────────────────── #

def pick_text(tag):
    return tag.get_text(" ", strip=True) if tag else ""


def split_place(div):
    if not div:
        return ["", "", "", ""]
    parts = [a.get_text(strip=True) for a in div.select("a")]
    return (parts + ["", "", "", ""])[:4]


def parse_html(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1")
    header = pick_text(h1)
    name = header.split("(")[0].strip()
    dates = re.findall(
        r"(Jan\.|Feb\.|Mar\.|Apr\.|May|Jun\.|Jul\.|Aug\.|Sep\.|Oct\.|Nov\.)\s+\d{1,2},\s+\d{4}",
        header
    )

    def block(label):
        b_tag = soup.find("b", string=lambda t: t and t.strip() == label)
        if b_tag and b_tag.parent.name == 'div':
            return b_tag.parent
        return None

    birth_div  = block("Date of birth:")
    death_div  = block("Date of death:")
    burial_div = block("Date of burial:")
    from_div   = block("From:")
    rank_div   = block("Rank:")
    unit_div   = block("Military Unit:")

    disappeared_div = block("Disappeared in the area of:")
    if disappeared_div:
        is_missing = True
        loc_div = disappeared_div
    else:
        is_missing = False
        loc_div = block("Died in the area of:")

    b_set, b_com, b_dist, b_obl = split_place(from_div)
    d_set, d_com, d_dist, d_obl = split_place(loc_div)

    sources = [a["href"] for a in soup.select("div.source-links li a[href]")]

    return {
        "detail_url"      : url,
        "name"            : name,
        "birth_date"      : (pick_text(birth_div).replace("Date of birth:", "").strip() or (dates[0] if dates else "")),
        "death_date"      : (pick_text(death_div).replace("Date of death:", "").strip() or (dates[1] if len(dates) > 1 else "")),
        "burial_date"     : pick_text(burial_div).replace("Date of burial:", "").strip(),
        "birth_settlement": b_set,
        "birth_community" : b_com,
        "birth_district"  : b_dist,
        "birth_oblast"    : b_obl,
        "death_settlement": d_set,
        "death_community" : d_com,
        "death_district"  : d_dist,
        "death_oblast"    : d_obl,
        "rank"            : pick_text(rank_div).replace("Rank:", "").strip(),
        "military_unit"   : pick_text(unit_div).replace("Military Unit:", "").strip(),
        "sources"         : sources,
        "is_missing"      : is_missing
    }

# ─────────────────────── playwright + retry logic ────────────────────────── #

async def fetch_with_retry(page, url):
    last_rec = {"detail_url": url, "name": "", "sources": []}
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = await page.goto(url, timeout=60_000, wait_until="load")
            status = response.status if response else None
            if status and status >= 400:
                raise PlaywrightError(f"HTTP {status}")
            await page.wait_for_selector("h1", timeout=30_000)
            rec = parse_html(await page.content(), url)
            if not rec["name"] or rec["name"].startswith("Error"):
                last_rec = rec
                print(f"[!] retryable content at {url} (attempt {attempt}/{MAX_ATTEMPTS})")
            else:
                return rec
        except (PlayTimeout, PlaywrightError, Exception) as e:
            print(f"[!] error at {url} (attempt {attempt}/{MAX_ATTEMPTS}): {e}")
        delay = 5 if attempt <= 3 else 15
        print(f"    waiting {delay}s before retry")
        # reset page state
        try:
            await page.goto("about:blank")
        except:
            pass
        await asyncio.sleep(delay)
    print(f"[!] giving up on {url} after {MAX_ATTEMPTS} tries")
    return {"detail_url": url, "name": last_rec.get("name", ""), "sources": last_rec.get("sources", [])}

async def write_record(rec):
    url = rec["detail_url"]
    async with write_lock:
        if url in last_urls:
            print(f"[!] skipping duplicate {url} (in last 10)")
            return
        last_urls.append(url)
        async with aiofiles.open(OUT_JSON, 'a', encoding='utf-8') as f:
            await f.write(json.dumps(rec, ensure_ascii=False) + '\n')
        flat = {}
        for field in CSV_FIELDS:
            flat[field] = ';'.join(rec.get('sources', [])) if field == 'sources' else rec.get(field, '')
        with open(OUT_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writerow(flat)

async def worker(browser, queue):
    context = await browser.new_context()
    page = await context.new_page()
    try:
        while True:
            url = await queue.get()
            if url is None:
                queue.task_done()
                break
            rec = await fetch_with_retry(page, url)
            await write_record(rec)
            queue.task_done()
    finally:
        await page.close()
        await context.close()

async def main():
    items = json.loads(INPUT_FILE.read_text(encoding="utf-8"))
    urls  = [it["detail_url"] for it in items]
    queue = asyncio.Queue()
    for u in urls:
        queue.put_nowait(u)
    for _ in range(CONCURRENCY):
        queue.put_nowait(None)
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False, slow_mo=50)
        tasks = [asyncio.create_task(worker(browser, queue)) for _ in range(CONCURRENCY)]
        await asyncio.gather(*tasks)
        await browser.close()
    print("All done.")

if __name__ == "__main__":
    asyncio.run(main())
