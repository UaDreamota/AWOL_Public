
"""
UALOSSES list-page scraper (phase 1)

1) Discover total pages dynamically.
2) Scrape each list page for soldier entries.
3) Output results to NDJSON and CSV.
4) Maintain a processed_pages.json so you can resume.
"""

import asyncio
import json
import csv
import os
import logging
from urllib.parse import urljoin

from playwright.async_api import async_playwright, TimeoutError

# ─── CONFIG ────────────────────────────────────────────────────────────────────
BASE_URL = "https://ualosses.org"
LIST_PATH      = "/en/soldiers"
OUTPUT_JSON    = "soldiers.ndjson"
OUTPUT_CSV     = "soldiers.csv"
PROCESSED_FILE = "processed_pages.json"
CONCURRENCY    = 3
MAX_ATTEMPTS   = 3

CSV_FIELDS = [
    "name", "detail_url", "birth_date", "death_date",
    "location", "image_url", "page"
]

# ─── LOGGER SETUP ──────────────────────────────────────────────────────────────
def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger("ualosses_scraper")

# ─── BLOCK NON-ESSENTIAL RESOURCES ────────────────────────────────────────────
async def block_resources(route, request):
    if request.resource_type in ("image", "stylesheet", "font", "media"):
        await route.abort()
    else:
        await route.continue_()

# ─── 1) DISCOVER TOTAL PAGES ──────────────────────────────────────────────────
async def get_total_pages(browser, logger):
    page = await browser.new_page()
    await page.route("**/*", block_resources)
    page.set_default_timeout(30_000)

    url = urljoin(BASE_URL, LIST_PATH)
    logger.info("Fetching first page to discover total pages: %s", url)
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_selector("a[aria-label^='Page']", timeout=10_000)

    nums = []
    anchors = await page.query_selector_all("a[aria-label^='Page']")
    for a in anchors:
        label = await a.get_attribute("aria-label")
        try:
            nums.append(int(label.rsplit(" ", 1)[1]))
        except:
            logger.warning("Bad pagination label: %s", label)

    await page.close()
    total = max(nums) if nums else 1
    logger.info("Discovered total_pages = %d", total)
    return total

# ─── 2) SCRAPE ONE LIST PAGE ─────────────────────────────────────────────────
async def scrape_list_page(page, page_num, logger):
    url = f"{BASE_URL}{LIST_PATH}?page={page_num}"
    logger.info("[Page %d] → %s", page_num, url)

    try:
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_selector("ul.small-block-grid-2 li", timeout=10_000)
    except TimeoutError:
        logger.error("[Page %d] ✗ load or selector timeout", page_num)
        return None

    items = await page.query_selector_all("ul.small-block-grid-2 li")
    logger.info("[Page %d] ↳ Found %d entries", page_num, len(items))
    soldiers = []

    for idx, li in enumerate(items, start=1):
        try:
            face_div = await li.query_selector("div.face")
            style    = await face_div.get_attribute("style")
            img_rel  = style.split("url(")[1].split(")")[0].strip("'\"")
            img_url  = urljoin(BASE_URL, img_rel)

            link_el    = await li.query_selector("a.bl")
            href       = await link_el.get_attribute("href")
            detail_url = urljoin(BASE_URL, href)
            name_el    = await link_el.query_selector("b")
            name       = (await name_el.inner_text()).strip() if name_el else (await link_el.inner_text()).strip()

            date_divs  = await li.query_selector_all("div[style*='font-size:0.8rem']")
            dates_txt  = date_divs[0] and await date_divs[0].inner_text() or ""
            if " - " in dates_txt:
                birth_date, death_date = (d.strip() for d in dates_txt.split(" - "))
            else:
                birth_date = death_date = ""
            location = len(date_divs) > 1 and await date_divs[1].inner_text() or ""

            soldiers.append({
                "name"       : name,
                "detail_url" : detail_url,
                "birth_date" : birth_date,
                "death_date" : death_date,
                "location"   : location,
                "image_url"  : img_url,
                "page"       : page_num
            })

        except Exception as e:
            logger.error("[Page %d] ✗ parse error entry %d: %s", page_num, idx, e)

    return soldiers if soldiers else None

# ─── 3) WORKER WITH RETRY ──────────────────────────────────────────────────────
async def worker(page_num, browser, sem, logger):
    async with sem:
        page = await browser.new_page()
        await page.route("**/*", block_resources)
        page.set_default_timeout(30_000)

        for attempt in range(1, MAX_ATTEMPTS+1):
            try:
                result = await scrape_list_page(page, page_num, logger)
                if result:
                    await page.close()
                    return page_num, result
                raise RuntimeError("No data returned")
            except Exception as e:
                logger.warning("[Page %d] attempt %d failed: %s", page_num, attempt, e)
                await asyncio.sleep(2 ** attempt)

        await page.close()
        logger.error("[Page %d] Giving up after %d attempts", page_num, MAX_ATTEMPTS)
        return page_num, []

# ─── 4) MAIN ──────────────────────────────────────────────────────────────────
async def main():
    logger = setup_logger()
    logger.info("Starting UALOSSES list-page scraper…")

    # load or init processed‐pages
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, encoding="utf-8") as f:
            processed = set(json.load(f))
    else:
        processed = set()

    # init outputs on first run
    if not os.path.exists(OUTPUT_JSON):
        open(OUTPUT_JSON, "w", encoding="utf-8").close()
    if not os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()

    async with async_playwright() as pw:
        browser     = await pw.chromium.launch(headless=True)
        total_pages = await get_total_pages(browser, logger)
        logger.info("Scraping pages 1–%d (skipping %d already done)", total_pages, len(processed))

        sem   = asyncio.Semaphore(CONCURRENCY)
        tasks = [
            asyncio.create_task(worker(n, browser, sem, logger))
            for n in range(1, total_pages+1)
            if n not in processed
        ]

        for coro in asyncio.as_completed(tasks):
            page_num, soldiers = await coro

            # append to NDJSON
            with open(OUTPUT_JSON, "a", encoding="utf-8") as jf:
                for rec in soldiers:
                    jf.write(json.dumps(rec, ensure_ascii=False) + "\n")

            # append to CSV
            with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as cf:
                writer = csv.DictWriter(cf, fieldnames=CSV_FIELDS)
                writer.writerows(soldiers)

            # mark done
            processed.add(page_num)
            with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
                json.dump(sorted(processed), f, ensure_ascii=False)

            logger.info("[Page %d] ✓ %d records saved", page_num, len(soldiers))

        await browser.close()

    logger.info("All done.")

if __name__ == "__main__":
    asyncio.run(main())
