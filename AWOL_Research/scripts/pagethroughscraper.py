import asyncio
import json
import csv
import logging
import os
from urllib.parse import urljoin
from playwright.async_api import async_playwright, TimeoutError

# ==== CONFIG ====
BASE_URL = "https://ualosses.org"
LIST_PATH = "/en/soldiers/"
TOTAL_PAGES = 1471
CONCURRENCY = 1

OUTPUT_JSON = "soldiers_1.json"
OUTPUT_JSON_TMP = "soldiers.json.tmp"
OUTPUT_CSV = "soldiers_1.csv"
PROCESSED_PAGES_FILE = "processed_pages.json"

# ==== LOGGER ====
def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger("warrior_scraper")

# ==== BLOCK NON-ESSENTIAL RESOURCES ====
async def block_resources(route, request):
    if request.resource_type in ("image", "stylesheet", "font", "media"):
        await route.abort()
    else:
        await route.continue_()

# ==== SCRAPE ONE PAGE ====
async def scrape_list_page(page, page_num, logger):
    url = f"{BASE_URL}{LIST_PATH}?page={page_num}" if page_num > 1 else f"{BASE_URL}{LIST_PATH}"
    logger.info(f"[Page {page_num}] → Navigating to {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded")
    except TimeoutError:
        logger.error(f"[Page {page_num}] ✗ navigation timeout")
        return None

    # WAIT FOR THE ACTUAL GRID ITEMS
    try:
        await page.wait_for_selector("ul.small-block-grid-2 li", timeout=10_000)
    except TimeoutError:
        logger.error(f"[Page {page_num}] ✗ no <ul.small-block-grid-2 li> found")
        return None

    # QUERY THAT SAME SELECTOR
    items = await page.query_selector_all("ul.small-block-grid-2 li")
    logger.info(f"[Page {page_num}] ↳ found {len(items)} entries")

    soldiers = []
    for idx, li in enumerate(items, start=1):
        try:
            # image URL
            face_div = await li.query_selector("div.face")
            style    = await face_div.get_attribute("style")
            img_rel  = style.split("url(")[1].split(")")[0].strip("'\"")
            img_url  = urljoin(BASE_URL, img_rel)

            # detail link + name
            link_el    = await li.query_selector("a.bl")
            href       = await link_el.get_attribute("href")
            detail_url = urljoin(BASE_URL, href)
            name_el    = await link_el.query_selector("b")
            name       = await name_el.inner_text() if name_el else (await link_el.inner_text()).strip()

            # dates & location
            date_divs  = await li.query_selector_all("div[style*='font-size:0.8rem']")
            dates_text = await date_divs[0].inner_text() if date_divs else ""
            birth_date, death_date = (d.strip() for d in dates_text.split(" - "))
            location   = await date_divs[1].inner_text() if len(date_divs) > 1 else ""

            soldiers.append({
                "name":       name,
                "detail_url": detail_url,
                "birth_date": birth_date,
                "death_date": death_date,
                "location":   location,
                "image_url":  img_url,
                "page":       page_num
            })
        except Exception as e:
            logger.error(f"[Page {page_num}] ✗ parse error entry {idx}: {e}")

    return soldiers or None  # None triggers a retry

# ==== WORKER ====
async def worker(page_num, playwright, semaphore, logger):
    async with semaphore:
        while True:
            browser = await playwright.chromium.launch(headless=False)
            page    = await browser.new_page()
            await page.route("**/*", block_resources)
            page.set_default_timeout(60_000)
            try:
                result = await scrape_list_page(page, page_num, logger)
            finally:
                await page.close()
                await browser.close()

            if result:
                return page_num, result
            logger.warning(f"[Page {page_num}] retrying…")
            await asyncio.sleep(200)

# ==== MAIN ====
async def main():
    logger = setup_logger()
    logger.info("Starting parallel warrior scraper…")

    # load progress
    try:
        processed = set(json.load(open(PROCESSED_PAGES_FILE, encoding="utf-8")))
    except Exception:
        processed = set()

    # init on first run
    if not processed:
        json.dump([], open(OUTPUT_JSON, "w", encoding="utf-8"))
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as cf:
            csv.DictWriter(
                cf,
                fieldnames=[
                    "name", "detail_url", "birth_date", "death_date", "location", "image_url", "page"
                ]
            ).writeheader()

    all_soldiers = json.load(open(OUTPUT_JSON, encoding="utf-8"))

    semaphore = asyncio.Semaphore(CONCURRENCY)
    async with async_playwright() as pw:
        tasks = [
            asyncio.create_task(worker(n, pw, semaphore, logger))
            for n in range(1, TOTAL_PAGES + 1)
            if n not in processed
        ]

        for finished in asyncio.as_completed(tasks):
            page_num, soldiers = await finished

            # persist JSON atomically
            all_soldiers.extend(soldiers)
            with open(OUTPUT_JSON_TMP, "w", encoding="utf-8") as jf:
                json.dump(all_soldiers, jf, ensure_ascii=False, indent=2)
            os.replace(OUTPUT_JSON_TMP, OUTPUT_JSON)

            # append CSV
            with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as cf:
                csv.DictWriter(cf, fieldnames=soldiers[0].keys()).writerows(soldiers)

            processed.add(page_num)
            json.dump(sorted(processed), open(PROCESSED_PAGES_FILE, "w", encoding="utf-8"))

            logger.info(f"[Page {page_num}] ✓ {len(soldiers)} entries saved")

    logger.info("Scraping complete.")

if __name__ == "__main__":
    asyncio.run(main())
