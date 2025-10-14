import argparse
import csv
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import logging
import time
import chardet

# Setup logging
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("datadownloader.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

ERROR_PHRASES = [
    "Сервер перевантажений запитами",
    "Service Temporarily Unavailable",
]
MAX_RETRIES = 3
INITIAL_BACKOFF = 15

# Helper Functions
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))
    return result['encoding']

def sanitize_filename(name):
    invalid_chars = r'<>:"/\|?*'
    for char in invalid_chars:
        name = name.replace(char, "_")
    return name

def wait_and_restart_browser(playwright, wait_time):
    logger.info(
        f"Restarting the browser after waiting {wait_time} seconds...")
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context()
    page = context.new_page()
    return browser, context, page

# Main Function
def parse_args():
    repo_root = Path(__file__).resolve().parents[2]
    default_dir = repo_root / "data" / "awol_court"
    parser = argparse.ArgumentParser(description="Download court decisions with retry logic.")
    parser.add_argument(
        "--csv-file",
        type=Path,
        default=default_dir / "index_ad.csv",
        help="Path to CSV file containing links.",
    )
    parser.add_argument(
        "--save-dir",
        type=Path,
        default=default_dir / "html_ad",
        help="Directory to save HTML files.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    base_url = "https://reyestr.court.gov.ua"
    csv_file_path = args.csv_file
    save_directory = args.save_dir

    # Detect file encoding
    try:
        encoding = detect_encoding(csv_file_path)
        logger.info(f"Detected file encoding: {encoding}")
    except Exception as e:
        logger.error(f"Failed to detect file encoding: {e}")
        return

    # Ensure the save directory exists
    try:
        save_directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Files will be saved to: {save_directory.resolve()}")
    except Exception as e:
        logger.error(f"Failed to create/save directory: {e}")
        return

    # Preload existing file names
    existing_files = {file.name for file in save_directory.glob("*.html")}
    logger.info(f"Found {len(existing_files)} existing files in the directory.")

    # Function to process the CSV with a new browser instance
    def process_csv_with_browser():
        with sync_playwright() as p:
            browser, context, page = wait_and_restart_browser(p, 0)

            try:
                with open(csv_file_path, mode="r", encoding=encoding, newline="") as file:
                    reader = csv.DictReader(file)

                    logger.info(f"CSV Headers: {reader.fieldnames}")

                    for idx, row in enumerate(reader, start=1):
                        partial_link = row.get("Посилання")
                        custom_name = row.get("№ рішення", f"output_{idx}")
                        if not partial_link:
                            logger.warning(f"Row {idx} missing 'Посилання'; skipping.")
                            continue

                        custom_name = sanitize_filename(custom_name)
                        file_name = f"{custom_name}.html"

                        if file_name in existing_files:
                            logger.info(f"File already exists for {custom_name}. Skipping download.")
                            continue

                        file_path = save_directory / file_name
                        full_url = f"{base_url}{partial_link}"

                        retries = 0
                        backoff_seconds = INITIAL_BACKOFF

                        while retries < MAX_RETRIES:
                            try:
                                logger.info(f"Navigating to: {full_url}")
                                page.goto(full_url, timeout=10000)
                                page_content = page.content()
                                if any(phrase in page_content for phrase in ERROR_PHRASES):
                                    raise Exception("Server overloaded. Restarting the browser.")
                                page.click("text=Версія для друку")
                                page.wait_for_load_state("networkidle")
                                html_content = page.content()
                                with open(file_path, "w", encoding="utf-8") as out:
                                    out.write(html_content)
                                logger.info(f"Saved content from {full_url} to {file_path}")
                                break
                            except Exception as e:
                                retries += 1
                                logger.error(
                                    f"Encountered an issue for {custom_name} (attempt {retries}/{MAX_RETRIES}): {e}"
                                )
                                browser.close()
                                if retries >= MAX_RETRIES:
                                    logger.error(
                                        f"Max retries reached for {custom_name}. Skipping decision."
                                    )
                                    browser, context, page = wait_and_restart_browser(p, 0)
                                    break
                                time.sleep(backoff_seconds)
                                logger.info(
                                    f"Restarting browser for {custom_name} (retry {retries}/{MAX_RETRIES})"
                                )
                                browser, context, page = wait_and_restart_browser(
                                    p, backoff_seconds
                                )
                                backoff_seconds *= 2

            except Exception as e:
                logger.error(f"Error reading or processing the CSV file: {e}")

            finally:
                browser.close()
                logger.info("Browser closed.")
            return True  # Processing completed successfully

    # Process the CSV, restarting the browser when needed
    while True:
        success = process_csv_with_browser()
        if success:
            break  # Exit the loop if processing completed successfully

if __name__ == "__main__":
    main()
