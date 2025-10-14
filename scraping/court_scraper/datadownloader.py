import argparse
import csv
from pathlib import Path
from playwright.sync_api import sync_playwright
import chardet
import logging


def sanitize_filename(name):
    """
    Remove or replace characters that are invalid in file names.
    """
    invalid_chars = r'<>:"/\|?*'
    for char in invalid_chars:
        name = name.replace(char, "_")
    return name


def detect_encoding(file_path):
    """
    Detect the encoding of the given file using chardet.
    """
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read(10000))  # Read first 10,000 bytes
    return result['encoding']


def setup_logging():
    """
    Set up logging configuration.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments."""
    repo_root = Path(__file__).resolve().parents[2]
    default_dir = repo_root / "data" / "awol_court"
    parser = argparse.ArgumentParser(description="Download court decisions HTML pages.")
    parser.add_argument(
        "--csv-file",
        type=Path,
        default=default_dir / "input.csv",
        help="Path to CSV file with court decision links.",
    )
    parser.add_argument(
        "--save-dir",
        type=Path,
        default=default_dir / "html",
        help="Directory where HTML files will be saved.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logger = setup_logging()

    base_url = "https://reyestr.court.gov.ua/"  # Your actual base URL
    csv_file_path = args.csv_file

    # Detect the encoding of the CSV file
    try:
        encoding = detect_encoding(csv_file_path)
        logger.info(f"Detected encoding: {encoding}")
    except Exception as e:
        logger.error(f"Failed to detect encoding: {e}")
        return

    # Define the save directory
    save_directory = args.save_dir
    try:
        save_directory.mkdir(parents=True, exist_ok=True)  # Create directory if it doesn't exist
        logger.info(f"Files will be saved to: {save_directory.resolve()}")
    except Exception as e:
        logger.error(f"Failed to create/save directory: {e}")
        return

    # Start Playwright in sync mode
    try:
        with sync_playwright() as p:
            # Launch a Chromium browser; set headless=False to see the browser window
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()

            # Open and read the CSV
            with open(csv_file_path, mode="r", encoding=encoding) as file:
                reader = csv.DictReader(file, delimiter=';')

                # Print the headers
                logger.info(f"CSV Headers: {reader.fieldnames}")

                # Print the first row to inspect
                try:
                    first_row = next(reader)
                    logger.info(f"First Row: {first_row}")
                    # Reset the reader to include the first row in processing
                    file.seek(0)
                    reader = csv.DictReader(file, delimiter=';')
                except StopIteration:
                    logger.error("The CSV file is empty.")
                    return

                for idx, row in enumerate(reader, start=1):
                    # Extract the partial link from the CSV column
                    partial_link = row.get("Посилання")
                    if not partial_link:
                        logger.warning(f"Row {idx} missing 'Посилання'; skipping.")
                        continue

                    # Build the full URL
                    full_url = base_url + partial_link
                    logger.info(f"Navigating to: {full_url}")

                    try:
                        # Go to the page
                        page.goto(full_url)

                        # Click the button; ensure the selector is correct
                        # Replace '#Версія для друку' with the correct selector
                        page.click("#btnPrint")

                        # Optionally, wait for the page to load fully
                        page.wait_for_load_state("networkidle")  # Wait until network is idle

                        # Get the page content
                        html_content = page.content()

                        # Get the custom file name and sanitize it
                        custom_name = row.get("№ рішення", f"output_{idx}")
                        custom_name = sanitize_filename(custom_name)

                        # Construct the full file path
                        file_path = save_directory / f"{custom_name}.html"

                        # Save the HTML content to the file
                        with open(file_path, "w", encoding="utf-8") as out:
                            out.write(html_content)

                        logger.info(f"Saved HTML from {full_url} as '{file_path}'.")
                    except Exception as e:
                        logger.error(f"Error processing {full_url}: {e}")

            # Close the browser after processing
            browser.close()
            logger.info("Browser closed.")

    except Exception as e:
        logger.error(f"Playwright encountered an error: {e}")


if __name__ == "__main__":
    main()
