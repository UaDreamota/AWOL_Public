import argparse
import csv
import time
from playwright.sync_api import sync_playwright

# Mapping of case types to the values needed to drive the form
CASE_CONFIG = {
    "criminal": {
        "CSType": "2",
        "CaseCat1": "40438",
        "CaseCat2": "40845",
        "CaseCat3": "40851",
        "VRType": "1",
    },
    "administrative": {
        "CSType": "5",
        "CaseCat1": "40933",
        "CaseCat2": "41222",
        "CaseCat3": "41224",
        "VRType": "2",
    },
}

def run(playwright, case_type: str, output_csv: str) -> None:
    """Drive the search form and write results to ``output_csv``.

    Parameters
    ----------
    playwright: Playwright instance created by ``sync_playwright``.
    case_type: One of the keys in ``CASE_CONFIG``.
    output_csv: Path where the resulting CSV will be written.
    """
    config = CASE_CONFIG[case_type]

    browser = playwright.chromium.launch(headless=False)
    page = browser.new_page()
    page.goto('https://reyestr.court.gov.ua/')
    print(page.title())

    # Select the form of proceedings
    page.locator("#CSType").click()
    page.click(f'input[name="CSType[]"][value="{config["CSType"]}"]')
    page.locator("#CSType").click()

    # Select category and subcategories
    page.locator("#CaseCat1").click()
    page.wait_for_selector(
        f'input[name="CaseCat1[]"][value="{config["CaseCat1"]}"]',
        state='visible'
    )
    page.click(f'input[name="CaseCat1[]"][value="{config["CaseCat1"]}"]')
    page.locator("#CaseCat1").click()

    page.wait_for_timeout(500)
    page.locator("#CaseCat2").click()
    page.wait_for_selector(
        f'input[name="CaseCat2[]"][value="{config["CaseCat2"]}"]',
        state='visible'
    )
    page.check(f'input[name="CaseCat2[]"][value="{config["CaseCat2"]}"]')
    page.locator("#CaseCat2").click()

    page.wait_for_timeout(500)
    page.locator("#CaseCat3").click()
    page.wait_for_selector(
        f'input[name="CaseCat3[]"][value="{config["CaseCat3"]}"]',
        state='visible'
    )
    page.check(f'input[name="CaseCat3[]"][value="{config["CaseCat3"]}"]')
    page.locator("#CaseCat3").click()

    # Select VRType
    page.locator("#VRType").click()
    page.wait_for_selector(f'input[name="VRType[]"][value="{config["VRType"]}"]')
    page.check(f'input[name="VRType[]"][value="{config["VRType"]}"]')
    page.locator("#VRType").click()

    # Sort order and pagination size
    page.locator("#Sort").select_option("1")
    page.locator("#PagingInfo_ItemsPerPage").select_option("100")

    page.locator("#btn").click()

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "№ рішення",
            "Форма судового рішення",
            "Дата ухвалення рішення",
            "Дата набрання законної сили",
            "Форма судочинства",
            "№ судової справи",
            "Назва суду",
            "Суддя",
            "Посилання",
        ])

        # Pagination loop
        while True:
            page.wait_for_selector("#tableresult tr", state="visible")
            rows_locator = page.locator("#tableresult tr")
            row_count = rows_locator.count()

            for i in range(row_count):
                row = rows_locator.nth(i)
                if row.locator("th").count() > 0:
                    continue

                cells = row.locator("td")
                cell_count = cells.count()
                if cell_count < 8:
                    continue

                no_rishennya = cells.nth(0).inner_text().strip()
                forma_rishennya = cells.nth(1).inner_text().strip()
                data_ukhvalennya = cells.nth(2).inner_text().strip()
                data_nabrannya_zakon = cells.nth(3).inner_text().strip()
                forma_sudochinstva = cells.nth(4).inner_text().strip()
                nomer_spravy = cells.nth(5).inner_text().strip()
                nazva_sudu = cells.nth(6).inner_text().strip()
                suddya = cells.nth(7).inner_text().strip()
                link_locator = cells.nth(0).locator("a.doc_text2")
                href = ""
                if link_locator.count() > 0:
                    href = link_locator.first.get_attribute("href")

                writer.writerow([
                    no_rishennya,
                    forma_rishennya,
                    data_ukhvalennya,
                    data_nabrannya_zakon,
                    forma_sudochinstva,
                    nomer_spravy,
                    nazva_sudu,
                    suddya,
                    href,
                ])

            next_page_locator = page.locator('a.enButton', has_text=">")
            if next_page_locator.count() == 0:
                break
            next_page_locator.first.click()

    time.sleep(40)
    browser.close()


def download(case_type: str, output_csv: str) -> None:
    """Convenience wrapper that manages the Playwright context."""
    with sync_playwright() as playwright:
        run(playwright, case_type, output_csv)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download court decisions.")
    parser.add_argument(
        "--case-type",
        choices=list(CASE_CONFIG.keys()),
        required=True,
        help="Type of cases to download",
    )
    parser.add_argument(
        "--output-csv",
        default="output.csv",
        help="Path to CSV file to write results",
    )
    args = parser.parse_args()
    download(args.case_type, args.output_csv)


if __name__ == "__main__":
    main()
