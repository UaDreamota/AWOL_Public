## Court Scraper Scripts

The utilities in `scraping/court_scraper` now expose command line options so
input and output locations can be configured. Each argument defaults to the
`data/awol_court` directory relative to the repository root, so running without
flags uses these paths.

Examples:

```bash
python scraping/court_scraper/datadownloader.py \
    --csv-file data/awol_court/input.csv \
    --save-dir data/awol_court/html

python scraping/court_scraper/scrapervr2downloader.py \
    --csv-file data/awol_court/input.csv \
    --save-dir data/awol_court/html

python scraping/court_scraper/guide_nltk.py \
    --html-dir data/awol_court/html \
    --output-csv data/awol_court/parsed_html_results.csv

python scraping/court_scraper/megercsv.py \
    --csv1 data/awol_court/output12.csv \
    --csv2 data/awol_court/parsed_html_results.csv \
    --output-dir data/awol_court

python scraping/court_scraper/fetch_missing_decisions.py \
    --index-csv data/awol_court/output.csv \
    --download-dir data/awol_court/html \
    --headless False \
    --max-retries 5

python scraping/court_scraper/pwdownloader.py \
    --case-type criminal \
    --output-csv data/awol_court/criminal_results.csv

python scraping/court_scraper/pwdownloader.py \
    --case-type administrative \
    --output-csv data/awol_court/admin_results.csv
```

Legacy scripts `pwdownloader_cr.py` and `pwdownloader_ad.py` now wrap
`pwdownloader.py` for backward compatibility.

Use `--help` with any script to view all available options.
