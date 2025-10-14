# Interesting findings on UALosses Data Integrity

Website UALosses was not once referenced by western media such as The Economist 
journal. It is OSINT platform which claims to gather open-source information about
Ukrainian soldier's death and provide descriptive statistics. 

Despite websites seeming transperency, which is implemented with a user-friendly interface, where
you can search soldiers by their name and looks something like this:

![ualosseswebsite.png](figs/img.png)

Website also keeps descriptive statistics, such as count of entries divided by 
deaths, MIA (missing in action) and prisoners or distributions of age and weekly deaths.

They claim to collect the public
data on social media, local authorities, registeries etc.

Each entry also has sources and demographic/social and military information
about killed or MIA or prisoned soldiers:

![img_3.png](figs/img_3.png)

When asked about an API or direct access to the data, creators did not provide
any instruments for easier access to the data. Which scraping the only solution of
acquiring the data. 

Some sources tried to validate the data by randomly drawing soldiers from their website the checking the 

Each person has a unique URL, which makes scraping easier. That was the main identifier for scraping.

I have scraped 142,644 (2nd quarter of 2025) from the website and tried to validate the data integrity.
The scraping consisted of 2 parts:
- First script was collecting all the URLs of the soldiers on each page of the website. There should not be
any duplicates as the count of entries on the website matches number of pages * entries per page.
    - As each soldier contains unique URL, we collected all the URLs and saved them to a CSV file. Later, the script was rerun with also an ID for page where the URL came from [data](../scripts/soldiers_1.json).
- Second script was directly visiting each URL and collecting the information about the soldier, that would be used later foe the actual reasearch.

First of all, I got the correct number of entries, which matched the number of the website, date of the scraping. 

It means that if I would to encounter any duplicates it would be the problem of the website or dataset. 

I tried two methods to check the duplicates:

1. I checked the number of unique URLs and duplicates. 
2. I checked exact duplicate rows (as a sanity check).

And here are the results:

First of all, duplicates by URL and Excact row duplicates matches, which makes perfect sense.
So, I will be mostly showing duplicates by URL.

| Metric                                 |                      Value |
| -------------------------------------- | -------------------------: |
| Distinct `detail_url` values           |                     90,505 |
| `detail_url` appearing once            |                     52,687 |
| Rows in duplicated `detail_url` groups | 89,957 (63.1% of all rows) |


| Repetition count (k) | # Groups / # URLs | Rows represented (= k × groups) | % of duplicate rows | % of all rows |
| -------------------: | ----------------: | ------------------------------: | ------------------: | ------------: |
|                    2 |            26,907 |                          53,814 |               59.8% |        37.73% |
|                    3 |             8,194 |                          24,582 |               27.3% |        17.23% |
|                    4 |             2,155 |                           8,620 |                9.6% |         6.04% |
|                    5 |               454 |                           2,270 |                2.5% |         1.59% |
|                    6 |                85 |                             510 |                0.6% |         0.36% |
|                    7 |                23 |                             161 |                0.2% |         0.11% |
|            **Total** |        **37,818** |                      **89,957** |            **100%** |    **63.10%** |

So, 63.1% of all rows are duplicates, which is a lot. With some of the entries being repeated up to 7 times. 

This raises huge concerns about the integrity and validity of the data.

The scripts for checking duplicates can be found here: [expl_an.ipynb](../data/Soldiers_deaths/scripts/expl_an.ipynb)

The scripts for scraping can be found here: [ualosses_scraper.py](../scripts/ualosses_scraper.py) and [ualosses_get_urls.py](../scripts/ualosses_get_urls.py)