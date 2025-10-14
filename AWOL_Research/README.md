# AWOL_Research


<mark style="background: #FF5582A6;">The project is WiP, and the repository will be updated as the research progresses
many scripts will change and become more readable as the project evolves. It captures my personal growth in quantitative analysis 
and basic skills such as Python.</mark>

This repository is created as big research project on an undergraduate Ukrainian student in the Czech Republic.

It studies the phenomena of AWOL (Absent Without Leave) in the context of military service during wartime. 
Sadly, not a lot of data is available on the topic, so the project aims to understate as much as possible about the topic through
data driven research.

The repository contains multiple scripts for scraping, data processing, visualization, analysis, modern methods exploring
etc. 

The idea is to approximate geospatial data of AWOL cases, analyze temporal trends and test multiple hypothesis. 

The highlights of the project are:

1. Data Scraping: The repository includes scripts for scraping multiple sources of data:
   - Court decisions: from the official court website (done via playwright)
   - Death records: from a public dataset UAlosses (my analysis proofs great degree of the falsification of the data on the websitem, more can be found [here](research_results/ualosses_ingerity.md))
2. Data processing: The repository contains a lot of scripts that process different datasets, cleans them and prepares fot analysis. 
   - [UaControlMap](https://www.uacontrolmap.com/map-viewer/) (Geospatial data about unit location and frontline)
   - [UAlosses](https://ualosses.org/en/soldiers/) (Database of death records)
   - [Ukraine's court decisions registry](https://reyestr.court.gov.ua/)
   - [LiveUaMap](https://liveuamap.com/) (WiP, data about violent events: shellings, combats etc.)
3. Interesting aproximation design of geospatial data of AWOL cases (WiP, more can be found [here](research_results/court_aproximation_design.md))
4. Research design and analysis. (WiP, more can be found [here](research_results/research_design.md))

## Repository Structure

```text
AWOL_Research/
├── court_qa/                 # Notebooks and scripts for question-answer data preparation
├── data/                     # Local storage for intermediate & raw datasets (kept private)
│   ├── Soldiers_deaths/      # CSV/JSON exports and duplicate analysis for soldier casualties
│   ├── awol_court/           # Output of court scraping tools
│   ├── kmz/                  # KMZ file analysis notebooks
│   └── spine/                # Spine/medical record analysis notebook
├── scraping/
│   ├── court_scraper/        # CLI utilities for downloading & parsing court documents
│   └── ua_scrap/             # Scrapers for Ukrainian administrative sources
├── scripts/                  # Stand-alone utilities & notebooks used across workflows
├── requirements.txt          # Python dependencies for scrapers and data processing
└── README.md
```

