# EP Vote Collection

This repository collects and processes voting data from the European Parliament (EP). 
It provides tools to gather either (1) daily votes or (2) historical data for the 9th and 10th mandates using the [EP Open Data API](https://data.europarl.europa.eu/en/developer-corner/opendata-api). 
The scripts are designed to automate data collection, cleaning, and aggregation, making it easier to analyze voting patterns and trends.

---

## Overview

The repository serves three primary purposes:

1. **Daily Vote Collection**: Automates the retrieval and processing of voting data from the most recent EP Plenary Session (`scripts_main/ep_rcv_today.R`).
2. **Historical Data Collection**: Provides scripts to download and process all voting data for the 9th and 10th mandates, or just the 10th (respectively, via `scripts_main/ep_rcv_mandate_all.R` and `scripts_main/ep_rcv_mandate_10.R`).
3. **Analysis**: Facilitates the analysis of MEPs' behavior (these are the scripts in the `\analyses\` folder). 

---

## Setup Instructions

To use this repository, follow these steps:

1. Clone the repository:
```bash
   git clone https://github.com/your-repo/ep_vote_collection.git
   cd ep_vote_collection
```

2. Install R and required dependencies. 
3. (Optional) Use the provided .devcontainer setup to deploy the repository in a GitHub Codespace for a pre-configured environment.
4. Ensure API access to the EP Open Data API.

---

## Usage
### Daily Votes
To collect and process voting data for the current day:

1. Run `scripts_main/ep_rcv_today.R` to retrieve and clean the data. This script:
   - Selects the last session from the EP calendar.
   - Downloads and processes voting data for the day.
   - Outputs two files in the `data_out` folder:
     - `meps_rcv_today.csv`: Contains MEP-level voting data.
     - `votes_today.csv`: Contains aggregated voting data.
2. Run `source_scripts_r/aggregate_rcv.R` to aggregate the results by EP Political Groups. This script generates:
   - `result_bygroup_byrcv.csv`: Tallies of votes by group.
   - `fullresult_bygroup_byrcv.csv`: Includes additional data on absent MEPs and those who did not vote.


### Historical Votes (9th & 10th Mandates)
To collect and process all votes during the 9th mandate:

1. Run `scripts_main/ep_rcv_mandate_all.R`. 
This master script:
   - Retrieves meeting data, attendance lists, decisionsand voting .
   - Cleans and merges the data into two files:
     - `data_out/votes/votes_dt.csv`: Wide-format data with one row per vote.
     - `data_out/rcv/rcv_dt.csv`: Long-format data with all RCVs.
2. Run additional scripts as needed:
    - `source_scripts_r/api_meps.R`: Retrieves MEP metadata (e.g., country, party, group, mandates).
    - `source_scripts_r/api_bodies.R`: Downloads dictionaries for national parties and political groups.

---     

## File Structure
The repository is organized as follows:

* `scripts_main/`: Contains master scripts for daily and historical data collection.
* `source_scripts_r/`: Contains helper scripts for API calls and data processing.
* `data_in/`: Stores raw input data.
* `data_out/`: Stores processed output data.
* `analyses/`: Contains analysis scripts and reports.
* `.devcontainer/`: Configuration for GitHub Codespaces.
