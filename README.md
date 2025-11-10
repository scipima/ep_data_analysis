# EP Vote Collection
This repository collects and processes voting data from the European Parliament (EP). 
It provides tools to gather either (1) daily votes or (2) historical data for the 9th and 10th mandates using the [EP Open Data API](https://data.europarl.europa.eu/en/developer-corner/opendata-api). 
The scripts are designed to automate data collection, cleaning, and aggregation, making it easier to analyze voting patterns and trends.

---

## Overview

The repository serves three primary purposes:

1. **Daily Vote Collection**: Automates the retrieval and processing of voting data from the most recent EP Plenary Session (`scripts_main/ep_rcv_today.R`).
2. **Historical Data Collection**: Provides scripts to download and process all voting data for the 9th and 10th mandates, or just the 10th (respectively, via `scripts_main/ep_rcv_mandates_all.R` and `scripts_main/ep_rcv_mandate_10.R`).
3. **Analysis**: Facilitates the analysis of MEPs' behavior (these are the scripts in the `analyses/` folder).

---

## Repository Architecture

### Main Scripts (`scripts_main/`)
These are the entry points that orchestrate the entire data collection workflow:

- **`ep_rcv_today.R`**: Collects today's plenary votes and roll-call votes (RCV)
- **`ep_rcv_mandate_10.R`**: Collects all votes for the 10th mandate (2024-present)  
- **`ep_rcv_mandates_all.R`**: Collects votes for both 9th and 10th mandates
- **`master_day.R`**: Daily orchestrator that runs `ep_rcv_today.R` and handles foreseen activities

### Child Scripts (`scripts_r/`)
These scripts handle specific API endpoints and data processing tasks. They are called by the main scripts:

#### API Data Collection Scripts:
- **`api_meetings.R`**: Fetches plenary meeting schedules and metadata
- **`api_meetings_decisions.R`**: Collects voting decisions (the core voting data)
- **`api_meetings_attendance.R`**: Gathers official attendance records
- **`api_meetings_voteresults.R`**: Retrieves vote titles and metadata
- **`api_meps.R`**: Downloads MEP information, mandates, and membership details
- **`api_bodies.R`**: Fetches political groups and national party lookup tables
- **`api_pl_docs.R`**: Collects plenary document information
- **`api_pl_session_docs_ids.R`**: Identifies final votes from session documents

#### Data Processing Scripts:
- **`clean_decisions.R`**: Processes raw voting decision JSON files
- **`process_decisions_session_*.R`**: Series of scripts that clean different aspects of voting data:
  - `process_decisions_session_metadata.R`: Vote metadata
  - `process_decisions_session_rcv.R`: Roll-call vote individual positions  
  - `process_decisions_session_intentions.R`: Vote corrections/intentions
- **`aggregate_rcv.R` / `aggregate_rcv_today.R`**: Aggregate individual votes by political groups

#### Utility Scripts:
- **`repo_setup.R`**: Creates directory structure and defines the `get_api_data()` function
- **`parallel_api_calls.R`**: Handles concurrent API requests for efficiency
- **`join_functions.R`**: Data merging utilities
- **`get_majority.R`** / **`cohesionrate_function.R`**: Analysis functions 

---

## Data Sources and API Endpoints

All data is retrieved from the **[EP Open Data API](https://data.europarl.europa.eu/en/developer-corner/opendata-api)** (base URL: `https://data.europarl.europa.eu/api/v2`). The main endpoints used are:

1. **`GET/meetings`**: Plenary meeting calendar and session metadata
2. **`GET/meetings/{event-id}/decisions`**: Individual voting decisions and roll-call votes (RCV) 
3. **`GET/meetings/{event-id}/attendance`**: Official attendance records
4. **`GET/meetings/{event-id}/vote-results`**: Vote titles and descriptions
5. **`GET/meps`** / **`GET/meps/{mep-id}`**: MEP biographical data, mandates, and political group memberships
6. **`GET/corporate-bodies/{body-id}`**: Political group and national party information
7. **`GET/plenary-documents`**: Plenary session document metadata
8. **`GET/plenary-session-documents/{doc-id}`**: Individual session documents (used to identify final votes)

### Key Data Concepts

- **Roll-Call Votes (RCV)**: Individual MEP voting positions (For/Against/Abstention) recorded electronically
- **Decisions**: Voting events that may or may not be RCVs (some are voice votes)
- **Final Votes**: Legislative votes that definitively adopt or reject proposals
- **Mandates**: MEP terms of office (9th mandate: 2019-2024, 10th mandate: 2024-2029)
- **Political Groups**: EP party groups (e.g., EPP, S&D, Renew, etc.)
- **National Parties**: Domestic political parties MEPs belong to

---

## Workflow Overview

### Core Workflow Logic
All main scripts follow a similar 4-step pattern:

1. **Fetch Meeting Data**: Get plenary session dates and identifiers
2. **Collect Voting Data**: Download decisions/RCVs for those sessions  
3. **Gather MEP Information**: Get current MEP list with political affiliations
4. **Merge & Process**: Combine voting data with MEP data, creating a complete grid

### Smart Caching System
The repository uses an intelligent caching mechanism via the `get_api_data()` function (defined in `repo_setup.R`):

- **Checks file age**: Only re-downloads data if existing files are older than `max_days` threshold
- **Conditional execution**: Loads existing CSV/RDS files if recent enough, otherwise runs the API script
- **Prevents redundant calls**: Avoids hitting the API unnecessarily, which is crucial given the data volume

### Data Processing Pipeline

1. **Raw JSON Collection**: API responses stored as JSON files in `data_in/meeting_decision_json/`
2. **Initial Cleaning**: JSON flattened and basic cleaning applied
3. **Specialized Processing**: Different processors handle votes, RCVs, and intentions separately
4. **MEP Grid Creation**: Generates comprehensive grid of all MEP-vote combinations, including absences
5. **Final Integration**: Merges voting positions with MEP metadata and political affiliations

---

## Setup Instructions

To use this repository, follow these steps:

1. Clone the repository:
```bash
   git clone https://github.com/your-repo/ep_vote_collection.git
   cd ep_vote_collection
```

2. Install R and required dependencies:
   - Install R from [CRAN](https://cran.rstudio.com/)
   - Use an IDE such as [RStudio](https://posit.co/download/rstudio-desktop/), [Positron](https://positron.posit.co/download.html), or [Visual Studio Code](https://code.visualstudio.com/docs/languages/r)
   - Required R packages are automatically installed via `pacman::p_load()` calls in each script

3. (Optional) Use the provided `.devcontainer` setup to deploy in a GitHub Codespace for a pre-configured environment.

4. **No API key required** - the EP Open Data API is publicly accessible

---

## Usage Examples

### Daily Vote Collection
To collect and clean **today's votes** in the EP:

**Primary Script**: `scripts_main/ep_rcv_today.R`

This script automatically:
1. Identifies today's plenary session using the pattern `MTG-PL-{today's date}`
2. Downloads attendance records and voting decisions for that session
3. Processes all roll-call votes (RCVs) for the day
4. Merges voting data with current MEP information
5. Creates a comprehensive grid including MEPs who were absent or didn't vote

**Output Files** (stored in `data_out/daily/`):
- `rcv_today_{YYYYMMDD}.csv`: Individual MEP voting positions
- `votes_today_{YYYYMMDD}.csv`: Vote metadata and results

**Aggregation**: Run `scripts_r/aggregate_rcv_today.R` to generate:
- `result_bygroup_byrcv.csv`: Vote tallies by political group
- `fullresult_bygroup_byrcv.csv`: Enhanced data including absent MEPs and non-votes

**Master Daily Script**: `scripts_main/master_day.R` coordinates the full daily workflow, including foreseen activities.


### Historical Mandate Collection

#### 10th Mandate (2024-present)
**Primary Script**: `scripts_main/ep_rcv_mandate_10.R`

**Execution Steps**:
1. **Meeting Collection** (`api_meetings.R`): Downloads plenary meeting calendar
2. **Attendance Records** (`api_meetings_attendance.R`): Official attendance lists  
3. **Vote Decisions** (`api_meetings_decisions.R`): Raw voting data (stored as `rcv_tmp_10.RDS`)
4. **Data Cleaning** (`clean_decisions.R`): Processes JSON files using specialized functions:
   - `process_decisions_session_metadata.R`: Vote metadata
   - `process_decisions_session_rcv.R`: Individual MEP positions
   - `process_decisions_session_intentions.R`: Vote corrections
5. **Vote Titles** (`api_meetings_voteresults.R`): Descriptive information about votes
6. **MEP Data** (`api_meps.R`): Current MEP list with mandate periods and political affiliations
7. **Lookup Tables** (`api_bodies.R`): Political groups and national party dictionaries
8. **Plenary Documents** (`api_pl_docs.R` + `api_pl_session_docs_ids.R`): Identifies final votes
9. **Final Integration**: Creates comprehensive MEP-vote grid and merges all datasets

**Key Output Files**:
- `data_out/votes/pl_votes_10.csv`: Vote metadata (wide format)
- `data_out/rcv/pl_rcv_10.csv`: Individual voting positions (long format) 
- `data_out/meps_rcv_mandate_10.csv`: Complete MEP-vote matrix including absences
- `data_out/meps/meps_dates_ids_10.csv`: MEP information with date ranges
- `data_out/bodies/`: Political group and national party lookup tables

#### All Mandates (9th + 10th)
**Primary Script**: `scripts_main/ep_rcv_mandates_all.R`
- Similar workflow to 10th mandate but covers 2019-present
- Significantly larger dataset requiring more processing time

**Important Notes**:
- The final MEP-vote file can be very large (12+ million rows for full mandates)
- Vote metadata is kept separate to manage file sizes
- Join `meps_rcv_mandate.csv` with `votes.csv` using the `notation_votingId` column for complete analysis


---

## Data Output Structure

### Directory Layout
```
data_out/
├── daily/           # Daily vote files with date stamps
├── votes/           # Vote metadata and titles
├── rcv/             # Roll-call voting positions  
├── meps/            # MEP information and date grids
├── bodies/          # Political group/national party lookup tables
├── attendance/      # Official attendance records
├── meetings/        # Plenary session metadata
├── docs_pl/         # Plenary document information
└── aggregates/      # Processed analytical outputs
```

### Key Data Files
- **Individual Votes**: Long format with one row per MEP-vote combination
- **Vote Metadata**: Wide format with one row per voting event
- **MEP Grids**: Complete matrices showing which MEPs should have been present for each vote
- **Lookup Tables**: ID-to-name mappings for political groups and national parties

---

## Important Limitations and Known Issues

### Data Reliability
⚠️ **Always cross-check against official records**: The ultimate authoritative source is the [EP finalised minutes](https://www.europarl.europa.eu/RegistreWeb/search/simpleSearchHome.htm?types=PPVD&sortAndOrder=DATE_DOCU_DESC).

### Potential Issues

**1. Data Availability Delays**
- API data may not be immediately available after votes occur
- Scripts will fail if data hasn't reached the servers yet
- **Solution**: Re-run scripts later in the day or next day

**2. Language/Translation Issues**  
- Many translations accumulate over time
- Initially, only multilingual (`mul`) or French (`.fr`) versions may be available
- Vote titles may appear in limited languages initially

**3. Data Quality Issues**
- Duplicate records can occur and require cleaning
- MEP voting intentions (corrections) are recorded separately and processed later
- Political group changes (e.g., "GUE" → "The Left") create duplicate membership records that need downstream handling

**4. Performance Considerations**
- Full mandate datasets are very large (12+ million rows)
- Excel and similar tools may not handle complete datasets
- Consider using R, Python, or database tools for analysis

**5. MEP Membership Complexity**
- MEPs can change political groups during their mandate
- National party information may be missing or incorrect in source data
- Always verify political affiliations for critical analysis

---

## Execution Environment

### Local Execution
Run scripts directly in R/RStudio after installing dependencies. All required packages are automatically installed via `pacman::p_load()`.

### GitHub Codespaces  
The repository includes a `.devcontainer` configuration for deployment in GitHub Codespaces:
- Pre-configured R environment with all dependencies
- See [r2u for Codespaces](https://eddelbuettel.github.io/r2u/vignettes/Codespaces/) for details
- Free tier available with GitHub account (see [limits](https://github.com/features/codespaces))

### Performance Notes
- **Daily scripts**: Run quickly (minutes)
- **Full mandate scripts**: Can take hours due to data volume
- **Large datasets**: As of 2024, RCV files exceed 12 million rows
- **Recommended tools**: R, Python, or database systems for analysis (Excel will truncate large files)

---

## Getting Started

### For Daily Monitoring
1. Run `scripts_main/ep_rcv_today.R` on plenary days
2. Optionally run `scripts_r/aggregate_rcv_today.R` for group-level summaries

### For Historical Analysis  
1. Run `scripts_main/ep_rcv_mandate_10.R` for current mandate data
2. Run `scripts_main/ep_rcv_mandates_all.R` for complete historical data

### For Analysis
- Use files in `analyses/` folder for example analytical workflows
- Join MEP-vote data with vote metadata using `notation_votingId`
- Refer to lookup tables in `data_out/bodies/` for human-readable labels

---

## Contributing

This repository is designed for researchers and analysts studying European Parliament voting behavior. When contributing:

1. **Test thoroughly**: Always verify output against official EP records
2. **Document changes**: Update this README when modifying workflows  
3. **Handle data carefully**: Be mindful of the large dataset sizes
4. **Respect API limits**: The caching system helps prevent excessive API calls

For questions about EP voting procedures or data interpretation, consult the [European Parliament's official documentation](https://www.europarl.europa.eu/about-parliament/en/organisation-and-rules/how-plenary-works).
