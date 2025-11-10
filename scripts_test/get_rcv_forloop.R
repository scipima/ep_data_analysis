###--------------------------------------------------------------------------###
# Get RCV ----------------------------------------------------------------------
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
#' `get_rcv_forloop.R` harvests all RCVs from the *EP Public Registry of Documents*.
#' While `get_rcv_forloop.R` is stored in the `tests` folder, a parallelised version is stored in the `source_files` folder (`get_rcv_parallel.R`).
#' Unfortunately the latter frequently crashes for unclear reasons (perhaps a rate limit is present which kills the multiple frequent queries), so the slower '*for loop*' version is preferable.
###--------------------------------------------------------------------------###

## Libraries -------------------------------------------------------------------
library(data.table)
library(xml2)
library(lubridate)
library(here)
library(httr)


#------------------------------------------------------------------------------#
# REPO SETUP: check if dir exists to dump incoming & processed files ----------#
# rm(list = ls())
script_starts <- Sys.time()
cat("\n=====\nStarting to collect RCV data.\n=====\n")

source(file = here::here("scripts_r", "repo_setup.R"))

# Source our improved XML parsing function
source(file = here::here("parse_rcv_xml.R"))

# Hard code the start of the mandate ------------------------------------------#
if ( !exists("mandate_starts") ) {
  mandate_starts <- "2024-07-14"
}


### -------------------------------------------------------------------------###
## Create grid of dates --------------------------------------------------------

#' The URL has this form: https://www.europarl.europa.eu/RegData/seance_pleniere/proces_verbal/2025/10-23/liste_presence/P10_PV(2025)10-23(RCV)_FR.xml
#' In order to get those, you have to navigate to:
#'  1) https://www.europarl.europa.eu/RegistreWeb/search/simpleSearchHome.htm?sortAndOrder=DATE_DOCU_DESC
#'  2) In the drop down menu, select '1.2.4.2 Finalised minutes - Plenary documents'


### Load list of RCVs Plenaries ------------------------------------------------
if ( !exists("today_date") && mandate_starts == "2019-07-01" ) {
  get_api_data(
    path = here::here("data_out", "docs_pl", "pl_session_docs_all.csv"),
    script = here::here("scripts_r", "api_pl_session_docs.R"),
    varname = "pl_session_docs",
  )
} else if (!exists("today_date") && mandate_starts == "2024-07-14" ) {
  get_api_data(
    path = here::here("data_out", "docs_pl", "pl_session_docs_10.csv"),
    script = here::here("scripts_r", "api_pl_session_docs.R"),
    varname = "pl_session_docs",
  )
} else # Validity checks
if (exists("today_date")) {
  mandate_starts <- Sys.Date() - 7
} else if ( !exists("today_date") # not a daily run
            && ( !exists("mandate_starts") # no mandate specified
                 || !mandate_starts %in% c("2024-07-14", "2019-07-01") ) ) {
  stop("\n=====\nYou need to specify a valid timeframe.\nThis could be either the starting date for a mandate (currently: '2019-07-01', '2024-07-14'), or today's date.\n=====\n")
}


# Extract vector of Plenary Dates ---------------------------------------------#
if ( exists("today_date") ) {
  meet_pl_ids = activity_id_today
} else {
  # Extract date
  pl_session_docs[, `:=`(
    activity_date = data.table::as.IDate(
      gsub(pattern = "PV-\\d{1,2}-|-RCV",
           replacement = "", x = identifier, perl = TRUE) ) )
  ]
  # Subset to relevant Plenary dates
  pl_session_docs_rcv = pl_session_docs[
    activity_date >= mandate_starts
    & grepl(pattern = "-RCV", x = id, fixed = TRUE)
  ]
  # Create Plenary ID
  pl_session_docs_rcv[, link := paste0(
    "https://www.europarl.europa.eu/RegData/seance_pleniere/proces_verbal/",
    year(activity_date), "/",
    substr(x = activity_date, start = 6, stop = 10),
    "/liste_presence/",
    label,
    "_FR.xml")]
  # Get MEETINGS IDs
  meet_pl_ids <- sort( unique(pl_session_docs_rcv$link) )
}


### -------------------------------------------------------------------------###
## Get all files ---------------------------------------------------------------
# Initialize lists for results
all_sessions_data <- vector(mode = "list", length = length(meet_pl_ids))
all_votes_data <- vector(mode = "list", length = length(meet_pl_ids))
counter <- 0L

# Main processing loop
for (link in seq_along(meet_pl_ids)) {
  Sys.sleep(time = 2)
  counter <- counter + 1L
  cat("Processing file", counter, "of", length(meet_pl_ids), ":", meet_pl_ids[link], "\n")
  
  tryCatch({
    # Parse XML using our improved function
    parsed_data <- parse_rcv_xml(meet_pl_ids[link])
    
    # Store session metadata with file reference
    session_meta <- parsed_data$session_metadata
    session_meta$file_link <- meet_pl_ids[link]
    session_meta$file_number <- counter
    all_sessions_data[[counter]] <- session_meta
    
    # Store vote data with session reference
    vote_data <- parsed_data$individual_votes
    if (nrow(vote_data) > 0) {
      vote_data$file_link <- meet_pl_ids[link] 
      vote_data$file_number <- counter
      all_votes_data[[counter]] <- vote_data
    }
    
  }, error = function(e) {
    cat("Error processing file", counter, ":", conditionMessage(e), "\n")
    # Store empty data frames to maintain list structure
    all_sessions_data[[counter]] <- data.frame(file_link = meet_pl_ids[link], 
                                              file_number = counter, error = TRUE)
    all_votes_data[[counter]] <- data.frame()
  })
}

### Process results and write output -------------------------------------------

# Combine all results
cat("Combining results...\n")
rcv_allsessions <- data.table::rbindlist(all_votes_data, fill = TRUE)
session_metadata <- data.table::rbindlist(all_sessions_data, fill = TRUE)

# Clean column names and dimensions
if (nrow(rcv_allsessions) > 0) {
  cat("Total records:", nrow(rcv_allsessions), "\n")
  
  # Extract legislation report number using base R
  rcv_allsessions[, report_number := {
    # Pattern: A9-0123/2021 or similar
    pattern <- "[A-Z][8-9]-\\d{4}/\\d{4}|[A-Z]{2}-[A-Z]9-\\d{4}/\\d{4}"
    sapply(rcv_description_text, function(x) {
      if (is.na(x)) return(NA)
      match <- regexpr(pattern, x, perl = TRUE)
      if (match > 0) {
        regmatches(x, match)
      } else {
        NA_character_
      }
    })
  }]
  
  # Clean empty strings to NA
  cols_to_clean <- names(rcv_allsessions)
  rcv_allsessions[, (cols_to_clean) := lapply(.SD, function(x) {
    ifelse(x == "" || is.null(x), NA_character_, as.character(x))
  }), .SDcols = cols_to_clean]
  
  # Manual corrections for data entry mistakes
  rcv_allsessions[
    grepl("\\nA9-057/2021 - Olivier Chastel", rcv_description_text, ignore.case = TRUE, perl = TRUE),
    report_number := "A9-0057/2021"]
  rcv_allsessions[
    grepl("A-0269/2020 ", rcv_description_text, ignore.case = TRUE, perl = TRUE),
    report_number := "A9-0269/2020"]
  rcv_allsessions[
    grepl("A9-0088/202 - Clara Aguilera - Proc", rcv_description_text, ignore.case = TRUE, perl = TRUE),
    report_number := "A9-0088/2020"]
  
  # Write file
  data.table::fwrite(
    x = rcv_allsessions, 
    file = here::here("data_out", "rcv_allsessions.csv"),
    showProgress = TRUE)
  
  cat("Successfully wrote rcv_allsessions.csv with", nrow(rcv_allsessions), "records\n")
  
} else {
  cat("No data to write - all parsing failed\n")
}
