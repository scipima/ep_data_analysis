###--------------------------------------------------------------------------###
# Get Vote Files ---------------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' This script collects all the RCV files, which are lebelled `decisions` in the API.
#' Having collected them, the script proceeds to process them and store the data on disk.
#' More precisely, it first checks consistency in terms of variables being available in the API.
#' Then calls several functions to clean the Votes, RCVs, as well as other columns.
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
# REPO SETUP: check if dir exists to dump incoming & processed files ----------#
# rm(list = ls())
script_starts <- Sys.time()
cat("\n=====\nStarting to collect Decisions.\n=====\n")

source(file = here::here("scripts_r", "repo_setup.R") )

# Load parallel API function --------------------------------------------------#
source(file = here::here("scripts_r", "parallel_api_calls.R"))

# Hard code the start of the mandate ------------------------------------------#
if ( !exists("mandate_starts") ) {
  mandate_starts <- "2024-07-14"
}

#------------------------------------------------------------------------------#
## GET/meetings/{event-id}/decisions -------------------------------------------
# Returns all decisions in a single EP Meeting --------------------------------#
# EXAMPLE: "https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-2023-07-12/decisions?format=application%2Fld%2Bjson&json-layout=framed"

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
} else if ( !exists("today_date") # not a daily run
            && ( !exists(mandate_starts) # no mandate specified
                 | !mandate_starts %in% c("2024-07-14", "2019-07-01") ) ) {
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
  pl_session_docs_rcv[, activity_id := paste0("MTG-PL-", activity_date)]

  # Get MEETINGS IDs
  meet_pl_ids <- sort( unique(pl_session_docs_rcv$activity_id) )
}

### API calls with conditional parallelization --------------------------------
# Build URLs for the meetings/decisions API endpoint
api_urls <- build_decisions_urls(meet_pl_ids)

# Decide whether to use parallel processing
# Only parallelize for multiple calls (non-daily runs with many meetings)
use_parallel <- !exists("today_date") && length(meet_pl_ids) > 1

if (use_parallel) {
  cat("Multiple API calls detected - using parallel processing\n")
  # Execute parallel API calls with progress tracking and error handling
  results <- parallel_api_calls(
    urls = api_urls,
    user_agent = "renew_parlwork-prd-2.0.0",
    capacity = 490,
    fill_time_s = 300,
    timeout_s = 300,  # Increase to 5 minutes
    max_retries = 5,  # Increase retries back to 5
    show_progress = TRUE,
    extract_data = TRUE
  )

  # Extract results
  resp_list <- results$responses
  failed_calls <- results$failed_calls

} else {
  # Single call or daily run - use simple sequential processing
  cat("Single API call or daily run detected - using sequential processing\n")

  # Initialize result containers
  resp_list <- vector(mode = "list", length = length(meet_pl_ids))
  failed_calls <- rep(x = FALSE, length(resp_list))

  start_time <- Sys.time()

  for (i in seq_along(meet_pl_ids)) {
    # Create an API request
    req <- httr2::request(api_urls[i])

    # Add time-out and ignore error before performing request
    resp <- tryCatch({
      req |>
        httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
        httr2::req_timeout(300) |> # Increase to 5 minutes to allow slow responses
        httr2::req_error(is_error = ~FALSE) |>
        httr2::req_throttle(capacity = 490, fill_time_s = 300) |>
        httr2::req_retry(max_tries = 5, # Increase retries back to 5
                         backoff = ~ 2 ^ .x + runif(n = 1, min = -0.5, max = 0.5)) |>
        httr2::req_perform()
    }, error = function(e) {
      cat("\nConnection error for", meet_pl_ids[i], ":", e$message, "\n")
      list(status_code = 408) # 408 = Request Timeout
    })

    # Handle response
    if (is.list(resp) && !is.null(resp$status_code) && resp$status_code == 408) {
      # This was a timeout or connection error
      if (!exists("today_date")) {
        cat("\nWARNING: API request timed out for", meet_pl_ids[i], "\n")
        failed_calls[i] <- TRUE
      }
    } else if (httr2::resp_status(resp) == 200L) {
      # Successful response
      resp_json <- httr2::resp_body_json(resp, simplifyDataFrame = TRUE)
      if (!is.null(resp_json[["data"]]) && length(resp_json[["data"]]) > 0) {
        resp_list[[i]] <- resp_json[["data"]]
      } else {
        # API returned 200 but with empty/null data
        cat("\nNOTE: API returned empty data for", meet_pl_ids[i], "\n")
        resp_list[[i]] <- NULL
      }
    } else if (!exists("today_date") && httr2::resp_status(resp) != 200L) {
      cat("\nWARNING: API request failed for",
          meet_pl_ids[i],
          "- Status:",
          httr2::resp_status(resp),
          "\n")
      failed_calls[i] <- TRUE
    }

    # Simple progress for sequential processing
    if (length(meet_pl_ids) > 1) {
      cat("Completed", i, "of", length(meet_pl_ids), "API calls\n")
    }
  }

  end_time <- Sys.time()
  cat("Sequential processing completed in", round(as.numeric(end_time - start_time, units = "mins"), 2), "minutes\n")
}

# Sometimes we encounter empty lists - Stop if that's the case on daily execution
if ( exists("today_date")
     && ( length(resp_list) == 0L || is.null(resp_list[[1]]) ) ) {
  stop("\n=====\nAPI request failed. Data might not be available yet. Try again later.\n=====\n")
}


#------------------------------------------------------------------------------#
#### Check for breaking changes in API -----------------------------------------

#------------------------------------------------------------------------------#
#' Here we check the list of all column names in the DECISIONS files.
#' The colnames are hard coded, and then we check if new cols are included.
#------------------------------------------------------------------------------#

# This is the list of all the columns we can retrieve from all the votes as of 2024-10-29
all_colnames <- c(
  "activity_date", "activity_id", "activity_label", "activity_order",
  "activity_start_date", "comment", "decided_on_a_part_of_a_realization_of",
  "decided_on_a_realization_of", "decision_method", "decisionAboutId",
  "decisionAboutId_XMLLiteral", "decision_outcome", "had_activity_type",
  "had_decision_outcome", "eli-dl:decision_outcome", "eli-dl:activity_order",
  "had_voter_abstention", "had_voter_against", "had_voter_favor",
  "had_voter_intended_abstention", "had_voter_intended_against",
  "had_voter_intended_favor", "headingLabel", "id", "inverse_consists_of",
  "notation_votingId", "note", "number_of_attendees", "number_of_votes_abstention",
  "number_of_votes_against", "number_of_votes_favor", "recorded_in_a_realization_of",
  "referenceText", "responsible_organization_label", "type", "was_motivated_by")

# Get the list of cols in current data
colnames_rawlist <- sapply(X = resp_list, FUN = names, simplify = TRUE) |>
  unlist() |>
  unique()

if ( all( colnames_rawlist %in% all_colnames ) ) {
  cat("\n=====\nNo new columns. You're good to go.\n=====\n")
  rm(colnames_rawlist)
} else {
  missing_cols <- colnames_rawlist[!colnames_rawlist %in% all_colnames]
  cat( paste0(
      "\n=====\nYou have new columns. Check again for changes in the API. Also check for any downstream effects this may have on the processing functions. Missing cols:\n",
      missing_cols,
      "\n=====\n"
      ) )
}
# Test
# colnames_rawlist[!colnames_rawlist %in% all_colnames]


#------------------------------------------------------------------------------#
#### Check for missing Plenaries from API --------------------------------------
if ( sum(sapply(X = resp_list, FUN = is.null)) > 0L ){
  # Get the missing session IDs
  missing_sessions <- meet_pl_ids[sapply(X = resp_list, FUN = is.null)]

  # Print error message and stop execution
  stop(
    paste0(
      "\n=====\nERROR: These sessions return an empty list:\n",
      paste0(missing_sessions, collapse = ", "),
      "\n\nExecution stopped. Please investigate why these sessions failed to return data.\n=====\n"
    )
  )
}


#------------------------------------------------------------------------------#
### Save temporary file --------------------------------------------------------
if ( exists("resp_list")
     && exists("today_date") # TODAY
     && mandate_starts == "2024-07-14" ) {
  readr::write_rds(x = resp_list, file = here::here(
    "data_out", "rcv", "rcv_tmp_today.RDS") )
} else if ( exists("resp_list")
            && !exists("today_date")
            && mandate_starts == "2024-07-14" ) { # 10th mandate
  readr::write_rds(x = resp_list, file = here::here(
    "data_out", "rcv", "rcv_tmp_10.RDS") )
} else if ( exists("resp_list")
            && !exists("today_date")
            && mandate_starts == "2019-07-01" ) { # ALL mandates
  readr::write_rds(x = resp_list, file = here::here(
    "data_out", "rcv", "rcv_tmp_all.RDS") )
} else {
  stop("Something is off - The data collection for RCVs has no output.")
}

# Run time
script_ends <- Sys.time()
script_lapsed = script_ends - script_starts
cat("\n=====\nFinished collecting Decisions.\n=====\n")
print(script_lapsed) # Execution time
