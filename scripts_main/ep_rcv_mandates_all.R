#------------------------------------------------------------------------------#
# EP Votes All Mandates --------------------------------------------------------
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
#' DESCRIPTION.
#' We follow 4 steps to get the daily RCVs from the EP Open Data Portal (https://data.europarl.europa.eu/en/developer-corner/opendata-api).
#' First, we grab the meetings_plenary and subset it to get the identifier for the last Plenary.
#' Second, with that identifier, we grab the RCV.
#' Third, we get the list of the current MEPs, with all the details.
#' Forth, and because the current MEPs list does not contain info on national party, we need to grab a further dataset with all the MEPs info, and subset to the last membership.
#------------------------------------------------------------------------------#

# rm(list = ls())
master_starts <- Sys.time()

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c("curl", "data.table", "dplyr", "tidyr", "tidyselect",
                        "future.apply", "httr", "httr2", "here", "lubridate",
                        "janitor", "jsonlite", "readr", "stringi") )


#------------------------------------------------------------------------------#
# REPO SETUP: check if dir exists to dump incoming & processed files ----------#
source(file = here::here("scripts_r", "repo_setup.R") )


# Hard code the start of the mandate ------------------------------------------#
mandate_starts <- as.Date("2019-07-01")


#------------------------------------------------------------------------------#
## GET/meetings/ ---------------------------------------------------------------
#' Download all these files may take very long, make sure we do it rarely

pl_meetings <- get_api_data(
  path = here::here("data_out", "meetings", "pl_meetings_all.csv"),
  script = here::here("scripts_r", "api_meetings.R"),
  max_days = 1,
  varname = "pl_meetings",
  envir = .GlobalEnv
)


###--------------------------------------------------------------------------###
## GET/meetings/{event-id}/attendance ------------------------------------------

pl_attendance <- get_api_data(
  path = here::here("data_out", "attendance", "pl_attendance_all.csv"),
  script = here::here("scripts_r", "api_meetings_attendance.R"),
  max_days = 1,
  varname = "pl_attendance",
  envir = .GlobalEnv
)


###--------------------------------------------------------------------------###
## GET/meetings/{event-id}/decisions -------------------------------------------

invisible(get_api_data(
  path = here::here("data_out", "rcv", "rcv_tmp_all.RDS"),
  script = here::here("scripts_r", "api_meetings_decisions.R"),
  max_days = 1,
  varname = NULL,
  envir = .GlobalEnv
))

### Clean Decisions from JSONs ------------------------------------------------#
# After storing the API dump in a tmp .RDS file, we process it here
source(file = here::here("scripts_r", "clean_decisions.R"),
       echo = TRUE, local = TRUE)


#------------------------------------------------------------------------------#
## GET/meetings/{event-id}/vote-results ----------------------------------------

#' Get titles of votes.
#' Download all these files is very long, make sure we do it rarely

meetings_voteresults_titles_10 <- get_api_data(
  path = here::here("data_out", "votes", "votes_labels_all.csv"),
  script = here::here("scripts_r", "api_meetings_voteresults.R"),
  max_days = 1,
  varname = "meetings_voteresults_titles_10",
  envir = .GlobalEnv
)


###--------------------------------------------------------------------------###
## GET/meps/{mep-id} -----------------------------------------------------------
# Returns a single MEP for the specified mep ID -------------------------------#

#' Get clean data on MEPs' membership and mandate duration.

### Download all these files is very long, make sure we do it rarely ----------#
meps_dates_ids <- get_api_data(
  path = here::here("data_out", "meps", "meps_dates_ids_all.csv"),
  script = here::here("scripts_r", "api_meps.R"),
  max_days = 1,
  varname = "meps_dates_ids",
  envir = .GlobalEnv
)
# sapply(meps_dates_ids, function(x) sum(is.na(x))) # check


###--------------------------------------------------------------------------###
## GET/corporate-bodies/{body-id} ----------------------------------------------
# Returns a single EP Corporate Body for the specified body ID ----------------#

#' Get look-up tables for MEPs' memberships

### Download all these files is very long, make sure we do it rarely ----------#
invisible(get_api_data(
  path = here::here("data_out", "bodies", "national_parties_all.csv"),
  script = here::here("scripts_r", "api_bodies.R"),
  max_days = 1,
  varname = NULL,
  envir = .GlobalEnv
))
national_parties <- data.table::fread(here::here("data_out", "bodies", "national_parties_all.csv"))
political_groups <- data.table::fread(here::here("data_out", "bodies", "political_groups_all.csv"))


###--------------------------------------------------------------------------###
## GET/plenary-documents -------------------------------------------------------

#' The script grabs all Plenary Documents from the EP API.

pl_documents <- get_api_data(
  path = here::here("data_out", "docs_pl", "pl_docs.csv"),
  script = here::here("scripts_r", "api_pl_docs.R"),
  max_days = 1,
  varname = "pl_documents",
  envir = .GlobalEnv
)


##----------------------------------------------------------------------------##
## GET/plenary-session-documents/{doc-id} - RCV --------------------------------

#' The script grabs all Plenary Sessions Documents from the EP API.
#' From the resulting files we extract the list of `final` votes.

# check whether data already exists -------------------------------------------#
votes_final <- get_api_data(
  path = here::here("data_out", "votes", "votes_final_all.csv"),
  script = here::here("scripts_r", "api_pl_session_docs_ids.R"),
  max_days = 1,
  varname = "votes_final",
  envir = .GlobalEnv
)


###--------------------------------------------------------------------------###
## Load join functions ---------------------------------------------------------

#' This brings in a set of convenience functions to merge data.

source(file = here::here("scripts_r", "join_functions.R"), echo = TRUE)
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
## Connect RCV ID, Doc ID, and Committee ---------------------------------------
source(file = here::here("scripts_r", "rcv_doc_cmt.R"), echo = TRUE)
#------------------------------------------------------------------------------#


###--------------------------------------------------------------------------###
## Merge RCV with MEPs ---------------------------------------------------------

# Read in file if not present already -----------------------------------------#
# Votes DF
if ( !exists("pl_votes") ) {
  pl_votes = data.table::fread(file = here::here(
    "data_out", "votes", "pl_votes_all.csv") ) }

if ("is_final" %in% names(pl_votes)) {
  cat("\n=====\nThe vote file has a flag for final votes.\n=====\n")
} else {
  cat("\n=====\nThe vote file does not have a flag for final votes. Appending it now ...\n=====\n")
  pl_votes = votes_final[
    pl_votes,
    on = "rcv_id"
  ]
  data.table::fwrite(x = pl_votes, file = here::here(
    "data_out", "votes", "pl_votes_all.csv") )
}

# RCVs DF
if ( !exists("pl_rcv") ) {
  pl_rcv = data.table::fread(file = here::here(
    "data_out", "rcv", "pl_rcv_all.csv") ) }

# MEPs & Dates
if ( !exists("meps_dates_ids") ) {
  meps_dates_ids <- data.table::fread(file = here::here(
    "data_out", "meps", "meps_dates_ids_all.csv") ) }

# Get list of RCV dates
rcv_dates = unique(pl_rcv$activity_date)
# Filter MEPs' membership just to the RCV dates
meps_dates_ids = meps_dates_ids[activity_date %in% rcv_dates]


# Create grid with all MEPs who SHOULD have been present, by date and rcv_id --#
meps_rcv_grid <- meps_dates_ids[
  unique( pl_rcv[,  list(activity_date, rcv_id) ] ),
  on = c("activity_date"), allow.cartesian=TRUE
]
# dim(meps_rcv_grid)
# sapply(meps_rcv_grid, function(x) sum(is.na(x)))


# merge grid with RCV data ----------------------------------------------------#
meps_rcv_mandate <- pl_rcv[
  meps_rcv_grid,
  on = c("activity_date", "rcv_id", "pers_id")
]
# dim(meps_rcv_mandate)
# sapply(meps_rcv_mandate, function(x) sum(is.na(x)))

# check
if ( nrow(meps_rcv_mandate) > nrow(meps_rcv_grid) ) {
  warning("WATCH OUT: You may have duplicate records") }


###--------------------------------------------------------------------------###
### Final cleaning -------------------------------------------------------------

# Fill NAs --------------------------------------------------------------------#
#' There are mismatches for 2 MEPs' political groups - fix it here
#' they are: https://www.europarl.europa.eu/meps/en/226260/KAROLIN_BRAUNSBERGER-REINHOLD/history/9#detailedcardmep; https://www.europarl.europa.eu/meps/en/228604/KAROLIN_BRAUNSBERGER-REINHOLD/history/9#detailedcardmep;
#' Carry the last observation forward and then carries the next observation backward to fill NA values within each group.
#' In `data.table`, we can achieve this by chaining two calls to nafill() using
#' - the locf (Last Observation Carried Forward)
#' - and nocb (Next Observation Carried Backward) types

# sapply(meps_rcv_mandate, function(x) sum(is.na(x))) # check
if( any( is.na(meps_rcv_mandate$polgroup_id) ) ) {
  cat("\n=====\nFilling missing values for polgroup_id...\n=====\n")
  meps_rcv_mandate[, `:=`(
    polgroup_id = data.table::nafill(
      data.table::nafill(
        polgroup_id, type = "locf"), # Last Observation Carried Forward
      type = "nocb") # Next Observation Carried Backward
    ),
    by = .(pers_id, mandate)
  ]
} else {
  cat("\n=====\nNo missing values for polgroup_id found.\n=====\n")
}
# Tidyverse version
# meps_rcv_mandate <- meps_rcv_mandate |>
#   dplyr::group_by(pers_id, mandate) |>
#   tidyr::fill(polgroup_id, .direction = "downup") |>
#   dplyr::ungroup(pers_id) |>
#   data.table::as.data.table()

# sapply(meps_rcv_mandate, function(x) sum(is.na(x)))


# Other forms of voting -------------------------------------------------------#
#' ABSENT: if `result` is NA, then `is_absent` = 1
meps_rcv_mandate[, is_absent := data.table::fifelse(
  test = is.na(result), yes = 1L, no = 0L)]
# table(meps_rcv_mandate$is_absent)

#' True absent are absent the entire day, so take the average of `is_absent`
meps_rcv_mandate[, is_absent_avg := mean(is_absent, na.rm = TRUE),
                 by = list(activity_date, pers_id) ]
# head( sort( unique(meps_rcv_mandate$is_absent_avg) ) )

#' Convert `is_absent` to binary
meps_rcv_mandate[, is_absent := data.table::fifelse(
  test = is_absent_avg == 1, yes = 1L, no = 0L) ]
meps_rcv_mandate[, c("is_absent_avg") := NULL]

# Flag for DID NOT VOTE
meps_rcv_mandate[, is_novote := data.table::fifelse(
  test = is.na(result) & is_absent == 0L, # NO VOTE but PRESENT
  yes = 1L, no = 0L) ]
# table(meps_rcv_mandate$is_absent, meps_rcv_mandate$is_novote, exclude = NULL) # check
# table(meps_rcv_mandate$result, meps_rcv_mandate$is_absent, exclude = NULL) # check
# table(meps_rcv_mandate$result, meps_rcv_mandate$is_novote, exclude = NULL) # check

# Recode
meps_rcv_mandate[is_absent == 1L, result := -3L]
meps_rcv_mandate[is_novote == 1L, result := -2L]


# Vote data dictionary --------------------------------------------------------#
data.frame(
  result = -3 : 1,
  result_fct = c("absent", "no_vote", "against", "abstain", "for") ) |>
  data.table::fwrite(file = here::here("data_out", "votes", "vote_dict.csv") )

# Drop cols -------------------------------------------------------------------#
# We've kept the date col so far because we need it to create several vars above
meps_rcv_mandate[, c("activity_date", "is_novote", "is_absent") := NULL]

# sort table ------------------------------------------------------------------#
data.table::setkeyv(x = meps_rcv_mandate,
                    cols = c("mandate", "rcv_id", "pers_id"))
# sapply(meps_rcv_mandate, function(x) sum(is.na(x)))


#------------------------------------------------------------------------------#
## Save data to disk -----------------------------------------------------------
data.table::fwrite(x = meps_rcv_mandate,
                   file = here::here("data_out", "meps_rcv_mandate_all.csv"),
                   verbose = TRUE)

#------------------------------------------------------------------------------#
# Clean up before exiting -----------------------------------------------------#
rm( meps_dates_ids, meps_mandate, meps_rcv_grid )

# Run all aggregates
source(file = here::here("scripts_r", "aggregate_rcv.R") )


#------------------------------------------------------------------------------#
## Reproducibility
sessionInfo()
master_ends <- Sys.time()
master_lapsed = master_ends - master_starts
cat("\n=====\nFinished creating All Mandates Data.\n=====\n")
print(master_lapsed)# Execution time
