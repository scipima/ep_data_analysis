###--------------------------------------------------------------------------###
# EP Votes 10th Mandate --------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' DESCRIPTION.
#' We follow 4 steps to get the daily RCVs from the EP Open Data Portal (https://data.europarl.europa.eu/en/developer-corner/opendata-api).
#' First, we grab the meetings_plenary and subset it to get the identifier for the last Plenary.
#' Second, with that identifier, we grab the RCV.
#' Third, we get the list of the current MEPs, with all the details.
#' Forth, and because the current MEPs list does not contain info on national party, we need to grab a further dataset with all the MEPs info, and subset to the last membership.
#------------------------------------------------------------------------------#

# rm(list = ls())
script_starts <- Sys.time()

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c(
  "countrycode", "data.table", "dplyr", "future.apply", "here", "httr2",
  "lubridate", "janitor", "jsonlite", "readr", "stringi", "tidyr", "tidyselect",
   "writexl") )


#------------------------------------------------------------------------------#
# REPO SETUP: check if dir structure exists; load convenience functions -------#
source(file = here::here("scripts_r", "repo_setup.R") )


# Hard code the start of the mandate ------------------------------------------#
mandate_starts <- as.Date("2024-07-14")


data_grid = data.frame(
  path = c(
    here::here("data_out", "meetings", "meetings_plenary_10.csv"),
    here::here("data_out", "attendance", "attendance_dt_10.csv"),
    here::here("data_out", "docs_pl", "pl_session_docs_10.csv")
    here::here("data_out", "votes", "votes_dt_10.csv"),
    here::here("data_out", "rcv", "rcv_dt_10.csv"),

    here::here("data_out", "votes", "votes_labels_10.csv"),
    here::here("data_out", "meps", "meps_dates_ids_10.csv"),
    here::here("data_out", "bodies", "national_parties_10.csv"),
    here::here("data_out", "bodies", "political_groups_10.csv"),
    here::here("data_out", "docs_pl", "plenary_docs.csv"),
    here::here("data_out", "votes", "votes_final_10.csv")
    ),
  script = c(
    here::here("scripts_r", "api_meetings.R"),
    here::here("scripts_r", "api_meetings_attendance.R"),
    here::here("scripts_r", "api_pl_session_docs.R")
    here::here("scripts_r", "api_meetings_decisions.R"),
    here::here("scripts_r", "api_meetings_decisions.R")

    here::here("scripts_r", "api_meetings_voteresults.R"),
    here::here("scripts_r", "api_meps.R"),
    here::here("scripts_r", "api_bodies.R"),
    here::here("scripts_r", "api_bodies.R"),
    here::here("scripts_r", "api_plenary_docs.R"),
    here::here("scripts_r", "api_plenary_session_docs.R")
  ),
  varname = c(
    "meetings_plenary",
    "attendance_dt",
    "pl_session_docs"
    "votes_dt",
    "rcv_dt",

    "meetings_voteresults_titles_10",
    "meps_dates_ids",
    "national_parties",
    "political_groups",
    "plenary_documents",
    "votes_final"
  )
)

# Load all data
apply(
  X = data_grid,
  MARGIN = 1,
  FUN = function(i_data) {
    get_api_data(
      path = i_data["path"],
      script = i_data["script"],
      varname = i_data["varname"]
    )
  }
)




#------------------------------------------------------------------------------#
## GET/meetings/ ---------------------------------------------------------------

### Download all these files is very long, make sure we do it rarely ----------#
# check whether data already exists
if ( file.exists( here::here(
  "data_out", "meetings", "meetings_plenary_10.csv") ) ) {
  # get date of last version
  mtime <- as.Date(file.info(
    here::here("data_out", "meetings", "meetings_plenary_10.csv"))[["mtime"]])
  if ( (Sys.Date() - mtime) < 1L ) {
    # read data ---------------------------------------------------------------#
    meetings_plenary <- data.table::fread(
      here::here("data_out", "meetings", "meetings_plenary_10.csv") )
  } else {
    # If the data is older than today, recreate data from source --------------#
    source(file = here::here("scripts_r", "api_meetings.R"), echo = TRUE)
  }
} else {
  # If the data is not there, create data from source -------------------------#
  source(file = here::here("scripts_r", "api_meetings.R"), echo = TRUE)
}
rm(mtime)


#------------------------------------------------------------------------------#
## GET/meetings/{event-id}/attendance ------------------------------------------

### Download all these files is very long, make sure we do it rarely ----------#
# check whether data already exists
if ( file.exists( here::here("data_out", "attendance", "attendance_dt_10.csv") ) ) {
  # get date of last version
  mtime <- as.Date(file.info(
    here::here("data_out", "attendance", "attendance_dt_10.csv"))[["mtime"]])
  if ( (Sys.Date() - mtime) < 1L ) {
    # read data ---------------------------------------------------------------#
    attendance_dt <- data.table::fread(
      here::here("data_out", "attendance", "attendance_dt_10.csv") )
  } else {
    # If the data is older than today, recreate data from source --------------#
    source(file = here::here("scripts_r", "api_meetings_attendance.R"), echo = TRUE )
  }
} else {
  # If the data is not there, create data from source -------------------------#
  source(file = here::here("scripts_r", "api_meetings_attendance.R"), echo = TRUE )
}
rm(mtime)


#------------------------------------------------------------------------------#
## GET/meetings/{event-id}/decisions -------------------------------------------

### Download all these files is very long, make sure we do it rarely ----------#
# check whether data already exists
if ( file.exists( here::here("data_out", "votes", "votes_dt_10.csv") ) ) {
  # get date of last version
  mtime <- as.Date(file.info(
    here::here("data_out", "votes", "votes_dt_10.csv"))[["mtime"]])
  if ( (Sys.Date() - mtime) < 1L ) {
    # read data ---------------------------------------------------------------#
    votes_dt <- data.table::fread( here::here("data_out", "votes", "votes_dt_10.csv") )
    rcv_dt <- data.table::fread( here::here("data_out", "rcv", "rcv_dt_10.csv") )
  } else {
    # If the data is older than today, recreate data from source --------------#
    source(file = here::here("scripts_r", "api_meetings_decisions.R"), echo = TRUE )
  }
} else {
  # If the data is not there, create data from source -------------------------#
  source(file = here::here("scripts_r", "api_meetings_decisions.R"), echo = TRUE )
}
rm(mtime)


#------------------------------------------------------------------------------#
## GET/meetings/{event-id}/vote-results ----------------------------------------

#' Get titles of votes.
#' Download all these files is very long, make sure we do it rarely

# check whether data already exists
if ( file.exists( here::here("data_out", "votes", "votes_labels_10.csv") ) ) {
  # get date of last version
  mtime <- as.Date(file.info(
    here::here("data_out", "votes", "votes_labels_10.csv"))[["mtime"]])
  if ( (Sys.Date() - mtime) < 1L ) {
    # read data ---------------------------------------------------------------#
    meetings_voteresults_titles_10 <- data.table::fread( here::here(
      "data_out", "votes", "votes_labels_10.csv") )
  } else {
    # If the data is older than today, recreate data from source --------------#
    source(file = here::here("scripts_r", "api_meetings_voteresults.R"), echo = TRUE )
  }
} else {
  # If the data is not there, create data from source -------------------------#
  source(file = here::here("scripts_r", "api_meetings_voteresults.R"), echo = TRUE )
}
rm(mtime)


###--------------------------------------------------------------------------###
## GET/meps/{mep-id} -----------------------------------------------------------
# Returns a single MEP for the specified mep ID -------------------------------#

#' Get clean data on MEPs' membership and mandate duration.

### Download all these files is very long, make sure we do it rarely ----------#
# check whether data already exists
if ( file.exists( here::here("data_out", "meps", "meps_dates_ids_10.csv") ) ) {
  # get date of last version
  mtime <- as.Date(file.info(
    here::here("data_out", "meps", "meps_dates_ids_10.csv"))[["mtime"]])
  if ( (Sys.Date() - mtime) < 7L ) {
    # read data ---------------------------------------------------------------#
    meps_dates_ids <- data.table::fread(
      here::here("data_out", "meps", "meps_dates_ids_10.csv") )
  } else {
    # If the data is older than today, recreate data from source --------------#
    source(file = here::here("scripts_r", "api_meps.R"), echo = TRUE )
  }
} else {
  # If the data is not there, create data from source -------------------------#
  source(file = here::here("scripts_r", "api_meps.R"), echo = TRUE )
}
rm(mtime)
# sapply(meps_dates_ids, function(x) sum(is.na(x))) # check


###--------------------------------------------------------------------------###
## GET/corporate-bodies/{body-id} ----------------------------------------------
# Returns a single EP Corporate Body for the specified body ID ----------------#

#' Get look-up tables for MEPs' memberships

### Download all these files is very long, make sure we do it rarely ----------#
# check whether data already exists
if ( file.exists( here::here("data_out", "bodies", "national_parties_10.csv") ) ) {
  # get date of last version
  mtime <- as.Date(file.info(
    here::here("data_out", "bodies", "national_parties_10.csv"))[["mtime"]])
  if ( (Sys.Date() - mtime) < 7L ) {
    # read data ---------------------------------------------------------------#
    national_parties <- data.table::fread(
      here::here("data_out", "bodies", "national_parties_10.csv") )
    political_groups <- data.table::fread(
      here::here("data_out", "bodies", "political_groups_10.csv") )
  } else {
    # If the data is older than today, recreate data from source --------------#
    source(file = here::here("scripts_r", "api_bodies.R"), echo = TRUE )
  }
} else {
  # If the data is not there, create data from source -------------------------#
  source(file = here::here("scripts_r", "api_bodies.R"), echo = TRUE )
}
rm(mtime)


###--------------------------------------------------------------------------###
## GET/plenary-documents -------------------------------------------------------

#' The script grabs all Plenary Documents from the EP API.

if ( file.exists( here::here("data_out", "docs_pl", "plenary_docs.csv") ) ) {
  # get date of last version
  mtime <- as.Date(file.info(
    here::here("data_out", "docs_pl", "plenary_docs.csv"))[["mtime"]])
  if ( (Sys.Date() - mtime) < 7L ) {
    # read data ---------------------------------------------------------------#
    plenary_documents <- data.table::fread( here::here(
      "data_out", "docs_pl", "plenary_docs.csv") )
  } else {
    # If the data is older than today, recreate data from source --------------#
    source(file = here::here("scripts_r", "api_plenary_docs.R"), echo = TRUE )
  }
} else {
  # If the data is not there, create data from source -------------------------#
  source(file = here::here("scripts_r", "api_plenary_docs.R"), echo = TRUE )
}
rm(mtime)


###--------------------------------------------------------------------------###
## GET/plenary-session-documents/{doc-id} - RCV --------------------------------

#' The script grabs all Plenary Sessions Documents from the EP API.
#' From the resulting files we extract the list of `final` votes.

# check whether data already exists -------------------------------------------#
if ( file.exists( here::here("data_out", "votes", "votes_final_10.csv") ) ) {
  # get date of last version
  mtime <- as.Date(file.info(
    here::here("data_out", "votes", "votes_final_10.csv"))[["mtime"]])
  if ( (Sys.Date() - mtime) < 1L ) {
    # read data ---------------------------------------------------------------#
    votes_final <- data.table::fread(
      here::here("data_out", "votes", "votes_final_10.csv") )
  } else {
    # If the data is older than today, recreate data from source --------------#
    source(file = here::here("scripts_r", "api_plenary_session_docs.R"), echo = TRUE )
  }
} else {
  # If the data is not there, create data from source -------------------------#
  source(file = here::here("scripts_r", "api_plenary_session_docs.R"), echo = TRUE )
}
rm(mtime)


#------------------------------------------------------------------------------#
## Load join functions ---------------------------------------------------------
source(file = here::here("scripts_r", "join_functions.R"), echo = TRUE)
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
## Connect RCV ID, Doc ID, and Committee ---------------------------------------
source(file = here::here("scripts_r", "rcv_doc_cmt.R"), echo = TRUE)
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
## Merge RCV with MEPs ---------------------------------------------------------
# read in file if not present already
if ( !exists("meps_dates_ids") ) {
  meps_dates_ids <- data.table::fread(file = here::here(
    "data_out", "meps", "meps_dates_ids_10.csv") ) }

# Get list of RCV dates
rcv_dates = unique(rcv_dt$activity_date)
# Filter MEPs' membership just to the RCV dates
meps_dates_ids = meps_dates_ids[activity_date %in% rcv_dates]


# Create grid with all MEPs who SHOULD have been present, by date and rcv_id --#
meps_rcv_grid <- meps_dates_ids[
  unique( rcv_dt[,  list(activity_date, rcv_id) ] ),
  on = c("activity_date"), allow.cartesian=TRUE
]
# dim(meps_rcv_grid)
# sapply(meps_rcv_grid, function(x) sum(is.na(x)))


# merge grid with RCV data ----------------------------------------------------#
meps_rcv_mandate <- rcv_dt[
  meps_rcv_grid,
  on = c("activity_date", "rcv_id", "pers_id")
]
# dim(meps_rcv_mandate)
# sapply(meps_rcv_mandate, function(x) sum(is.na(x)))

# check
if ( nrow(meps_rcv_mandate) > nrow(meps_rcv_grid) ) {
  warning("WATCH OUT: You may have duplicate records") }


#------------------------------------------------------------------------------#
### Final cleaning -------------------------------------------------------------

# Fill NAs --------------------------------------------------------------------#
# There are mismatches for 2 MEPs' political groups - fix it here
# they are: https://www.europarl.europa.eu/meps/en/226260/KAROLIN_BRAUNSBERGER-REINHOLD/history/9#detailedcardmep; https://www.europarl.europa.eu/meps/en/228604/KAROLIN_BRAUNSBERGER-REINHOLD/history/9#detailedcardmep;
# sapply(meps_rcv_mandate, function(x) sum(is.na(x)))
meps_rcv_mandate <- meps_rcv_mandate |>
  dplyr::group_by(pers_id, mandate) |>
  tidyr::fill(polgroup_id, .direction = "downup") |>
  dplyr::ungroup(pers_id) |>
  data.table::as.data.table()
# sapply(meps_rcv_mandate, function(x) sum(is.na(x)))


# Other forms of voting -------------------------------------------------------#
#' ABSENT: if both `result` and `vote_intention` are NA, then `is_absent` = 1
meps_rcv_mandate[, is_absent := data.table::fifelse(
  test = is.na(vote_intention) & is.na(result), yes = 1L, no = 0L)]
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
  test = is.na(result) & is.na(vote_intention) & is_absent == 0L, # NO VOTE but PRESENT
  yes = 1L, no = 0L) ]
# table(meps_rcv_mandate$is_absent, meps_rcv_mandate$is_novote, exclude = NULL) # check
# table(meps_rcv_mandate$result, meps_rcv_mandate$is_absent, exclude = NULL) # check
# table(meps_rcv_mandate$result, meps_rcv_mandate$is_novote, exclude = NULL) # check
# table(meps_rcv_mandate$vote_intention, meps_rcv_mandate$is_absent, exclude = NULL) # check
# table(meps_rcv_mandate$vote_intention, meps_rcv_mandate$is_novote, exclude = NULL) # check
# table(meps_rcv_mandate$vote_intention, meps_rcv_mandate$result, exclude = NULL) # check

# Recode
meps_rcv_mandate[is_absent == 1L, result := -3L]
meps_rcv_mandate[is_novote == 1L, result := -2L]
with(meps_rcv_mandate,
     table(result, vote_intention, exclude = NULL) )


# Vote data dictionary --------------------------------------------------------#
data.frame(
  result = -3 : 1,
  result_fct = c("absent", "no_vote", "against", "abstain", "for") ) |>
  data.table::fwrite(file = here::here("data_out", "votes", "vote_dict.csv") )

# Drop cols --------------------------------------------------------------------#
# We've kept the date col so far because we need it to create several vars above
meps_rcv_mandate[, c("activity_date", "is_novote", "is_absent") := NULL]

# sort table ------------------------------------------------------------------#
data.table::setkeyv(x = meps_rcv_mandate,
                    cols = c("mandate", "rcv_id", "pers_id"))
# sapply(meps_rcv_mandate, function(x) sum(is.na(x)))

# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = meps_rcv_mandate,
                   file = here::here("data_out", "meps_rcv_mandate_10.csv"),
                   verbose = TRUE)

# remove objects --------------------------------------------------------------#
rm( meps_dates_ids, meps_mandate, meps_rcv_grid )


#------------------------------------------------------------------------------#
## Aggregates ------------------------------------------------------------------
source(file = here::here("scripts_r", "aggregate_rcv.R"), echo = TRUE)


#------------------------------------------------------------------------------#
## .qmd grid ------------------------------------------------------------------#
source(file = here::here("scripts_r", "create_qmd_grid.R"), echo = TRUE)


#------------------------------------------------------------------------------#
## Reproducibility -------------------------------------------------------------
sessionInfo()
script_ends <- Sys.time()
script_lapsed = script_ends - script_starts
print(script_lapsed)
