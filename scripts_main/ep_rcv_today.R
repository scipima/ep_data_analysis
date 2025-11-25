###--------------------------------------------------------------------------###
# Daily EP Votes ---------------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' DESCRIPTION.
#' We follow 4 steps to get the daily RCVs from the EP Open Data Portal (https://data.europarl.europa.eu/en/developer-corner/opendata-api).
#' First, we grab the calendar and subset it to get the identifier for the last Plenary.
#' Second, with that identifier, we grab the RCV.
#' Third, we get the list of the current MEPs, with all the details.
#' Forth, and because the current MEPs list does not contain info on national party, we need to grab a further dataset with all the MEPs info, and subset to the last membership.
#------------------------------------------------------------------------------#

rm(list = ls())

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if (!require("pacman")) install.packages("pacman")
pacman::p_load(char = c(
  "countrycode", "data.table", "dplyr", "future.apply", "here", "httr2",
  "lubridate", "janitor", "jsonlite", "readr", "stringi", "tidyr", "tidyselect",
  "writexl") )


#------------------------------------------------------------------------------#
# REPO SETUP: check if dir exists to dump incoming & processed files ----------#
source(file = here::here("scripts_r", "repo_setup.R") )


#------------------------------------------------------------------------------#
# Hard code the start of the mandate ------------------------------------------#
mandate_starts <- "2024-07-14"

# Hard code today's date ------------------------------------------------------#
if ( !exists("today_date") ) {
  today_date <- gsub(pattern = "-", replacement = "",
                     x = as.character( Sys.Date() ) )
  # today_date <- "20250618" # test
}


#------------------------------------------------------------------------------#
## GET/meetings/{event-id}/attendance ---------------------------------------

# API parameter
if ( !exists("activity_id_today") ) {
  activity_id_today <- paste0("MTG-PL-", Sys.Date())
  # activity_id_today <- paste0("MTG-PL-", "2025-06-18") # test
}
# Call the API for MEETINGS and Attendance
source(file = here::here("scripts_r", "api_meetings.R"),
       echo = TRUE, local = TRUE)
source(file = here::here("scripts_r", "api_meetings_attendance.R"),
       echo = TRUE, local = TRUE)


#------------------------------------------------------------------------------#
## GET/meetings/{event-id}/decisions -------------------------------------------
# Returns all decisions in a single EP Meeting
# EXAMPLE: "https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-2024-04-23/decisions?format=application%2Fld%2Bjson&json-layout=framed&offset=0"

source(file = here::here("scripts_r", "api_meetings_decisions.R"),
       echo = TRUE, local = TRUE)

# Clean Decisions from JSONs --------------------------------------------------#
source(file = here::here("scripts_r", "clean_decisions.R"),
       echo = TRUE, local = TRUE)

# Read in file if not present already -----------------------------------------#
# Votes DF
if ( !exists("pl_votes") ) {
  pl_votes = data.table::fread(file = here::here(
    "data_out", "votes", "pl_votes_today.csv") ) }

# RCVs DF
if ( !exists("pl_rcv") ) {
  pl_rcv = data.table::fread(file = here::here(
    "data_out", "rcv", "pl_rcv_today.csv") ) }

#------------------------------------------------------------------------------#
## GET/meetings/{event-id}/vote-results ----------------------------------------
# Returns all vote results in a single EP Meeting
# EXAMPLE: https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-2024-09-19/vote-results?format=application%2Fld%2Bjson&offset=0

#' Get titles of votes.

source(file = here::here("scripts_r", "api_meetings_voteresults.R"),
       echo = TRUE, local = TRUE)


# Get vote labels of the day --------------------------------------------------#
row_before <- dim(pl_votes)[1]
pl_votes <- pl_votes |>
  dplyr::mutate(
    inverse_consists_of = gsub(pattern = "eli/dl/event/", replacement = "",
                               x = inverse_consists_of, fixed = TRUE)
  ) |>
  dplyr::left_join(
    y = votes_labels,
    by = c("inverse_consists_of" = "activity_id")
  ) |>
  data.table::as.data.table()
row_after <- dim(pl_votes)[1]
# Check
if (row_before != row_after) {stop("Mismatch in voting file!")}


#------------------------------------------------------------------------------#
### DOC_ID; FINALS -------------------------------------------------------------
# quick and dirty way to get doc_id out of labels -----------------------------#
if ("activity_label_mul" %in% names(pl_votes) ) {

  # Flag for final ----------------------------------------------------------#
  pl_votes[
    grepl(pattern = "vote unique|vote final|vote\\:.*|ensembl*e du texte|Election|Proposition de la Commission|Proc.dure d.approbation|proc.dure d.urgence|Demande de d.cision d.urgence|Approbation|Demande de vote|Projet de d.cision du Conseil|Projet du Conseil|D.cision d.engager des n.gociations interinstitutionnelles|Proposition de r.solution|Accord\\s?provisoire|Proposition.? de d.cision|Projet de recommandation|Proposition de recommandation|Demande de procéder au vote sur les amendements|Demande de mettre aux voix les amendements|Projet de d.cision|Projet de directive du Conseil|Projet de reglement|.lection de la Commission|Projet commun|Recommandation de d.cision|D.cision du maintien du recours",
          x = activity_label_mul, ignore.case = TRUE, perl = TRUE),
    `:=`( is_final = 1L ) ]

  # Deal with special cases
  pl_votes[
    # If this is present ...
    grepl(pattern = "r.solution|d.cision|approbation|Accord\\s?provisoire",
          x = activity_label_mul, ignore.case = TRUE, perl = TRUE)
    # ... AND this is present ...
    & grepl(pattern = "Am \\d+|§ \\d+|Consid.rant|recommandation",
            x = activity_label_mul, ignore.case = FALSE, perl = TRUE),
    # ... THEN it's not final
    `:=`( is_final = 0L ) ]

  # If NA, then 0
  pl_votes[is.na(is_final), is_final := 0L]
} else if ("activity_label_fr" %in% names(pl_votes) ) {
  pl_votes[
    grepl(pattern = "vote unique|vote final|vote\\:.*|ensembl*e du texte|Election|Proposition de la Commission|Proc.dure d.approbation|proc.dure d.urgence|Demande de d.cision d.urgence|Approbation|Demande de vote|Projet de d.cision du Conseil|Projet du Conseil|D.cision d.engager des n.gociations interinstitutionnelles|Proposition de r.solution|Accord\\s?provisoire|Proposition.? de d.cision|Projet de recommandation|Proposition de recommandation|Demande de procéder au vote sur les amendements|Demande de mettre aux voix les amendements|Projet de d.cision|Projet de directive du Conseil|Projet de reglement|.lection de la Commission|Projet commun|Recommandation de d.cision|D.cision du maintien du recours",
          x = activity_label_fr, ignore.case = TRUE, perl = TRUE),
    `:=`( is_final = 1L ) ]

  pl_votes[
    grepl(pattern = "vote unique|vote final|vote\\:.*|ensembl*e du texte|Election|Proposition de la Commission|Proc.dure d.approbation|proc.dure d.urgence|Demande de d.cision d.urgence|Approbation|Demande de vote|Projet de d.cision du Conseil|Projet du Conseil|D.cision d.engager des n.gociations interinstitutionnelles|Proposition de r.solution|Accord\\s?provisoire|Proposition.? de d.cision|Projet de recommandation|Proposition de recommandation|Demande de procéder au vote sur les amendements|Demande de mettre aux voix les amendements|Projet de d.cision|Projet de directive du Conseil|Projet de reglement|.lection de la Commission|Projet commun|Recommandation de d.cision|D.cision du maintien du recours|Nomination de ",
          x = referenceText_fr, ignore.case = TRUE, perl = TRUE),
    `:=`( is_final = 1L ) ]

  # Deal with special cases
  pl_votes[
    # If this is present ...
    grepl(pattern = "r.solution|d.cision|approbation|Accord\\s?provisoire|recommandation",
          x = activity_label_fr, ignore.case = TRUE, perl = TRUE)
    # ... AND this is  present ...
    & grepl(pattern = "Am \\d+|§ \\d+|Consid.rant",
            x = activity_label_fr, ignore.case = FALSE, perl = TRUE),
    # ... THEN it's not final
    `:=`( is_final =  0L ) ]

  # If NA, then 0
  pl_votes[is.na(is_final), is_final := 0L]

}


### Write data to disk ---------------------------------------------------------
data.table::fwrite(x = pl_votes,
                   file = here::here("data_out", "daily",
                                     paste0("votes_today_", today_date, ".csv") ) )


###--------------------------------------------------------------------------###
## Current EP Composition ------------------------------------------------------
meps_current <- data.table::fread(here::here(
  "data_out", "meps", "meps_current.csv"),
  na.strings = c(NA_character_, "") )
# source(file = here::here("scripts_r", "meps_current.R"))


###--------------------------------------------------------------------------###
# Clean final data --------------------------------------------------------------#
pl_rcv[, rcv_id := as.integer(rcv_id)]


#------------------------------------------------------------------------------#
## Merge RCV with MEPs ---------------------------------------------------------
# Create a grid with all MEPs who SHOULD have been present
meps_rcv_grid <- tidyr::expand_grid(
  meps_current,
  rcv_id = unique(pl_rcv$rcv_id) ) |>
  data.table::as.data.table()

# merge grid with RCV data
meps_rcv_today <- pl_rcv[meps_rcv_grid,
                         on = c("pers_id", "rcv_id")]

# check
if ( nrow(meps_rcv_today) > nrow(meps_rcv_grid) ) {
  warning("WATCH OUT: You may have duplicate records") }


#------------------------------------------------------------------------------#
# Final cleaning --------------------------------------------------------------#
# Flag for ABSENT
meps_rcv_today[, result := fifelse(
  test = !pers_id %in% pl_rcv$pers_id, # do we have a vote for MEP?
  yes = -3L, no = result) ]
# Flag for DID NOT VOTE
meps_rcv_today[, result := fifelse(
  test = is.na(result), # NO VOTE but PRESENT
  yes = -2L, no = result) ]


# Sort data
data.table::setkeyv(x = meps_rcv_today, cols = c("rcv_id", "pers_id") )

### Write data to disk ---------------------------------------------------------
data.table::fwrite(x = meps_rcv_today,
                   file = here::here("data_out", "daily",
                                     paste0("rcv_today_", today_date, ".csv") ) )

