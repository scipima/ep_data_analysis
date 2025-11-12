###--------------------------------------------------------------------------###
# Connect RCV ID, Doc ID, and Committee ----------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' This script brings together information regarding Plenary Documents coming from different sources.
#' The goal is to associate as many Plenary Votes as possible with Documents and, as a consequence, Committees.
#' In terms of workflow, this script is meant to be executed at the end of the data collection, when all API endpoints have been called and data is as recent as possible.
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c(
    "data.table", "dplyr", "here", "lubridate", "janitor", "readr", "stringi",
    "tidyr", "tidyselect")
)

# Hard code the start of the mandate ------------------------------------------#
if ( !exists("mandate_starts") ) {
    mandate_starts = "2024-07-14" }

#------------------------------------------------------------------------------#
## Data ------------------------------------------------------------------------
### rcvid_docid ----------------------------------------------------------------

#' Data coming from regex extraction of strings associated with Votes (API: Meetings/Decisions)

if ( !exists("rcvid_docid") ) {
    # Read data conditional on mandate ----------------------------------------#
    if (mandate_starts == as.Date("2019-07-01")) {
        rcvid_docid <- data.table::fread(input = here::here(
            "data_out", "rcv", "rcvid_docid_all.csv"),
            na.strings = c("", NA_character_, "datatable.na.strings") )
    } else if ( mandate_starts == as.character("2024-07-14") ) {
        rcvid_docid <- data.table::fread(input = here::here(
            "data_out", "rcv", "rcvid_docid_10.csv"),
            na.strings = c("", NA_character_, "datatable.na.strings") )
    } }
rcvid_docid[doc_id == "", doc_id := NA] # clean NAs
# Check
sapply(rcvid_docid, function(x) sum(is.na(x)))
length(unique(rcvid_docid$doc_id))
length(unique(rcvid_docid$rcv_id))
dt_tmp <- rcvid_docid[, .N, by = rcv_id]
# Data quality check
if (mean(dt_tmp$N) > 1) {
    stop("!!You cannot have more than 1 dossier per vote!! Check your data.")
}
rm(dt_tmp)


### pl_docs_committee ----------------------------------------------------------
#' Data coming from the Plenary Documents endpoint.
if ( !exists("pl_docs_committee") ) {
    # Read data conditional on mandate ----------------------------------------#
    pl_docs_committee <- data.table::fread(input = here::here(
        "data_out", "docs_pl", "pl_docs_committee.csv"),
        na.strings = c(NA_character_, "")  )
}
# sapply(pl_docs_committee, function(x) sum(is.na(x)))
pl_docs_committee[, .N, by = doc_id][order(N)]

#' Here we're getting also Joint Committees.
#' For instance: A9-0248/2022; AFCO, AFET, INTA, CJ41.


### rcv_inverse_consists_of ----------------------------------------------------
#' Get `EVENT/VOT-ITM` for every `DEC-ITM`
#' Data coming from API: Meetings/Decisions

if ( !exists("rcv_inverse_consists_of") ) {
    # Read data conditional on mandate ----------------------------------------#
    if (mandate_starts == as.Date("2019-07-01")) {
        rcv_inverse_consists_of <- data.table::fread(input = here::here(
            "data_out", "rcv", "rcv_inverse_consists_of_all.csv"),
            na.strings = c(NA_character_, "")  )
    } else if ( mandate_starts == as.character("2024-07-14") ) {
        rcv_inverse_consists_of <- data.table::fread(input = here::here(
            "data_out", "rcv", "rcv_inverse_consists_of_10.csv"),
            na.strings = c(NA_character_, "")  ) } }
length(unique(rcv_inverse_consists_of$notation_votingId))
length(unique(rcv_inverse_consists_of$vote_id))


### rcv_inverse_consists_of ----------------------------------------------------
#' Get DOC_ID for every `DEC-ITM`
#' Data coming from API: Meetings/Decisions

if ( !exists("rcv_decided_on_a_realization_of") ) {
    # Read data conditional on mandate ----------------------------------------#
    if (mandate_starts == as.Date("2019-07-01")) {
        rcv_decided_on_a_realization_of <- data.table::fread(input = here::here(
            "data_out", "rcv", "rcv_decided_on_a_realization_of_all.csv"),
            na.strings = c(NA_character_, "")  )
    } else if ( mandate_starts == as.character("2024-07-14") ) {
        rcv_decided_on_a_realization_of <- data.table::fread(input = here::here(
            "data_out", "rcv", "rcv_decided_on_a_realization_of_10.csv"),
            na.strings = c(NA_character_, "")  )
    } }


### votes_based_on_a_realization_of --------------------------------------------
#' Get DOC_ID for every `VOT-ITM`
if ( !exists("votes_based_on_a_realization_of") ) {
    # Read data conditional on mandate ----------------------------------------#
    if (mandate_starts == as.Date("2019-07-01")) {
        votes_based_on_a_realization_of <- data.table::fread(input = here::here(
            "data_out", "votes", "votes_based_on_a_realization_of_all.csv"),
            na.strings = c(NA_character_, "")  )
    } else if ( mandate_starts == as.character("2024-07-14") ) {
        votes_based_on_a_realization_of <- data.table::fread(input = here::here(
            "data_out", "votes", "votes_based_on_a_realization_of_10.csv"),
            na.strings = c(NA_character_, "")  )
    } }

### procedures & committee --------------------------------------------
#' Get Committee for every Process ID
if ( !exists("procedures_cmt") ) {
    # Read data conditional on mandate ----------------------------------------#
    if (mandate_starts == as.Date("2019-07-01")) {
        procedures_cmt <- data.table::fread(input = here::here(
            "data_out", "procedures", "procedures_cmt_all.csv"),
            na.strings = c(NA_character_, "")  )
    } else if ( mandate_starts == as.character("2024-07-14") ) {
        procedures_cmt <- data.table::fread(input = here::here(
            "data_out", "procedures", "procedures_cmt_10.csv"),
            na.strings = c(NA_character_, "") ) } }

### procedures & votes --------------------------------------------
if ( !exists("today_date")
     && mandate_starts == as.character("2019-07-01") ) {
    votes_inverse_consists_of = data.table::fread(file = here::here(
        "data_out", "votes", "votes_inverse_consists_of_all.csv") )
} else if ( !exists("today_date")
            && mandate_starts == as.character("2024-07-14") ) {
    votes_inverse_consists_of = data.table::fread(file = here::here(
        "data_out", "votes", "votes_inverse_consists_of_10.csv") ) }
votes_inverse_consists_of[, vot_id := gsub(
    pattern = "eli/dl/event/", replacement = "", x = event_vot_itm_id)]

#------------------------------------------------------------------------------#
## RCV ID and Committees -------------------------------------------------------
rcvid_cmts <- rcvid_docid |>
    dplyr::full_join(
        y = rcv_inverse_consists_of,
        by = c("rcv_id" = "notation_votingId") ) |>
    # BREAK THE CODE HERE AND TRACK DOWN NAs IN DOCIDs
    dplyr::full_join(
        y = votes_based_on_a_realization_of,
        by = c("vot_id", "doc_id") ) |>
    # BREAK THE CODE HERE AND TRACK DOWN NAs IN `DOCIDs `basedonarealizationof``
    dplyr::full_join(
        y = votes_inverse_consists_of,
        by = c("vot_id") ) |>
    # BREAK THE CODE HERE AND TRACK DOWN NAs IN `DOCIDs `processid`
    dplyr::full_join(
        y = procedures_cmt,
        by = c("process_id") ) |>
    dplyr::full_join(
        y = pl_docs_committee,
        by = c(
            "doc_id" = "doc_id",
             "committee_lab" = "cmt_lab",
            "based_on_a_realization_of" = "id"
            ) ) |>
    dplyr::distinct(rcv_id, committee_lab ) |>
    na.omit()
# Write data conditional on mandate ----------------------------------------#
if (mandate_starts == as.Date("2019-07-01")) {
    data.table::fwrite(x = rcvid_cmts, file = here::here(
        "data_out", "rcv", "rcvid_cmts_all.csv") )
} else {
    data.table::fwrite(x = rcvid_cmts, file = here::here(
        "data_out", "rcv", "rcvid_cmts_10.csv") ) }


#------------------------------------------------------------------------------#
## RCV ID and Doc IDs ----------------------------------------------------------
rcvid_docid <- rcvid_docid |>
    dplyr::left_join(
        y = rcv_inverse_consists_of,
        by = c("rcv_id" = "notation_votingId") ) |>
    dplyr::left_join(
        y = votes_based_on_a_realization_of,
        by = c("vot_id", "doc_id") ) |>
    dplyr::distinct(rcv_id, doc_id ) |>
    na.omit()
# Write data conditional on mandate ----------------------------------------#
if (mandate_starts == as.Date("2019-07-01")) {
    data.table::fwrite(x = rcvid_docid, file = here::here(
        "data_out", "rcv", "rcvid_docid_all.csv") )
} else {
    data.table::fwrite(x = rcvid_docid, file = here::here(
        "data_out", "rcv", "rcvid_docid_10.csv") ) }
