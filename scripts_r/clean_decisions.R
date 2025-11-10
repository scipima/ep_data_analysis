###--------------------------------------------------------------------------###
# Process Vote Files -----------------------------------------------------------
###--------------------------------------------------------------------------###

script_starts <- Sys.time()

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c(
    "data.table", "dplyr", "here", "lubridate", "janitor", "jsonlite", "readr",
    "stringi", "tidyr", "tidyselect") )


#------------------------------------------------------------------------------#
## Parameters ------------------------------------------------------------------
# Hard code the start of the mandate ------------------------------------------#
if ( !exists("mandate_starts") ) {
  mandate_starts <- "2024-07-14"
}

#------------------------------------------------------------------------------#
### Load temporary file --------------------------------------------------------
# Determine which temporary RDS to use (today / 10 / all) and ensure it exists
# get_api_data() will source the generator if the target file is missing or stale
if ( exists("today_date") && mandate_starts == "2024-07-14" ) {
    resp_list <- get_api_data(
        path = here::here("data_out", "rcv", "rcv_tmp_today.RDS"),
        script = here::here("scripts_r", "api_meetings_decisions.R"),
        max_days = 1,
        file_type = "rds",
        varname = NULL,
        envir = .GlobalEnv
    )
} else if ( !exists("today_date") && mandate_starts == "2024-07-14" ) {
    resp_list <- get_api_data(
        path = here::here("data_out", "rcv", "rcv_tmp_10.RDS"),
        script = here::here("scripts_r", "api_meetings_decisions.R"),
        max_days = 1,
        file_type = "rds",
        varname = NULL,
        envir = .GlobalEnv
    )
} else if ( !exists("today_date") && mandate_starts == "2019-07-01" ) {
    resp_list <- get_api_data(
        path = here::here("data_out", "rcv", "rcv_tmp_all.RDS"),
        script = here::here("scripts_r", "api_meetings_decisions.R"),
        max_days = 1,
        file_type = "rds",
        varname = NULL,
        envir = .GlobalEnv
    )
} else {
    stop("Something is off - The data collection for RCVs has no output.")
}


#------------------------------------------------------------------------------#
#' The first 3 functions clean, respectively: the Decisions metadata, RCVs, Intentions.
#' The `process_decisions_list_longdf.R` cleans the DF-cols in the data.

source(file = here::here("scripts_r", "process_decisions_session_metadata.R"),
       local = FALSE)
source(file = here::here("scripts_r", "process_decisions_session_rcv.R"),
       local = FALSE)
source(file = here::here("scripts_r", "process_decisions_session_intentions.R"),
       local = FALSE)
source(file = here::here("scripts_r", "process_decisions_list_longdf.R"),
       local = FALSE)


#------------------------------------------------------------------------------#
### Append & Clean Votes and RCV DFs -------------------------------------------
# append & clean votes --------------------------------------------------------#
pl_votes <- lapply(X = resp_list,
                   FUN = function(x) process_decisions_session_metadata(x) ) |>
data.table::rbindlist(use.names = TRUE, fill = TRUE)
# sapply(pl_votes, function(x) sum(is.na(x))) # check


#------------------------------------------------------------------------------#
### Doc_ID ---------------------------------------------------------------------
# quick and dirty way to get doc_id out of labels
if ( "activity_label_fr" %in% names(pl_votes) ) {
    tmp_docid_activity_fr = stringi::stri_extract(
        str = pl_votes$activity_label_fr,
        regex = "[A-Z]\\d{1,2}.\\d{4}.\\d{4}|[A-Z]{2}.[A-Z]\\d{1,2}.\\d{4}.\\d{4}") }
if ( "activity_label_en" %in% names(pl_votes) ) {
    tmp_docid_activity_en = stringi::stri_extract(
        str = pl_votes$activity_label_en,
        regex = "[A-Z]\\d{1,2}.\\d{4}.\\d{4}|[A-Z]{2}.[A-Z]\\d{1,2}.\\d{4}.\\d{4}") }
if ( "activity_label_mul" %in% names(pl_votes)) {
    tmp_docid_activity_mul = stringi::stri_extract(
        str = pl_votes$activity_label_mul,
        regex = "[A-Z]\\d{1,2}.\\d{4}.\\d{4}|[A-Z]{2}.[A-Z]\\d{1,2}.\\d{4}.\\d{4}") }
# If the former fails, it can be the case that the string pops up in another col
if ("headingLabel_en" %in% names(pl_votes) ) {
    tmp_docid_head_en = stringi::stri_extract(
        str = pl_votes$headingLabel_en,
        regex = "[A-Z]\\d{1,2}.\\d{4}.\\d{4}|[A-Z]{2}.[A-Z]\\d{1,2}.\\d{4}.\\d{4}") }
if ("headingLabel_fr" %in% names(pl_votes) ) {
    tmp_docid_head_fr = stringi::stri_extract(
        str = pl_votes$headingLabel_fr,
        regex = "[A-Z]\\d{1,2}.\\d{4}.\\d{4}|[A-Z]{2}.[A-Z]\\d{1,2}.\\d{4}.\\d{4}") }

# Put all these vectors in a list
docid_tmp_cols = mget(ls(pattern = "tmp_docid_"))
# Grab the first non-NA value
pl_votes[, doc_id := data.table::fcoalesce(docid_tmp_cols)]

# Unfortunately there seems to be a data entry issue with the hyphens ---------#
# First Reports & Resolutions ("A*" and "B*" files)
pl_votes[, doc_id := gsub(pattern = "(^[A-Z]{1}\\d{1,2})(.)(\\d{4}\\/\\d{4})",
                          replacement = "\\1-\\3", x = doc_id) ]
# Then Joint Resolutions ("RC-B*" files)
pl_votes[, doc_id := gsub(pattern = "(^[A-Z]{2})(.)([A-Z]{1,2}\\d{1,2})(.)(\\d{4}\\/\\d{4})",
                          replacement = "\\1-\\3-\\5", x = doc_id) ]

# rename col
data.table::setnames(x = pl_votes, old = c("notation_votingId"), new = c("rcv_id"))


#------------------------------------------------------------------------------#
# append & clean rcv ----------------------------------------------------------#
pl_rcv <- lapply(
  X = resp_list, 
  FUN = function(x) process_decisions_session_rcv (x) ) |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE)
# rename & drop cols
pl_rcv[, c("id") := NULL]
data.table::setnames(x = pl_rcv, old = c("notation_votingId"), new = c("rcv_id"))
# sapply(pl_rcv, function(x) sum(is.na(x))) # check

# Add identifier for mandate --------------------------------------------------#
pl_votes[, mandate := data.table::fifelse(
    test = activity_date >= data.table::as.IDate("2024-07-14"),
    yes = 10L, no = 9L) ]


#------------------------------------------------------------------------------#
## Save to disk conditional on mandate -----------------------------------------
if ( exists("today_date")
     && mandate_starts == as.character("2024-07-14") ) {
    # Today -------------------------------------------------------------------#
    # Votes data.frame
    data.table::fwrite(x = pl_votes,
                       file = here::here("data_out", "votes", "pl_votes_today.csv") )

    # RCVs
    data.table::fwrite(x = unique(pl_rcv[
        !is.na(result), list(pers_id, rcv_id, result, activity_date)]),
        file = here::here("data_out", "rcv", "pl_rcv_today.csv") )

} else if ( !exists("today_date")
            && mandate_starts == as.character("2024-07-14") ) {
    # Current Mandate ---------------------------------------------------------#
    # Votes data.frame
    data.table::fwrite(x = pl_votes,
                       file = here::here("data_out", "votes", "pl_votes_10.csv") )

    # List of Doc IDs by RCVs
    data.table::fwrite(x = unique(pl_votes[
        grepl(pattern = "VOTE_ELECTRONIC_ROLLCALL", x = decision_method)
        & !decision_outcome %in% c("LAPSED", "WITHDRAWN"),
        list(rcv_id, doc_id)]),
        file = here::here("data_out", "rcv", "rcvid_docid_10.csv") )

    # RCVs
    data.table::fwrite(x = unique(pl_rcv[
        !is.na(result), list(pers_id, rcv_id, result, activity_date)]),
        file = here::here("data_out", "rcv", "pl_rcv_10.csv") )

} else if ( !exists("today_date")
            && mandate_starts == as.character("2019-07-01") ) {
    # All Mandates ------------------------------------------------------------#
    # Votes data.frame
    data.table::fwrite(x = pl_votes,
                       file = here::here("data_out", "votes", "pl_votes_all.csv") )

    # List of Doc IDs by RCVs
    data.table::fwrite(x = unique(pl_votes[
        grepl(pattern = "VOTE_ELECTRONIC_ROLLCALL", x = decision_method)
        & !decision_outcome %in% c("LAPSED", "WITHDRAWN"),
        list(rcv_id, doc_id)]),
        file = here::here("data_out", "rcv", "rcvid_docid_all.csv") )

    # RCVs
    data.table::fwrite(x = unique(pl_rcv[
        !is.na(result), list(pers_id, rcv_id, result, activity_date)]),
        file = here::here("data_out", "rcv", "pl_rcv_all.csv") )
}


#------------------------------------------------------------------------------#
### Convert list-cols ----------------------------------------------------------
# Decided_on_a_realization_of -------------------------------------------------#
decided_on_a_realization_of <- lapply(
    X = resp_list,
    FUN = function(i_df) {
        if ("decided_on_a_realization_of" %in% names(i_df)) {
            i_df[, c("notation_votingId", "decided_on_a_realization_of")] |>
                tidyr::unnest(decided_on_a_realization_of) } } ) |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE)

# Conditional execution based on whether the data is actually there
if ( nrow(decided_on_a_realization_of) > 0L ) {

    # Clean DOC_ID col --------------------------------------------------------#
    decided_on_a_realization_of[, identifier := gsub(
        pattern = "eli/dl/doc/|-AM-.*|-SPLIT.*", replacement = "",
        x = decided_on_a_realization_of)]
    decided_on_a_realization_of[, type := ifelse(
        test = grepl(pattern = "[A-Z]{1,2}.\\d{1,2}.\\d{4}.\\d{4}",
                     x = identifier),
        yes = "doc", no = "vote_id"
    )]

    # Fix docs' labels --------------------------------------------------------#
    # create temporary duplicate col for string processing
    decided_on_a_realization_of[, identifier2 := ifelse(
        test = type == "doc", yes = identifier, no = NA)]
    # invert the orders of the groups for A-, B-, C- files
    decided_on_a_realization_of[
        grepl(pattern = "^[ABC]{1}.\\d{1,2}.", x = identifier2),
        doc_id := gsub(
            pattern = "(^[ABC]{1}.\\d{1,2}.)(\\d{4}).(\\d{4})",
            replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
    # treat RC separately
    decided_on_a_realization_of[
        grepl(pattern = "^RC.\\d{1,2}", x = identifier2),
        doc_id := gsub(
            pattern = "(^RC.\\d{1,2}.)(\\d{4}).(\\d{4})",
            replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
    decided_on_a_realization_of[, doc_id := gsub(pattern = "^RC", replacement = "RC-B",
                                                 x = doc_id, perl = T)]
    # delete - between doc_id LETTER and MANDATE NUMBER
    decided_on_a_realization_of[, doc_id := gsub(pattern = "([A-Z]{1}).(\\d{1,2})",
                                                 replacement = "\\1\\2", x = doc_id, perl = T)]
    # slash at the end
    decided_on_a_realization_of[, doc_id := gsub(pattern = "(?<=.\\d{4}).",
                                                 replacement = "/", x = doc_id, perl = T)]
    # Delete cols
    decided_on_a_realization_of[, c("identifier", "identifier2") := NULL]

    # unique(decided_on_a_realization_of[
    #     type == "doc",
    #     list(notation_votingId, doc_id)
    #     ])[, .N, by = notation_votingId][order(N)]
}

# Write data conditional on mandate -------------------------------------------#
if ( !exists("today_date") && mandate_starts == as.character("2019-07-01")) {
    data.table::fwrite(x = decided_on_a_realization_of, file = here::here(
        "data_out", "rcv", "rcv_decided_on_a_realization_of_all.csv") )
} else if ( !exists("today_date") && mandate_starts == as.character("2024-07-14") ) {
    data.table::fwrite(x = decided_on_a_realization_of, file = here::here(
        "data_out", "rcv", "rcv_decided_on_a_realization_of_10.csv") ) }


# inverse_consists_of ---------------------------------------------------------#
#' This connects the DEC-ID with the VOT-ID
inverse_consists_of <- lapply(
    X = resp_list,
    FUN = function(i_df) {
        if ("inverse_consists_of" %in% names(i_df)) {
            i_df[, c("notation_votingId", "inverse_consists_of")] |>
                tidyr::unnest(inverse_consists_of) } } ) |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE)
inverse_consists_of[, vote_id := gsub(pattern = "eli/dl/event/", replacement = "",
                                      x = inverse_consists_of, fixed = TRUE)]
inverse_consists_of[, inverse_consists_of := NULL]
# Write data conditional on mandate -------------------------------------------#
if ( !exists("today_date") && mandate_starts == as.character("2019-07-01")) {
    data.table::fwrite(x = inverse_consists_of, file = here::here(
        "data_out", "rcv", "rcv_inverse_consists_of_all.csv") )
} else if ( !exists("today_date") && mandate_starts == as.character("2024-07-14") ) {
    data.table::fwrite(x = inverse_consists_of, file = here::here(
        "data_out", "rcv", "rcv_inverse_consists_of_10.csv") ) }


##----------------------------------------------------------------------------##
# Clean up before exiting -----------------------------------------------------#

# Message on exit -------------------------------------------------------------#
# if ( !exists("today_date") && all(pl_session_docs_rcv$activity_id %in% unique(
#     gsub(pattern = "-DEC-.*$", replacement = "",
#          x = pl_votes$activity_id, perl=TRUE) ) ) ) {
#     print("All Meetings have been processed")
# } else {
#     warning("You might have missed some Meetings - Please revise.")
# }

# Run time
# script_ends <- Sys.time()
# script_lapsed = script_ends - script_starts
# cat("\nThe script run in:", script_lapsed, "\n")

# Remove objects
rm(decided_on_a_realization_of, docid_tmp_cols, inverse_consists_of, pl_rcv,
   pl_votes, resp_list, script_ends, script_lapsed, script_starts)
rm(list = ls(pattern = "tmp_docid_"))
