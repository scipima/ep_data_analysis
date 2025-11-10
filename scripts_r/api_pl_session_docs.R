###--------------------------------------------------------------------------###
# EP Plenary Session Documents -------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' The script grabs all Plenary Sessions Documents from the EP API.
#' REF: https://data.europarl.europa.eu/en/home; https://data.europarl.europa.eu/en/developer-corner/opendata-api
#------------------------------------------------------------------------------#

# Clean ENV -------------------------------------------------------------------#
# rm(list = ls())
script_starts <- Sys.time()

# Hard code the start of the mandate ------------------------------------------#
if ( !exists("mandate_starts") ) {
    mandate_starts = "2024-07-14" }


#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
packages <- c("data.table", "dplyr", "future.apply", "here", "httr2", "janitor",
              "jsonlite", "tidyr", "xml2")
if ( any( !packages %in% pacman::p_loaded() ) ) {
    pacman::p_load(char = packages[ !packages %in% pacman::p_loaded() ] ) }


#------------------------------------------------------------------------------#
## GET/plenary-session-documents -----------------------------------------------
# Returns the list of all EP Plenary Session documents

#------------------------------------------------------------------------------#
#' Plenary Part-Session Date.
#' This parameter refers to the date of the first day of a given part-session.
#' Parliament sits monthly in Strasbourg in a four-day part-session (Monday to Thursday).
#' Additional part-sessions are held in Brussels.
#' The format of the value is YYYY-MM-DD.
#------------------------------------------------------------------------------#

req = httr2::request("https://data.europarl.europa.eu/api/v2/plenary-session-documents?work-type=LIST_ATTEND_PLENARY,VOTE_RESULTS_PLENARY,VOTE_ROLLCALL_PLENARY&format=application%2Fld%2Bjson&offset=0") |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
    httr2::req_perform() |>
    httr2::resp_body_json(simplifyDataFrame = TRUE)

pl_session_docs = data.table::as.data.table( req$data )
pl_session_docs[, `:=`(
    pl_id = stringi::stri_replace(
        str = identifier,
        regex = "-ATT|-RCV|-VOT",
        replacement = ""),
    document_date = data.table::as.IDate( stringi::stri_replace(
        str = identifier,
        regex = "PV-\\d{1,2}-|-ATT|-RCV|-VOT",
        replacement = "")
    ) )
]
# Check
if (sum(sapply(X = pl_session_docs, FUN = function(x) sum(is.na(x))) > 0L) ) {
    warning("Somehting is off in the Plenary Session Documents - Check it again!") }

# Filter and sort
pl_session_docs = pl_session_docs[
    order(document_date)
][
    document_date >= mandate_starts
]

# Save to disk conditional on mandate -----------------------------------------#
if (mandate_starts == as.Date("2019-07-01")) {
    data.table::fwrite(
        x = pl_session_docs[, list(id, work_type, identifier, label)],
        file = here::here("data_out", "docs_pl", "pl_session_docs_all.csv") )
} else {
    data.table::fwrite(
        x = pl_session_docs[, list(id, work_type, identifier, label)],
        file = here::here("data_out", "docs_pl", "pl_session_docs_10.csv") )
}


#------------------------------------------------------------------------------#
# Clean up before exiting -----------------------------------------------------#
# Remove objects
rm(req)
# Run time
script_ends <- Sys.time()
script_lapsed = script_ends - script_starts
cat("\nThe script run in:", script_lapsed, "\n")
