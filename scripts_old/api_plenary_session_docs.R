###--------------------------------------------------------------------------###
# EP Plenary Session Documents -------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' The script grabs all Plenary Sessions Documents from the EP API.
#' REF: https://data.europarl.europa.eu/en/home; https://data.europarl.europa.eu/en/developer-corner/opendata-api
#------------------------------------------------------------------------------#


# Clean ENV -------------------------------------------------------------------#
# rm(list = ls())
# Hard code the start of the mandate ------------------------------------------#
# mandate_starts <- as.Date("2024-07-14")


###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
packages <- c("data.table", "dplyr", "future.apply", "here", "httr2", "janitor",
              "jsonlite", "tidyr", "xml2")
if ( any( !packages %in% pacman::p_loaded() ) ) {
  pacman::p_load(char = packages[ !packages %in% pacman::p_loaded() ] ) }


###--------------------------------------------------------------------------###
## GET/events - PLENARY_PART_SESSION -------------------------------------------
# Returns the list of all EP Events

#' Plenary Part-Session Date.
#' This parameter refers to the date of the first day of a given part-session.
#' Parliament sits monthly in Strasbourg in a four-day part-session (Monday to Thursday).
#' Additional part-sessions are held in Brussels.
#' The format of the value is YYYY-MM-DD.

# EXAMPLE: https://data.europarl.europa.eu/api/v2/events?activity-type=PLENARY_PART_SESSION&format=application%2Fld%2Bjson&offset=0&limit=50

req <- httr2::request("https://data.europarl.europa.eu/api/v2/events?activity-type=PLENARY_PART_SESSION&format=application%2Fld%2Bjson&offset=0") |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
  httr2::req_perform() |>
  httr2::resp_body_json(simplifyDataFrame = TRUE)

plenary_part_session <- req$data |>
  dplyr::mutate(date = as.Date(
    stringi::stri_replace_first_fixed(
      str = activity_id,
      pattern = "GMTG-PL-",
      replacement = "")
    ) ) |>
  dplyr::select(date, activity_id) |>
  dplyr::filter(date >= mandate_starts) |>
  dplyr::arrange(date)

# remove objects
rm(req)


###--------------------------------------------------------------------------###
## GET/plenary-session-documents -----------------------------------------------
# Returns the list of all EP Plenary Session documents

# EXAMPLE: https://data.europarl.europa.eu/api/v2/plenary-session-documents?part-session-date=2022-09-12&work-type=LIST_ATTEND_PLENARY&format=application%2Fld%2Bjson&offset=0&limit=50

plenary_part_session_dates <- sort(unique(plenary_part_session$date))

# call
req <- httr2::request(
  paste0(
    "https://data.europarl.europa.eu/api/v2/plenary-session-documents?part-session-date=",
    paste0(plenary_part_session_dates, collapse = ","),
    "&work-type=LIST_ATTEND_PLENARY,VOTE_RESULT_PLENARY,VOTE_ROLLCALL_PLENARY&format=application%2Fld%2Bjson&offset=0") ) |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
  httr2::req_error(is_error = ~FALSE) |>
  httr2::req_perform()

# extract data
if ( httr2::resp_status(req) == 200L ) {
  plenary_session_docs <- req |>
    httr2::resp_body_json( simplifyDataFrame = TRUE ) |>
    purrr::pluck("data") }

# Save to disk conditional on mandate -----------------------------------------#
if (mandate_starts == as.Date("2019-07-01")) {
  data.table::fwrite(x = plenary_session_docs, file = here::here(
    "data_out", "docs_pl", "plenary_session_docs_all.csv") )
} else {
  data.table::fwrite(x = plenary_session_docs, file = here::here(
    "data_out", "docs_pl", "plenary_session_docs_10.csv") ) }


#------------------------------------------------------------------------------#
## GET/plenary-session-documents/{doc-id} - RCV --------------------------------
# Returns a single EP Plenary Session document for the specified doc ID

# EXAMPLE: https://data.europarl.europa.eu/api/v2/plenary-session-documents/PV-9-2021-02-10-RCV?format=application%2Fld%2Bjson&language=fr

# get RCV ids
rcv_ids <- sort( unique( plenary_session_docs$identifier[
  plenary_session_docs$work_type == "def/ep-document-types/VOTE_ROLLCALL_PLENARY"] ) )

# loop to get all decisions
list_tmp <- vector(mode = "list", length = length(rcv_ids) )
for ( i_param in seq_along(rcv_ids) ) {
  print(rcv_ids[i_param])
  # Create an API request
  req <- httr2::request("https://data.europarl.europa.eu/api/v2") |>
    httr2::req_url_path_append("plenary-session-documents") |>
    httr2::req_url_path_append(rcv_ids[i_param]) |>
    httr2::req_url_path_append("?format=application%2Fld%2Bjson&language=fr")
  # Add time-out and ignore error
  resp <- req |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
    httr2::req_error(is_error = ~FALSE) |>
    httr2::req_throttle(30 / 60) |>
    httr2::req_perform()
  # If not an error, download and make available in ENV
  if ( httr2::resp_status(resp) == 200L) {
    resp_body <- resp |>
      httr2::resp_body_json(simplifyDataFrame = TRUE) |>
      purrr::pluck("data")
    if ("title_dcterms" %in% names(resp_body) ) {
      resp_body <- resp_body |>
        tidyr::unnest(title_dcterms, names_sep = "_") }
    list_tmp[[i_param]] <- resp_body
  }

  # remove objects
  rm(req, resp, resp_body)
}

# append list
plenary_session_docs_rcv <- data.table::rbindlist(l = list_tmp,
                                                  use.names = TRUE, fill = TRUE) |>
  dplyr::select(identifier, parliamentary_term, document_date, title_dcterms_fr,
                label, epNumber, inverse_is_part_of) |>
  tidyr::unnest(inverse_is_part_of, names_sep = "_") |>
  tidyr::unnest(inverse_is_part_of_inverse_recorded_in_a_realization_of) |>
  tidyr::unnest(inverse_is_part_of_title_dcterms, names_sep = "_") |>
  tidyr::unnest(recorded_in_a_realization_of) |>
  dplyr::select(-c(inverse_is_part_of_id, inverse_is_part_of_type_subdivision,
                   inverse_is_part_of_is_part_of, inverse_is_part_of_numbering,
                   inverse_is_part_of_type) )
# sapply(plenary_session_docs_rcv, function(x) sum(is.na(x))) # check NAs


# Save to disk conditional on mandate -----------------------------------------#
if (mandate_starts == as.Date("2019-07-01")) {
  data.table::fwrite(x = plenary_session_docs_rcv, file = here::here(
    "data_out", "rcv", "plenary_session_docs_rcv_all.csv"))
} else {
  data.table::fwrite(x = plenary_session_docs_rcv, file = here::here(
    "data_out", "rcv", "plenary_session_docs_rcv_10.csv")) }


#------------------------------------------------------------------------------#
# Get list of final votes
source(file = here::here("scripts_r", "get_final_votes.R"))
