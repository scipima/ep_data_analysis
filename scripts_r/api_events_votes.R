###--------------------------------------------------------------------------###
# EP Votes API -------------------------------------------------------------
###--------------------------------------------------------------------------###

# rm(list = ls())

#' This scripts checks the list of votes during the mandate.
#' Its aim is to check whether the list of votes we get is complete or not.

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
library(httr2)
library(here)
library(jsonlite)
library(future.apply)
library(tidyverse)
library(data.table)
library(googledrive)

# store authorisation ---------------------------------------------------------#
googledrive::drive_auth(email = "marco.scipioni05@gmail.com")


#------------------------------------------------------------------------------#
## GET/events - PLENARY_VOTE ---------------------------------------------------
# The service returns the list of all EP Events. A type of an Activity. Values are concepts of the EP Vocabulary 'ep-activities'.
# EXAMPLE: https://data.europarl.europa.eu/api/v2/events?activity-type=PLENARY_VOTE&format=application%2Fld%2Bjson&offset=0&limit=20

# PLENARY_VOTE
req <- httr2::request(base_url = "https://data.europarl.europa.eu/api/v2/events?activity-type=PLENARY_VOTE&format=application%2Fld%2Bjson&offset=0")
# Add time-out and ignore error
resp <- req |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
    httr2::req_error(is_error = ~FALSE) |> # handle errors silently
    httr2::req_throttle(30 / 60) |> # call politely
    httr2::req_perform()
# If not an error, download and make available in ENV
if ( httr2::resp_status(resp) == 200L ) {
    resp_body <- resp |>
        httr2::resp_body_json(simplifyDataFrame = TRUE)
    plenary_votes <- resp_body$data
    rm(req, resp, resp_body) }


#------------------------------------------------------------------------------#
## GET/events - PLENARY_VOTE_RESULTS -------------------------------------------
# The service returns the list of all EP Events. A type of an Activity. Values are concepts of the EP Vocabulary 'ep-activities'.
# EXAMPLE: https://data.europarl.europa.eu/api/v2/events?activity-type=PLENARY_VOTE_RESULTS&format=application%2Fld%2Bjson&offset=0&limit=20

# PLENARY_VOTE_RESULTS
req <- httr2::request(base_url = "https://data.europarl.europa.eu/api/v2/events?activity-type=PLENARY_VOTE_RESULTS&format=application%2Fld%2Bjson&offset=0" )
# Add time-out and ignore error
resp <- req |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
    httr2::req_error(is_error = ~FALSE) |> # handle errors silently
    httr2::req_throttle(30 / 60) |> # call politely
    httr2::req_perform()
# If not an error, download and make available in ENV
if ( httr2::resp_status(resp) == 200L ) {
    resp_body <- resp |>
        httr2::resp_body_json(simplifyDataFrame = TRUE)
    plenary_vote_results <- resp_body$data
    rm(req, resp, resp_body) }


#------------------------------------------------------------------------------#
## GET/events/{event-id} - PLENARY_VOTE-----------------------------------------
# Returns a single EP Events for the specified event ID

# EXAMPLE: https://data.europarl.europa.eu/api/v2/events/MTG-PL-2019-07-15-VOT-ITM-000001?format=application%2Fld%2Bjson&json-layout=framed

# get procedures IDs
event_vote_ids <- sort( unique(plenary_votes$activity_id) )

# loop to get all decisions
list_tmp <- vector(mode = "list", length = length(event_vote_ids) )
for ( i_param in seq_along(event_vote_ids) ) {
    print(event_vote_ids[i_param])
    # Create an API request
    req <- httr2::request("https://data.europarl.europa.eu/api/v2") |>
        httr2::req_url_path_append("events") |>
        httr2::req_url_path_append(event_vote_ids[i_param]) |>
        httr2::req_url_path_append("?format=application%2Fld%2Bjson&json-layout=framed")
    # Add time-out and ignore error
    resp <- req |>
        httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
        httr2::req_error(is_error = ~FALSE) |>
        httr2::req_throttle(30 / 60) |>
        httr2::req_perform()
    # If not an error, download and make available in ENV
    if ( httr2::resp_status(resp) == 200L) {
        resp_body <- resp |>
            httr2::resp_body_json(simplifyDataFrame = TRUE)
        list_tmp[[i_param]] <- resp_body$data }
    rm(req, resp, resp_body) }

# append list
events_vote_dt <- data.table::rbindlist(l = list_tmp, use.names = TRUE, fill = TRUE)
# write to disk
data.table::fwrite(
    x = events_vote_dt,
    file = here::here("data_out", "votes", "events_vote.csv") )


#------------------------------------------------------------------------------#
## GET/events/{event-id} - PLENARY_VOTE_RESULTS --------------------------------
# Returns a single EP Events for the specified event ID

# EXAMPLE: https://data.europarl.europa.eu/api/v2/events/MTG-PL-2019-07-15-VOT-ITM-000001?format=application%2Fld%2Bjson&json-layout=framed

# get procedures IDs
event_vote_result_ids <- sort( unique(plenary_vote_results$activity_id) )

# loop to get all decisions
list_tmp <- vector(mode = "list", length = length(event_vote_result_ids) )
for ( i_param in seq_along(event_vote_result_ids) ) {
    print(event_vote_result_ids[i_param])
    # Create an API request
    req <- httr2::request("https://data.europarl.europa.eu/api/v2") |>
        httr2::req_url_path_append("events") |>
        httr2::req_url_path_append(event_vote_result_ids[i_param]) |>
        httr2::req_url_path_append("?format=application%2Fld%2Bjson&json-layout=framed")
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
        resp_data <- resp_body |>
            dplyr::select(-dplyr::starts_with("structuredLabel") ,
                          -any_of(c("activity_label", "comment") ) )
        if ( "activity_label" %in% names(resp_body) ) {
            resp_data <- resp_body |>
                tidyr::unnest(activity_label, names_sep = "_") |>
                dplyr::select(dplyr::any_of(
                    c("activity_label_en", "activity_label_fr",
                      "activity_label_mul") ) ) |>
                dplyr::bind_cols(resp_data) }
        if ( "comment" %in% names(resp_body) ) {
            resp_data <- resp_body |>
                dplyr::select(comment) |>
                tidyr::unnest(comment, names_sep = "_") |>
                dplyr::select(dplyr::any_of(c("comment_en", "comment_fr") ) ) |>
                dplyr::bind_cols(resp_data) }
        # store data in list
        list_tmp[[i_param]] <- resp_data
    }
    rm(req, resp, resp_body) }

# append list
event_vote_result_dt <- data.table::rbindlist(
    l = list_tmp, use.names = TRUE, fill = TRUE)
# write to disk
data.table::fwrite(
    x = event_vote_result_dt,
    file = here::here("data_out", "votes", "events_vote_result.csv") )


#------------------------------------------------------------------------------#
## GET/meetings/{event-id}/vote-results ----------------------------------------
# Returns a single EP Events for the specified event ID

# EXAMPLE: https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-2024-04-23/vote-results?format=application%2Fld%2Bjson&offset=0&limit=50


meetings_plenary <- data.table::fread(file = here::here(
    "data_out", "meetings_plenary.csv") )

# get procedures IDs
meetings_plenary_ids <- sort( unique(meetings_plenary$activity_id) )

# loop to get all decisions
list_tmp <- vector(mode = "list", length = length(meetings_plenary_ids) )
for ( i_param in seq_along(meetings_plenary_ids) ) {
    print(meetings_plenary_ids[i_param])
    # Create an API request
    req <- httr2::request("https://data.europarl.europa.eu/api/v2") |>
        httr2::req_url_path_append("meetings") |>
        httr2::req_url_path_append(meetings_plenary_ids[i_param]) |>
        httr2::req_url_path_append("vote-results?format=application%2Fld%2Bjson&offset=0")
    # Add time-out and ignore error
    resp <- req |>
        httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
        httr2::req_error(is_error = ~FALSE) |>
        httr2::req_throttle(30 / 60) |>
        httr2::req_perform()
    # If not an error, download and make available in ENV
    if ( httr2::resp_status(resp) == 200L) {
        resp_body <- resp |>
            httr2::resp_body_json(simplifyDataFrame = TRUE)
        list_tmp[[i_param]] <- resp_body$data } }

# append list
meeting_event_vote_result <- data.table::rbindlist(
    l = list_tmp, use.names = TRUE, fill = TRUE)
# write to disk
# data.table::fwrite(
#     x = meeting_event_vote_result,
#     file = here::here("data_out", "votes", "meeting_event_vote_result.csv") )
