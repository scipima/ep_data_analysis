###--------------------------------------------------------------------------###
# EP MEETINGS Activities -------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' The script grabs the list of all Plenary Sessions Meetings Activities from the EP API.
#------------------------------------------------------------------------------#

# rm(list = ls())

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c(
  "data.table", "dplyr", "future.apply", "httr2", "here", "lubridate", "janitor",
  "jsonlite", "stringi", "tidyr", "tidyselect") )


#------------------------------------------------------------------------------#
## GET/meetings/{event-id} - Attendance ----------------------------------------
# Returns a single EP Plenary Session document for the specified doc ID
# EXAMPLE: https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-2023-07-12?format=application%2Fld%2Bjson

# Hard code the start of the mandate ------------------------------------------#
if ( !exists("mandate_starts") ) {
  mandate_starts <- as.Date("2024-07-14") }

# Load data conditional on mandate --------------------------------------------#
if ( exists("today_date") ) {
  meetings_plenary <- data.table::fread(
    file = here::here("data_out", "meetings", "meetings_plenary_10.csv"),
    key = "activity_date") |>
    dplyr::filter(activity_id == activity_id_today)
} else if ( !exists("today_date") && mandate_starts == as.Date("2019-07-01") ) {
  meetings_plenary <- data.table::fread(
    file = here::here("data_out", "meetings", "meetings_plenary_all.csv"),
    key = "activity_date")
} else if ( !exists("today_date") && mandate_starts == as.Date("2024-07-14") ) {
  meetings_plenary <- data.table::fread(
    file = here::here("data_out", "meetings", "meetings_plenary_10.csv"),
    key = "activity_date")
} else {
  stop("You need to specify a valid time period: today; or the starting day for an EP mandate (e.g. for the 10th mandate, '2024-07-14')")
}

# Get MEETINGS IDs
meetings_plenary_ids <- sort( unique(meetings_plenary$activity_id) )
# Split the vector in chunks of size 50 each
chunk_size <- 50L
meetings_ids_chunks <- split(
  x = meetings_plenary_ids, 
  f = ceiling(seq_along(meetings_plenary_ids) / chunk_size)
)

# loop to get all decisions ---------------------------------------------------#
list_tmp <- vector(mode = "list", length = length(meetings_ids_chunks) )
for ( i_param in seq_along(meetings_ids_chunks) ) {
  print(i_param)
  # Create an API request
  req <- httr2::request("https://data.europarl.europa.eu/api/v2") |>
    httr2::req_url_path_append("meetings") |>
    httr2::req_url_path_append(
      paste0(meetings_plenary_ids[[i_param]], collapse = ",")
      ) |>
    httr2::req_url_path_append("activities?format=application%2Fld%2Bjson&offset=0")
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
    list_tmp[[i_param]] <- resp_body
  } else if ( exists("today_date") ) {
    stop("API request failed. Data is not available yet. Please try again later.")
  }
  # remove objects
  rm(req, resp, resp_body) 
}


# append list
had_participant_person <- lapply(
  X = list_tmp, FUN = function(x) {
    if ("had_participant_person" %in% names( x ) ) {
      x |>
        dplyr::select(activity_id, had_participant_person) |>
        tidyr::unnest(had_participant_person) |>
        dplyr::mutate(pers_id = as.integer(
          gsub(pattern = "person/", replacement = "",
               x = had_participant_person) ),
          is_present = 1L) |>
        dplyr::select(-had_participant_person) } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)

# append list
had_excused_person <- lapply(
  X = list_tmp, FUN = function(x) {
    if ("had_excused_person" %in% names( x ) ) {
      x |>
        dplyr::select(activity_id, had_excused_person) |>
        tidyr::unnest(had_excused_person) |>
        dplyr::mutate(pers_id = as.integer(
          gsub(pattern = "person/", replacement = "",
               x = had_excused_person) ),
          is_present = 0L ) |>
        dplyr::select(-had_excused_person) } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)


# append list -----------------------------------------------------------------#
number_of_attendees <- lapply(
  X = list_tmp,
  FUN = function(x) {
    if ("number_of_attendees" %in% names( x ) ) {
      x |>
        dplyr::select(activity_id, number_of_attendees) } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)

if ( nrow(had_excused_person) > 0L | nrow(had_participant_person) > 0L) {
  attendance_dt <- dplyr::bind_rows(
    had_participant_person, had_excused_person) |>
    dplyr::mutate(activity_date = as.Date(
      gsub(pattern = "MTG-PL-", replacement = "", x = activity_id) ) ) |>
    dplyr::arrange(activity_date, pers_id)
}

if ( exists("attendance_dt") && nrow(number_of_attendees) > 0L ) {
  attendance_dt <- attendance_dt |>
    dplyr::full_join(
      y = number_of_attendees,
      by = "activity_id")|>
    dplyr::arrange(activity_date, pers_id)
}


## Save to disk conditional on mandate -----------------------------------------
if ( exists("attendance_dt") && exists("today_date") ) {
  data.table::fwrite(
    x = attendance_dt,
    file = here::here("data_out", "attendance", "attendance_dt_today.csv") )
} else if ( exists("attendance_dt")
            && !exists("today_date")
            && mandate_starts == as.Date("2019-07-01")) {
  data.table::fwrite(
    x = attendance_dt,
    file = here::here("data_out", "attendance", "attendance_dt_all.csv") )
} else if ( exists("attendance_dt")
            && !exists("today_date")
            && mandate_starts == as.Date("2024-07-14") ) {
  data.table::fwrite(
    x = attendance_dt,
    file = here::here("data_out", "attendance", "attendance_dt_10.csv") )
}


# append list
# consists_of <- lapply(
#   X = list_tmp, FUN = function(x) {
#     if ("consists_of" %in% names( x ) ) {
#       x |>
#         dplyr::select(activity_id, consists_of) |>
#         tidyr::unnest(consists_of, names_sep = "_") # |>
# tidyr::unnest(consists_of_recorded_in_a_realization_of) |>
# tidyr::unnest(consists_of_activity_label, names_sep = "_") |>
# dplyr::rename(consists_of_fr = consists_of_activity_label_fr,
#               consists_of_en = consists_of_activity_label_en) |>
# tidyr::unnest(consists_of_en) |>
# dplyr::select(
#     -dplyr::starts_with("consists_of_activity_label_"),
#     -c(consists_of_id, consists_of_type, consists_of_activity_date,
#        consists_of_had_activity_type, consists_of_inverse_consists_of,
#        consists_of_recorded_in_a_realization_of),
#     -any_of(
#         c("consists_of_executed", "consists_of_consists_of",
#           "consists_of_notation_dlvId") ) )
# } } ) |>
# data.table::rbindlist(use.names = TRUE, fill = TRUE)

# write data to disk ----------------------------------------------------------#
# data.table::fwrite(x = consists_of,
#                    file = here::here("data_out", "docs", "plenary_consists_of.csv") )

# Remove API objects ----------------------------------------------------------#
rm(had_excused_person, had_participant_person, i_param, list_tmp,
  meetings_plenary_ids, number_of_attendees)
