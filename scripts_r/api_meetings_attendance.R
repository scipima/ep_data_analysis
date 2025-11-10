###--------------------------------------------------------------------------###
# EP Attendance Lists ----------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' The script grabs the list of all Plenary Sessions Meetings from the EP API.
#' It also collects the `plenary-session-documents/{doc-id}`, which contain the `attendance lists` for the RCV during such `meetings`.
#------------------------------------------------------------------------------#

# rm(list = ls())
script_starts <- Sys.time()
cat("\n=====\nStarting to collect Attendance Lists.\n=====\n")


#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c(
    "data.table", "dplyr", "httr2", "here", "lubridate", "janitor", "jsonlite",
    "stringi", "tidyr", "tidyselect") )


#------------------------------------------------------------------------------#
## GET/meetings/{event-id} - Attendance ----------------------------------------
# Returns a single EP Plenary Session document for the specified doc ID
# EXAMPLE: https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-2023-07-12?format=application%2Fld%2Bjson


# Load data conditional on mandate --------------------------------------------#
if ( exists("today_date") ) {
    pl_meetings <- data.table::fread(
        file = here::here("data_out", "meetings", "pl_meetings_10.csv") ) |>
        dplyr::filter(activity_id == activity_id_today)
} else if ( !exists("today_date")
            && mandate_starts == as.character("2019-07-01") ) {
    pl_meetings <- data.table::fread(
        file = here::here("data_out", "meetings", "pl_meetings_all.csv"))
} else if ( !exists("today_date")
            && mandate_starts == as.character("2024-07-14") ) {
    pl_meetings <- data.table::fread(
        file = here::here("data_out", "meetings", "pl_meetings_10.csv") )
} else {
    stop("You need to specify a valid time period: today; or the starting day for an EP mandate (e.g. for the 10th mandate, '2024-07-14')")
}

# Get MEETINGS IDs
meet_pl_ids <- sort( unique(pl_meetings$activity_id) )
# Split the vector in chunks of size 50 each
chunk_size <- 50L
meet_pl_ids_chunks <- split(
    x = meet_pl_ids, f = ceiling(seq_along(meet_pl_ids) / chunk_size)
)

# loop to get all decisions ---------------------------------------------------#
list_tmp <- vector(mode = "list", length = length(meet_pl_ids_chunks) )
for ( i_chunk in seq_along(list_tmp) ) {
    print(i_chunk)
    # Create an API request
    req <- httr2::request(
        paste0(
            "https://data.europarl.europa.eu/api/v2/meetings/",
            paste0(meet_pl_ids_chunks[[i_chunk]], collapse = ","),
            "?format=application%2Fld%2Bjson")
    )
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
        list_tmp[[i_chunk]] <- resp_body
    } else if ( exists("today_date")
                && httr2::resp_status(resp) != 200L ) {
        stop("API request failed. Data is not available yet. Try again later.")
    }
    # remove objects
    rm(req, resp, resp_body)
}

#------------------------------------------------------------------------------#
#### Check for missing Meeting chunks from API --------------------------------
if ( sum(sapply(X = list_tmp, FUN = is.null)) > 0L ){
  # Get the missing chunk indices
  missing_chunk_indices <- which(sapply(X = list_tmp, FUN = is.null))
  missing_chunks <- meet_pl_ids_chunks[missing_chunk_indices]
  
  # Convert chunks back to individual meeting IDs for clearer error message
  missing_meeting_ids <- unlist(missing_chunks)
  
  if (exists("today_date")) {
    # For daily runs, issue a warning but continue execution
    cat(
      paste0(
        "\n=====\nWARNING: Daily run - meeting returned empty data:\n",
        "Meeting ID: ", paste0(missing_meeting_ids, collapse = ", "), "\n",
        "Continuing execution with available data...\n=====\n"
      )
    )

  } else {
    # For non-daily runs, stop execution
    stop(
      paste0(
        "\n=====\nERROR: These meeting chunks returned empty data:\n",
        "Chunks: ", paste0(missing_chunk_indices, collapse = ", "), "\n", 
        "Meeting IDs: ", paste0(missing_meeting_ids, collapse = ", "), "\n\n",
        "Execution stopped. Please investigate why these meetings failed to return data.\n=====\n"
      )
    )
  }
}


### Participant person ---------------------------------------------------------
had_participant_person <- lapply(
    X = list_tmp, FUN = function(x) {
        if ("had_participant_person" %in% names( x ) ) {
            x |>
                dplyr::select(id, had_participant_person) |>
                tidyr::unnest(had_participant_person) |>
                dplyr::mutate(pers_id = as.integer(
                    gsub(pattern = "person/", replacement = "",
                         x = had_participant_person) ),
                    is_present = 1L) |>
                dplyr::select(-had_participant_person)
        } } ) |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE)


### Excused person -------------------------------------------------------------
had_excused_person <- lapply(
    X = list_tmp, FUN = function(x) {
        if ("had_excused_person" %in% names( x ) ) {
            x |>
                dplyr::select(id, had_excused_person) |>
                tidyr::unnest(had_excused_person) |>
                dplyr::mutate(pers_id = as.integer(
                    gsub(pattern = "person/", replacement = "",
                         x = had_excused_person) ),
                    is_present = 0L ) |>
                dplyr::select(-had_excused_person)
        } } ) |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE)


### Number of Attendees --------------------------------------------------------
number_of_attendees <- lapply(
    X = list_tmp,
    FUN = function(x) {
        if ("number_of_attendees" %in% names( x ) ) {
            x |>
                dplyr::select(id, number_of_attendees)
        } } ) |>
    data.table::rbindlist(use.names = TRUE, fill = TRUE)


### Append list ----------------------------------------------------------------
if ( nrow(had_excused_person) > 0L
     | nrow(had_participant_person) > 0L) {
    pl_attendance <- dplyr::bind_rows(
        had_participant_person, had_excused_person
    ) |>
        dplyr::mutate(activity_date = as.Date(
            gsub(pattern = "eli/dl/event/MTG-PL-", replacement = "", x = id) ) ) |>
        dplyr::arrange(activity_date, pers_id)
}

if ( exists("pl_attendance") && nrow(number_of_attendees) > 0L ) {
    pl_attendance <- pl_attendance |>
        dplyr::full_join(
            y = number_of_attendees,
            by = "id" ) |>
        dplyr::rename(event_pl_id = id) |>
        dplyr::arrange(activity_date, pers_id)
}


## Save to disk conditional on mandate -----------------------------------------
if ( exists("pl_attendance") && exists("today_date") ) {
    data.table::fwrite(
        x = pl_attendance[, list(activity_date, pers_id, is_present)],
        file = here::here("data_out", "attendance", "pl_attendance_today.csv") )
} else if ( exists("pl_attendance")
            && !exists("today_date")
            && mandate_starts == as.character("2019-07-01")) {
    data.table::fwrite(
        x = pl_attendance[, list(activity_date, pers_id, is_present)],
        file = here::here("data_out", "attendance", "pl_attendance_all.csv") )
} else if ( exists("pl_attendance")
            && !exists("today_date")
            && mandate_starts == as.character("2024-07-14") ) {
    data.table::fwrite(
        x = pl_attendance[, list(activity_date, pers_id, is_present)],
        file = here::here("data_out", "attendance", "pl_attendance_10.csv") )
}


#------------------------------------------------------------------------------#
# Clean up before exiting -----------------------------------------------------#
# Remove objects
rm(pl_attendance, chunk_size, had_excused_person, had_participant_person,
   i_chunk, list_tmp, meet_pl_ids, meet_pl_ids_chunks,
   number_of_attendees)
# Run time
script_ends <- Sys.time()
script_lapsed = script_ends - script_starts
cat("\n=====\nFinished collecting Attendance Lists.\n=====\n")
print(script_lapsed) # Execution time

rm(list = ls(pattern = "script_"))
