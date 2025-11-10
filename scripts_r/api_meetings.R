###--------------------------------------------------------------------------###
# EP Plenary Meetings ----------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' The script grabs the list of all Plenary Sessions Meetings from the EP API.
#------------------------------------------------------------------------------#

# rm(list = ls())
script_starts <- Sys.time()
cat("\n=====\nStarting to collect Plenary Meetings.\n=====\n")


#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c(
  "data.table", "dplyr", "future.apply", "httr2", "here", "lubridate", "janitor",
  "jsonlite", "stringi", "tidyr", "tidyselect") )


#------------------------------------------------------------------------------#
## Parameters ------------------------------------------------------------------
# Hard code the start of the mandate for the current EP
if ( !exists("mandate_starts") ) {
  mandate_starts <- "2024-07-14"
}


#------------------------------------------------------------------------------#
## GET/Meetings ----------------------------------------------------------------

#------------------------------------------------------------------------------#
#' Here we're getting a list of all the Plenaries during the mandate.
#' EXAMPLE: # https://data.europarl.europa.eu/api/v2/meetings?year=2022&format=application%2Fld%2Bjson&offset=0&limit=50
#------------------------------------------------------------------------------#

# Parameters to loop over: list of years from mandate start to today
years <- paste(
  data.table::year(mandate_starts) : data.table::year(Sys.Date()),
  collapse=",")


### Get all meetings -----------------------------------------------------------
req = httr2::request(
  paste0("https://data.europarl.europa.eu/api/v2/meetings?year=",
         years,
         "&format=application%2Fld%2Bjson&offset=0") ) |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0")

# Add time-out and ignore error before performing request
resp = req |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
  httr2::req_error(is_error = ~FALSE) |> # ignore error, we deal with it below
  httr2::req_throttle(capacity = 490, fill_time_s = 300) |> # call politely
  httr2::req_retry(max_tries = 5, # retry a bunch of times in case of failures
                   backoff = ~ 2 ^ .x + runif(n = 1, min = -0.5, max = 0.5) ) |>
  httr2::req_perform()

# If not an error, download and make available in ENV
if ( httr2::resp_status(resp) == 200L ) {
  resp_body = resp |>
    httr2::resp_body_json(simplifyDataFrame = TRUE)
} else {
  stop("\nWARNING: API request failed for Plenary Meetings.\n")
}

# Extract data -----------------------------------------------------------------#
pl_meetings <- data.table::as.data.table(
  x = resp_body$data[, c("id", "activity_id", "activity_date")]
)

# Add date field --------------------------------------------------------------#
pl_meetings[, `:=`(
  activity_date = data.table::fifelse(
    test = is.na(activity_date),
    yes = as.Date( gsub(pattern = "eli/dl/event/MTG-PL-",
                        replacement = "", x = id) ),
    no = as.Date(activity_date) ) )
]

# Get just the Plenaries that will take place, including today ----------------#
pl_meetings_foreseen <- pl_meetings[
  activity_date >= Sys.Date()
][order(activity_date)]

# Get just the Plenaries that have already taken place, including today -------#
pl_meetings <- pl_meetings[activity_date >= mandate_starts
                           & activity_date <= Sys.Date()]
data.table::setkeyv(x = pl_meetings, cols = "activity_date")


# Save to disk conditional on mandate -----------------------------------------#
if (mandate_starts == as.character("2019-07-01") ) {
  data.table::fwrite(
    x = pl_meetings[order(activity_date)],
    file = here::here("data_out", "meetings", "pl_meetings_all.csv"))
} else {
  # Up to now
  data.table::fwrite(
    x = pl_meetings[order(activity_date)],
    file = here::here("data_out", "meetings", "pl_meetings_10.csv") )

  # Foreseen activities
  data.table::fwrite(
    x = pl_meetings_foreseen[order(activity_date)],
    file = here::here("data_out", "meetings", "pl_meetings_foreseen.csv") )
}


#------------------------------------------------------------------------------#
# Clean up before exiting -----------------------------------------------------#
rm(list = ls(pattern = "pl_"))
rm(req, resp, resp_body, years)

# Run time
script_ends <- Sys.time()
script_lapsed = script_ends - script_starts
cat("\n=====\nFinished collecting Plenary Meetings.\n=====\n")
print(script_lapsed) # Execution time

# Remove objects
rm(list = ls(pattern = "script_"))
