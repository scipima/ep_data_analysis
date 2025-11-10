###--------------------------------------------------------------------------###
# MEPs Open Data API -----------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' This script connects to the EP Open Data Portal API to download the list of MEPs for either the current mandate (i.e. 10th) or all mandates currently available on the servers (i.e. 9th and 10th).
#' The script implies that `api_meetings.R` has already been executed.
#' This is because it needs the output `pl_meetings_.*.csv` from that script to extract the Plenary dates.
#' One of the most important aspect of this script is the grid of MEPs.
#' More precisely, we create a grid of all combinations of Plenary dates and MEPs based upon the periods in which MEPs were Members.
#' This is a critical feature as it enables us to measure the participation of the MEPs.
#' In addition, there seems to be an issue with `GUE`- `The Left`, as MEPs belonging to that Group are reported twice (as the Group changed name during the mandate).
#' This needs to be sorted downstream, as we prefer to keep the hytorical record as is.
#' REF: https://data.europarl.europa.eu/en/home; https://data.europarl.europa.eu/en/developer-corner/opendata-api.
#------------------------------------------------------------------------------#

# rm(list = ls())
script_starts <- Sys.time()
cat("\n=====\nStarting to collect MEPs data.\n=====\n")

#-----------------------------------------------------------------------------#
## Libraries ------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c("curl", "data.table", "doFuture", "dplyr", "here", "httr2", 
                        "janitor", "jsonlite", "lubridate", "progressr", "readr",
                        "stringi", "tidyr", "tidyselect") )

#------------------------------------------------------------------------------#
# REPO SETUP: check if dir exists to dump incoming & processed files ----------#
source(file = here::here("scripts_r", "repo_setup.R") )
source(file = here::here("scripts_r", "parallel_api_calls.R"))

# Hard code the start of the mandate -----------------------------------------#
if ( !exists("mandate_starts") ) {
  mandate_starts <- "2024-07-14" }


###--------------------------------------------------------------------------###
## GET/meps --------------------------------------------------------------------
# Returns the list of all the MEPs --------------------------------------------#

### 9th mandate ----------------------------------------------------------------
#' EXAMPLE: https://data.europarl.europa.eu/api/v2/meps?parliamentary-term=9&format=application%2Fld%2Bjson&offset=0&limit=50

if ( mandate_starts == "2019-07-01" ) {
  # Create an API request
  req <- httr2::request(
    paste0("https://data.europarl.europa.eu/api/v2/meps?parliamentary-term=9&format=application%2Fld%2Bjson&offset=0") ) |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0")

  # Add time-out and ignore error before performing request
  resp <- req |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
    httr2::req_error(is_error = ~FALSE) |> # ignore error, we deal with it below
    httr2::req_throttle(capacity = 490, fill_time_s = 300) |> # call politely
    httr2::req_retry(max_tries = 5, # retry a bunch of times in case of failures
                     backoff = ~ 2 ^ .x + runif(n = 1, min = -0.5, max = 0.5) ) |>
    httr2::req_perform()

  # If not an error, download and make available in ENV
  if ( httr2::resp_status(resp) == 200L ) {
    resp_body <- resp |>
      httr2::resp_body_json( simplifyDataFrame = TRUE )
  } else {
    stop("\nWARNING: API request failed for 9th mandate MEPs.\n")
  }

  # Clean data
  meps_mandate_9 <- resp_body$data |>
    janitor::clean_names() |>
    data.table::as.data.table()
  meps_mandate_9 <- meps_mandate_9[, list(mep_name = label, family_name, given_name,
                                          pers_id = as.integer(identifier),
                                          mandate = 9L)]

  # Remove API objects
  rm(req, resp, resp_body)
}

### 10th mandate ----------------------------------------------------------------
#' EXAMPLE: https://data.europarl.europa.eu/api/v2/meps?parliamentary-term=9&format=application%2Fld%2Bjson&offset=0&limit=50

if ( mandate_starts %in% c("2019-07-01", "2024-07-14") ) {

  req <- httr2::request(
    paste0("https://data.europarl.europa.eu/api/v2/meps?parliamentary-term=10&format=application%2Fld%2Bjson&offset=0") ) |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0")

  # Add time-out and ignore error before performing request
  resp <- req |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
    httr2::req_error(is_error = ~FALSE) |> # ignore error, we deal with it below
    httr2::req_throttle(capacity = 490, fill_time_s = 300) |> # call politely
    httr2::req_retry(max_tries = 5, # retry a bunch of times in case of failures
                     backoff = ~ 2 ^ .x + runif(n = 1, min = -0.5, max = 0.5) ) |>
    httr2::req_perform()

  # If not an error, download and make available in ENV
  if ( httr2::resp_status(resp) == 200L ) {
    resp_body <- resp |>
      httr2::resp_body_json( simplifyDataFrame = TRUE )
  } else {
    stop("\nWARNING: API request failed for 10th mandate MEPs.\n")
  }

  # Clean data
  meps_mandate_10 <- resp_body$data |>
    janitor::clean_names() |>
    data.table::as.data.table()
  meps_mandate_10 <- meps_mandate_10[, list(mep_name = label, family_name, given_name,
                                            pers_id = as.integer(identifier),
                                            mandate = 10L)]

  # Remove API objects
  rm(req, resp, resp_body)
}


# Rename conditional on mandate -----------------------------------------------#
if ( mandate_starts == "2019-07-01") {
  # Append mandates ---------------------------------------------------------#
  meps_mandate <- data.table::rbindlist(list(meps_mandate_9, meps_mandate_10))
  # Remove objects ----------------------------------------------------------#
  rm(meps_mandate_9, meps_mandate_10)
} else if ( mandate_starts == "2024-07-14" ) {
  # Rename object
  meps_mandate <- data.table::copy(meps_mandate_10)
  # Remove objects ----------------------------------------------------------#
  rm(meps_mandate_10)
}


#------------------------------------------------------------------------------#
## GET/MEP-ID ------------------------------------------------------------------

#------------------------------------------------------------------------------#
#' Having collected all MEPs for the current mandate, we get detailed info on all of them.
#' This includes party and political group membership, nationality, etc.
#------------------------------------------------------------------------------#

# get MEPs' IDs and build API URLs
mep_ids <- sort( unique(meps_mandate$pers_id) )
chunk_size <- 50L
mep_ids_chunks <- split(x = mep_ids, ceiling(seq_along(mep_ids) / chunk_size))

# Build API URLs for each chunk
api_urls <- sapply(mep_ids_chunks, function(chunk) {
  paste0(
    "https://data.europarl.europa.eu/api/v2/meps/",
    paste0(chunk, collapse = ","),
    "?format=application%2Fld%2Bjson"
  )
})

# Use parallel API calls
use_parallel <- length(api_urls) > 1
if (use_parallel) {
  cat("Multiple MEP API calls detected - using parallel processing\n")
  results <- parallel_api_calls(
    urls = api_urls,
    capacity = 490,
    fill_time_s = 300,
    max_retries = 5,
    show_progress = TRUE,
    extract_data = TRUE
  )
} else {
  cat("Single MEP API call - using sequential processing\n")
  results <- parallel_api_calls(
    urls = api_urls,
    show_progress = FALSE,
    extract_data = TRUE
  )
}

meps_ids_list <- results$responses[!results$failed_calls]

if (length(meps_ids_list) == 0) {
  stop("\nWARNING: All API requests failed for MEPs. Try again later.\n")
}

# Check for any NULL list items -----------------------------------------------#
if ( any( sapply(X = meps_ids_list, is.null) ) ) {
  meps_ids_list <- meps_ids_list[ !sapply(X = meps_ids_list, is.null) ]
  warning("!!WATCH OUT!! You missed one or more MEPs!")
}

# store tmp list if code breaks down the line ---------------------------------#
if (mandate_starts == "2024-07-14") {
  readr::write_rds(x = meps_ids_list,
                   file = here::here("data_out", "meps", "meps_ids_list.rds") )
}


###--------------------------------------------------------------------------###
## Extract age, mandate, political group, and national party -------------------
meps_ids_list_dt = data.table::rbindlist(l = meps_ids_list,
                                         use.names = TRUE, fill = TRUE)
# Empty repository
hasMembership = vector(mode = "list",
                       length = length(meps_ids_list_dt$hasMembership))
for (i_data in seq_along(meps_ids_list_dt$hasMembership) ) {
  # Extract DF
  dt_tmp = meps_ids_list_dt[i_data, list(hasMembership)]
  dt_tmp = dt_tmp$hasMembership[[1]]
  # Convert cols
  dt_tmp$country = as.character(dt_tmp$represents)
  dt_tmp$start_date = dt_tmp$memberDuring$startDate
  if ("endDate" %in% names(dt_tmp$memberDuring)) {
    dt_tmp$end_date = dt_tmp$memberDuring$endDate }
  # Subset
  hasMembership[[i_data]] = dt_tmp[, names(dt_tmp) %in%
                                     c("country", "role", "organization",
                                       "membershipClassification",
                                       "start_date", "end_date")]
  rm(dt_tmp, i_data) # remove objects
}
# Append
hasMembership = data.table::rbindlist(
  l = setNames(object = hasMembership, nm = meps_ids_list_dt$identifier),
  use.names = TRUE, fill = TRUE, idcol = "pers_id") |>
  janitor::clean_names()
hasMembership[, pers_id := as.integer(pers_id)]


### MEPs age -------------------------------------------------------------------
meps_age <- unique(meps_ids_list_dt[, list(identifier, bday)])
meps_age[, ':='(
  mep_age = floor( as.numeric( Sys.Date() - as.Date(bday) ) / 365.25),
  identifier = as.integer(identifier) )
]

# Save to disk conditional on mandate -----------------------------------------#
if (mandate_starts == "2019-07-01") {
  meps_mandate |>
    # If you need MEPs' AGE, uncomment the following line
    # dplyr::left_join(y = meps_age, by = c("pers_id" = "identifier")) |>
    data.table::fwrite(file = here::here(
      "data_out", "meps", "meps_mandate_all.csv") )
} else if ( mandate_starts == "2024-07-14" ) {
  meps_mandate |>
    # If you need MEPs' AGE, uncomment the following line

    # dplyr::left_join(y = meps_age, by = c("pers_id" = "identifier")) |>
    data.table::fwrite(file = here::here(
      "data_out", "meps", "meps_mandate_10.csv") )
}


#------------------------------------------------------------------------------#
### Create grid of MEPs and Dates ----------------------------------------------

# Load meetings dates: 10th mandate (use centralized helper)
if (mandate_starts == "2024-07-14") {
  pl_meetings <- get_api_data(
    path = here::here("data_out", "meetings", "pl_meetings_10.csv"),
    script = here::here("scripts_r", "api_meetings.R"),
    max_days = 1L,
    file_type = "csv",
    verbose = TRUE
  )
  data.table::setDT(pl_meetings)
  if (is.null(pl_meetings) || nrow(pl_meetings) == 0L) {
    stop("pl_meetings is empty after get_api_data(); check api_meetings.R")
  }
}

# Load meetings dates: all mandates (use centralized helper)
if (mandate_starts == "2019-07-01") {
  pl_meetings <- get_api_data(
    path = here::here("data_out", "meetings", "pl_meetings_all.csv"),
    script = here::here("scripts_r", "api_meetings.R"),
    max_days = 1L,
    file_type = "csv",
    verbose = TRUE
  )
  data.table::setDT(pl_meetings)
  if (is.null(pl_meetings) || nrow(pl_meetings) == 0L) {
    stop("pl_meetings is empty after get_api_data(); check api_meetings.R")
  }
}


# Grid ------------------------------------------------------------------------#
if (mandate_starts == "2019-07-01") {
  meps_data_grid <- data.table::rbindlist(l = list(
    # 9th mandate ---------------------------------------------------------#
    expand.grid(
      activity_date = unique(pl_meetings$activity_date[
        pl_meetings$activity_date < as.IDate("2024-07-14")]),
      pers_id = unique(meps_mandate$pers_id[
        meps_mandate$mandate == 9L] ),
      mandate = 9L ) ,
    # 10th mandate --------------------------------------------------------#
    expand.grid(
      activity_date = unique(pl_meetings$activity_date[
        pl_meetings$activity_date >= as.IDate("2024-07-14")]),
      pers_id = unique(meps_mandate$pers_id[
        meps_mandate$mandate == 10L] ),
      mandate = 10L)
  ) )
} else {
  # 10th mandate ------------------------------------------------------------#
  meps_data_grid <- expand.grid(
    activity_date = unique(pl_meetings$activity_date),
    pers_id = unique(meps_mandate$pers_id),
    mandate = 10L) |>
    data.table::as.data.table()
}


###--------------------------------------------------------------------------###
### Extract start and end of mandates for each MEP, as well as country ---------
meps_start_end <- hasMembership[
  grepl("MEMBER_PARLIAMENT", x = role, ignore.case = TRUE)
  # grab mandate 9 or 10 ----------------------------------------------------#
  & (organization %in% c("org/ep-9", "org/ep-10") ),
  list(pers_id, country, mandate = organization, start_date, end_date)]
# Process data
meps_start_end[, `:=`(
  start_date = lubridate::as_date(start_date), # tz = "Europe/Brussels"
  # if still in EP, end=NA ; we input the current date in that case
  end_date = fifelse(is.na(end_date), as.character(Sys.Date()), end_date),
  country = gsub(
    pattern = "http://publications.europa.eu/resource/authority/country/",
    replacement = "",
    x = country)
)][, `:=`(
  end_date = lubridate::as_date(end_date), # tz = "Europe/Brussels"
  mandate = as.integer(
    gsub(pattern = "org/ep-", replacement = "", x = mandate))
)]


##----------------------------------------------------------------------------##
# There is a bug in the data whereby some MEPs are wrongly assigned to a different EP Mandate
# check if that is the case, and fix it if needs be
if ( mandate_starts == "2024-07-14" ){
  if(
    any(
      meps_start_end$start_date >= mandate_starts
      & meps_start_end$mandate == 9L
    )
  )
  {
    warning("Some MEPs have been wrongly assigned to a different EP Mandate")
    meps_start_end[
      start_date >= mandate_starts,
      mandate := 10L]
  }
}

if ( mandate_starts == "2019-07-01" ){
  if(
    any(
      meps_start_end$start_date >= as.Date("2024-07-14")
      & meps_start_end$mandate == 9L
    )
  )
  {
    warning("Some MEPs have been wrongly assigned to a different EP Mandate")
    meps_start_end[start_date >= as.Date("2024-07-14"), mandate := 10L]
  }
}
##----------------------------------------------------------------------------##


# Merge actual MEPs' periods with full grid of dates --------------------------#
cat("\nDIMs meps_data_grid:", paste(dim(meps_data_grid), collapse = ","), "\n")
cat("\nDIMs meps_start_end:", paste(dim(meps_start_end), collapse = ","), "\n")
meps_dates <- meps_start_end[
  meps_data_grid,
  on = list(pers_id, mandate), nomatch = NA, allow.cartesian = TRUE
]
cat("\nDIMs meps_dates after merge:", paste(dim(meps_dates), collapse = ","), "\n")

# Subset data based on choice of mandate --------------------------------------#
if (mandate_starts == "2024-07-14") {
  meps_dates <- meps_dates[mandate == 10L,] }


# Filter for just TRUE dates --------------------------------------------------#
meps_dates[, `:=`(
  to_keep = ifelse(
    test = activity_date >= start_date & activity_date <= end_date,
    yes = 1L, no = 0L)),
  by = list(pers_id)]
# dim(meps_dates)
meps_dates <- meps_dates[to_keep == 1L]
# dim(meps_dates)
meps_dates[, c("to_keep", "start_date", "end_date") := NULL]


###--------------------------------------------------------------------------###
### Extract political groups ---------------------------------------------------
meps_polgroups <- hasMembership[
  grepl(
    pattern = "EU_POLITICAL_GROUP",
    x = membership_classification, ignore.case = TRUE
  ),
  list(pers_id,
       polgroup_id = as.integer( gsub(
         pattern = "org/", replacement = "", x = organization) ),
       start_date = data.table::as.IDate(start_date),
       end_date
  )
][order(pers_id)]
meps_polgroups[, end_date := data.table::fifelse(
  test = is.na(end_date), yes = as.character(Sys.Date()), no = end_date)]
meps_polgroups[, end_date := data.table::as.IDate(end_date)]

# Filter to just membership started and ended after the beginning of the mandate
meps_polgroups = meps_polgroups[
  start_date >= mandate_starts
  & end_date >= as.Date(mandate_starts)
]

if ( all(mep_ids %in% meps_polgroups$pers_id) ) {
  cat("\n=====\nEvery MEP in this mandate has a Political Group.\n=====\n")
} else {
  cat("\n=====\nSome MEPs in this mandate do not have a Political Group.\n")
  cat("These are the MEPs with no Political Group:\n",
      paste0(mep_ids[ !mep_ids %in% meps_polgroups$pers_id ], collapse = ","),
      "\n=====\n")
}

# Merge
cat("\nDIMs meps_dates:", paste(dim(meps_dates), collapse = ","), "\n")
cat("\nDIMs meps_polgroups:", paste(dim(meps_polgroups), collapse = ","), "\n")
# perform join keeping all rows from both (full outer join) via rbind of keys then merge-like join
meps_dates_polgroups <- meps_polgroups[
  meps_dates,
  on = list(pers_id), allow.cartesian = TRUE
]
cat("\nDIMs meps_dates_polgroups after merge:", paste(dim(meps_dates_polgroups), collapse = ","), "\n")

# Filter for just TRUE dates
meps_dates_polgroups[, `:=`(
  to_keep = ifelse(
    test = activity_date >= start_date & activity_date <= end_date,
    yes = 1L, no = 0L)),
  by = list(pers_id)]
# dim(meps_dates_polgroups)
meps_dates_polgroups <- meps_dates_polgroups[to_keep == 1L]
# dim(meps_dates_polgroups)
meps_dates_polgroups[, c("to_keep", "country", "start_date", "end_date") := NULL]


###--------------------------------------------------------------------------###
### Extract national parties groups --------------------------------------------
meps_natparties <- hasMembership[
  grepl("NATIONAL_POLITICAL_GROUP",
        x = membership_classification, ignore.case = TRUE, perl = TRUE),
  list(pers_id,
       natparty_id = as.integer( gsub(
         pattern = "org/", replacement = "", x = organization) ),
       start_date = data.table::as.IDate(start_date),
       end_date)
][order(pers_id)]
meps_natparties[, end_date := data.table::fifelse(
  test = is.na(end_date), yes = as.character(Sys.Date()), no = end_date)]
meps_natparties[, end_date := data.table::as.IDate(end_date)]

# Filter to just membership started and ended after the beginning of the mandate
meps_natparties = meps_natparties[
  start_date >= mandate_starts
  & end_date >= as.Date(mandate_starts)
]

if ( all(mep_ids %in% meps_natparties$pers_id) ) {
  cat("\n=====\nEvery MEP in this mandate has a National Party.\n=====\n")
} else {
  cat("\n=====\nSome MEPs in this mandate do not have a National Party.\n")
  cat("These are the MEPs with no National Party:\n",
      paste0(mep_ids[ !mep_ids %in% meps_natparties$pers_id ], collapse = ","),
      "\n=====\n")
}
# Fix data bugs
if (mandate_starts == "2024-07-14"){
  meps_natparties[pers_id == 197526L, natparty_id := 6794]
}

# Merge
cat("\nDIMs meps_dates:", paste(dim(meps_dates), collapse = ","), "\n")
cat("\nDIMs meps_natparties:", paste(dim(meps_natparties), collapse = ","), "\n")
meps_dates_natparties <- meps_natparties[
  meps_dates,
  on = list(pers_id), allow.cartesian = TRUE
]
cat("\nDIMs meps_dates_natparties after merge:", paste(dim(meps_dates_natparties), collapse = ","), "\n")

# Filter for just TRUE dates
meps_dates_natparties[, `:=`(
  to_keep = ifelse(
    test = activity_date >= start_date & activity_date <= end_date,
    yes = 1L, no = 0L)),
  by = list(pers_id)]
# dim(meps_dates_natparties)
meps_dates_natparties <- meps_dates_natparties[to_keep == 1L]
# dim(meps_dates_natparties)
meps_dates_natparties[, c("to_keep", "country", "start_date", "end_date") := NULL]


###--------------------------------------------------------------------------###
### Merge MEPs' MANDATES, Political Groups, and National Parties ---------------
cat("\nDIMs meps_dates:", paste(dim(meps_dates), collapse = ","), "\n")
cat("\nDIMs meps_dates_polgroups:", paste(dim(meps_dates_polgroups), collapse = ","), "\n")
# merge on pers_id, activity_date, mandate - emulate full outer join via keys and joins
meps_dates_ids <- meps_dates_polgroups[
  meps_dates,
  on = c("pers_id", "activity_date", "mandate")
]
cat("\nDIMs meps_dates_ids after first merge:", paste(dim(meps_dates_ids), collapse = ","), "\n")
meps_dates_ids <- meps_dates_natparties[
  meps_dates_ids,
  on = c("pers_id", "activity_date", "mandate")
]
cat("\nDIMs meps_dates_ids after second merge:", paste(dim(meps_dates_ids), collapse = ","), "\n")

# Fix data entry issues -------------------------------------------------------#
# sapply(meps_dates_ids, function(x) sum(is.na(x)))
# https://www.europarl.europa.eu/meps/en/185974/JORDI_SOLE/history/9#detailedcardmep
meps_dates_ids[
  pers_id == 185974L & is.na(polgroup_id),
  polgroup_id := 5152L
]
# Barry ANDREWS has natparty membership gap
meps_dates_ids[
  pers_id == 204332L & activity_date == "2024-07-16" & is.na(natparty_id),
  natparty_id := 6741L
]


## Get Country dictionary ------------------------------------------------------
# Read in data
country_dict <- data.table::fread(input = here::here(
  "data_reference", "country_dict.csv") )

# merge -----------------------------------------------------------------------#
meps_dates_ids <- country_dict[
  meps_dates_ids,
  on = c("country_iso3c" = "country")
]
# delete col
meps_dates_ids[, country_iso3c := NULL]


# Save to disk conditional on mandate -----------------------------------------#
# Check
if ( all(mep_ids %in% unique(meps_dates_ids$pers_id)) ) {
  cat("\n=====\nYou have collected IDs for all MEPs.\n====\n")
} else {
  cat("\n=====\nYou did not collect IDs for all MEPs.\n")
  missing_meps = mep_ids[!mep_ids %in% unique(meps_dates_ids$pers_id)]
  cat("These are the MEPs with missing IDs:\n",
      paste0(missing_meps, collapse = ","),
      "\n====\n")

  if ( mandate_starts == "2019-07-01" ) {
    pl_attendance <- data.table::fread(here::here(
      "data_out", "attendance", "pl_attendance_all.csv") )
  } else {
    pl_attendance <- data.table::fread(here::here(
      "data_out", "attendance", "pl_attendance_10.csv") )
  }

  # Test whether Missing MEPs have ever attended a Plenary, otherwise they may just have recently joined
  if ( nrow(pl_attendance[missing_meps %in% pers_id]) > 0L ) {
    cat("\nVery strange: the missing MEPs have attended Plenaries. Check again.\n")
  } else {
    cat("\nThe missing MEPs have never attended Plenaries. They may be new.\n")
  }
  rm(pl_attendance)
}


#------------------------------------------------------------------------------#
## Store processed data --------------------------------------------------------
data.table::setcolorder(x = meps_dates_ids,
                        neworder = c("activity_date", "mandate", "pers_id",
                                     "country_id", "polgroup_id", "natparty_id"))
data.table::setindexv(x = meps_dates_ids,
                      cols = c("activity_date", "mandate", "pers_id"))
if (mandate_starts == "2019-07-01") {
  data.table::fwrite(x =  meps_dates_ids, file = here::here(
    "data_out", "meps", "meps_dates_ids_all.csv"))
} else {
  data.table::fwrite(x =  meps_dates_ids, file = here::here(
    "data_out", "meps", "meps_dates_ids_10.csv") )
}


#------------------------------------------------------------------------------#
## Get Current EP Composition --------------------------------------------------
if (mandate_starts == "2024-07-14") {
  source(file = here::here("scripts_r", "api_meps_current.R") ) }


###--------------------------------------------------------------------------###
# remove intermediate objects
rm(country_dict, hasMembership, pl_meetings)
rm(list = ls(pattern = "mep"))


# test:before brexit, N should be 751; after brexit, 705; 10th mandate 719
# meps_dates_ids[, .N, keyby = list(activity_date)]
