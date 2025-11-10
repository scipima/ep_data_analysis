#------------------------------------------------------------------------------#
# Current MEPS -----------------------------------------------------------------
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c("curl", "data.table", "dplyr", "here", "httr2", "lubridate",
                        "janitor", "jsonlite", "readr", "stringi", "tidyr",
                        "tidyselect") )


# Hard code the start of the mandate ----------------------------------------#
if ( !exists("mandate_starts") ) {
  mandate_starts <- as.Date("2024-07-14") }

#------------------------------------------------------------------------------#
## GET/meps/show-current -------------------------------------------------------
# Returns the list of all active MEPs for today's date

req <- httr2::request(
  paste0("https://data.europarl.europa.eu/api/v2/meps/show-current?format=application%2Fld%2Bjson&offset=0") ) |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
  httr2::req_retry(max_tries = 5,
                   backoff = ~ 2 ^ .x + runif(n = 1, min = -0.5, max = 0.5) ) |>
  httr2::req_perform() |>
  httr2::resp_body_json(simplifyDataFrame = TRUE)

# Get vector of current MEPs IDs
meps_ids_current <- req$data |>
  janitor::clean_names() |>
  dplyr::pull(identifier) |>
  as.integer()

# Remove API objects --------------------------------------------------------###
rm(req)

# store tmp list if code breaks down the line
if (!exists("meps_ids_list")) {
  meps_ids_list <- readr::read_rds(
    file = here::here("data_out", "meps", "meps_ids_list.rds") ) }


### --------------------------------------------------------------------------###
### Get mandate, political group, and national party information ----------------
hasMembership <- lapply(
  X = meps_ids_list,
  FUN = function(i_data) {
    i_data |>
      dplyr::select(
        pers_id = identifier,
        bday, hasMembership
      ) |>
      tidyr::unnest(hasMembership, keep_empty = TRUE) |>
      tidyr::unnest_wider(memberDuring, names_sep = "_")
  }
) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::filter(pers_id %in% meps_ids_current) |>  # filter to just current MEPs
  dplyr::select(-dplyr::any_of("contactPoint")) |>
  dplyr::mutate(pers_id = as.integer(pers_id)) |>
  janitor::clean_names()


### MEPs age -------------------------------------------------------------------
meps_age <- unique(hasMembership[, list(pers_id, bday)]) |>
  dplyr::mutate(
    mep_age = floor( as.numeric( Sys.Date() - as.Date(bday) ) / 365.25) )


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
       start_date = data.table::as.IDate(member_during_start_date),
       end_date = member_during_end_date)
][order(pers_id)]
meps_polgroups[, end_date := data.table::fifelse(
  test = is.na(end_date), yes = as.character(Sys.Date()), no = end_date)]
meps_polgroups[, end_date := data.table::as.IDate(end_date)]

# Filter to just membership started after the beginning of the mandate
meps_polgroups[start_date >= mandate_starts & end_date >= mandate_starts]

# Ensure that data is sorted so that subsequent filtering is correct
data.table::setorder(x = meps_polgroups, pers_id, -start_date)
meps_polgroups <- meps_polgroups[, head(.SD, 1L), # get just the most recent membership
                                 keyby = list(pers_id) # by pers_id
]
# Checks
if (meps_polgroups[, .N, by = "pers_id"][, mean(N)] > 1) {
  stop("\n=====\nYou cannot have more than 1 observation per MEP. Revise.\n=====\n")
}

# Check
if ( all(meps_ids_current %in% meps_polgroups$pers_id) ) {
  cat("\n=====\nEvery current MEP has a Political Group.\n=====\n")
} else {
  cat("\n=====\nSome of the current MEPs do not have a Political Group.\n")
  cat("These are the MEPs with no Political Group:\n",
      paste0(meps_ids_current[ !meps_ids_current %in% meps_polgroups$pers_id ],
             collapse = ","),
      "\n=====\n")
}


###--------------------------------------------------------------------------###
### Extract national parties ---------------------------------------------------
meps_natparties <- hasMembership[
  grepl(pattern = "NATIONAL_POLITICAL_GROUP",
        x = membership_classification, ignore.case = TRUE),
  list(pers_id,
       natparty_id = as.integer( gsub(
         pattern = "org/", replacement = "", x = organization) ),
       start_date = data.table::as.IDate(member_during_start_date),
       end_date = member_during_end_date)
][order(pers_id)]

meps_natparties[, end_date := data.table::fifelse(
  test = is.na(end_date), yes = as.character(Sys.Date()), no = end_date)]
meps_natparties[, end_date := data.table::as.IDate(end_date)]

# Filter to just membership started after the beginning of the mandate
meps_natparties[start_date >= mandate_starts & end_date >= mandate_starts]

# Ensure that data is sorted so that subsequent filtering is correct
data.table::setorder(x = meps_natparties, pers_id, -start_date)
meps_natparties <- meps_natparties[, head(.SD, 1L), # get just the most recent membership
                                 keyby = list(pers_id) # by pers_id
]
# Checks
if (meps_natparties[, .N, by = "pers_id"][, mean(N)] > 1) {
  stop("\n=====\nYou cannot have more than 1 observation per MEP. Revise.\n=====\n")
}

# Check
if ( all(meps_ids_current %in% meps_natparties$pers_id) ) {
  cat("\n=====\nEvery current MEP has a National Party.\n=====\n")
} else {
  cat("\n=====\nSome of the current MEPs do not have a National Party.\n")
  cat("These are the MEPs with no National Party:\n",
      paste0(meps_ids_current[ !meps_ids_current %in% meps_natparties$pers_id ],
             collapse = ","),
      "\n=====\n")
}


###--------------------------------------------------------------------------###
### Extract country ------------------------------------------------------------
meps_country <- hasMembership[
  grepl("MEMBER_PARLIAMENT", x = role, ignore.case = TRUE),
  # grab mandate 9 0r 10 ------------------------------------------------------#
  # & organization == "org/ep-10",
  list(pers_id,
       country = represents,
       mandate = as.integer(gsub(pattern="org/ep-", replacement="", x=organization)),
       start_date = data.table::as.IDate(member_during_start_date)
)]

# Filter to current mandate
meps_country = meps_country[
  mandate == 10L | start_date >= mandate_starts
]

# Clean COUNTRY values
meps_country[,`:=`(
  country_iso3c = gsub(
    pattern = "http://publications.europa.eu/resource/authority/country/",
    replacement = "", x = country)
)]

# Read in country dictionary
country_dict = data.table::fread(input = here::here(
  "data_reference", "country_dict.csv") )

# Merge MEPs data with country dictionary
meps_country = country_dict[
  meps_country,
  on = "country_iso3c"
][, country := NULL]


###--------------------------------------------------------------------------###
### Merge MEPs' MANDATES, Political Groups, and National Parties ---------------
## Convert to data.table and print dims
data.table::setDT(meps_country)
data.table::setDT(meps_polgroups)
data.table::setDT(meps_natparties)
data.table::setDT(meps_age)
cat("dim meps_country:", paste(dim(meps_country), collapse = ","), "\n")
cat("dim meps_polgroups:", paste(dim(meps_polgroups), collapse = ","), "\n")
cat("dim meps_natparties:", paste(dim(meps_natparties), collapse = ","), "\n")
cat("dim meps_age:", paste(dim(meps_age), collapse = ","), "\n")

# perform joins by pers_id (left joins behavior preserved by starting from meps_country)
meps_current <- meps_polgroups[
  meps_country, 
  on = .(pers_id)
  ]
cat("dim after join polgroups + country:", paste(dim(meps_current), collapse = ","), "\n")
meps_current <- meps_natparties[
  meps_current, 
  on = .(pers_id)
  ]
cat("dim after join natparties:", paste(dim(meps_current), collapse = ","), "\n")
meps_current <- meps_age[
  meps_current, 
  on = .(pers_id)
  ]
cat("dim after join age:", paste(dim(meps_current), collapse = ","), "\n")

# write to disk
data.table::fwrite(x = meps_current[, list(pers_id, country_id, polgroup_id,
                                           natparty_id)],
                   here::here("data_out", "meps", "meps_current.csv"))

rm(meps_country, meps_natparties, meps_polgroups)
