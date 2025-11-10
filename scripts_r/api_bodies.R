###--------------------------------------------------------------------------###
# BODIES -----------------------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' Here we grab all the bodies appearing in the MEPs' history.
#' For the EP administration, `bodies` are various things, from `Committees`, to `Political Groups`, to `national parties`.
#------------------------------------------------------------------------------#

# rm(list = ls(all.names = TRUE))

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c("curl", "data.table", "dplyr", "here", "httr2", "lubridate",
                        "janitor", "jsonlite", "readr", "stringi", "tidyr",
                        "tidyselect") )


# Hard code the start of the mandate ----------------------------------------#
if ( !exists("mandate_starts") ) {
  mandate_starts <- "2024-07-14" }


#------------------------------------------------------------------------------#
## Political Groups and National Parties ---------------------------------------
# Read in data conditional on mandate -----------------------------------------#
if (mandate_starts == "2019-07-01") {
  meps_dates_ids <- data.table::fread(
    here::here("data_out", "meps", "meps_dates_ids_all.csv") )
} else if (mandate_starts == "2024-07-14") {
  meps_dates_ids <- data.table::fread(
    here::here("data_out", "meps", "meps_dates_ids_10.csv") )
} else {
  stop("Error: No mandate speficied")
}


# Grab vector of bodies in the current data -----------------------------------#
bodies_ids <- na.omit( c( unique( meps_dates_ids$polgroup_id),
                          unique( meps_dates_ids$natparty_id) ) )

# Split the vector in chunks of size 50 each
chunk_size <- 50L
bodies_ids_chunks <- split(
  x = bodies_ids, ceiling(seq_along(bodies_ids) / chunk_size) )
# Empty repository
list_tmp <- vector(mode = "list", length = length(bodies_ids_chunks))

# loop to get all decisions ---------------------------------------------------#
for (i_chunk in seq_along(list_tmp) ) {
  print(i_chunk)
  # Create an API request
  req <- httr2::request( paste0(
    "https://data.europarl.europa.eu/api/v2/corporate-bodies/",
    paste0(bodies_ids_chunks[[i_chunk]], collapse = ","),
    "?format=application%2Fld%2Bjson") )
  # Add time-out and ignore error
  resp <- req |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
    httr2::req_error(is_error = ~FALSE) |>
    httr2::req_throttle(60 / 60) |> # 1 per second
    httr2::req_perform()
  # If not an error, download and make available in ENV
  if ( httr2::resp_status(resp) == 200L) {
    resp_body <- resp |>
      httr2::resp_body_json( simplifyDataFrame = TRUE )
    list_tmp[[i_chunk]] = resp_body$data
  }
}


# store tmp list if code breaks down the line ---------------------------------#
if (mandate_starts == "2024-07-14") {
  readr::write_rds(x = list_tmp,
                   file = here::here("data_out", "bodies", "bodies_ids_list.rds") ) }

# append list and transform into DF
body_id_full <- lapply(X = list_tmp, FUN = function(x) {
  x |>
    tidyr::unnest(temporal, keep_empty = TRUE, names_sep = "_") |>
    tidyr::unnest(altLabel, keep_empty = TRUE, names_sep = "_") |>
    tidyr::unnest(prefLabel, keep_empty = TRUE, names_sep = "_")
}) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::select(identifier, label, classification, temporal_id, temporal_startDate,
                ends_with("_en"), ends_with("_fr")) |>
  dplyr::mutate(classification = gsub(
    pattern = "http://publications.europa.eu/resource/authority/corporate-body-classification/",
    replacement = "", x = classification) ) |>
  janitor::clean_names()


# Save to disk conditional on mandate -----------------------------------------#
if (mandate_starts == "2019-07-01") {
  data.table::fwrite(x = body_id_full,
                     here::here("data_out", "bodies", "body_id_full_all.csv"))
  # write NATIONAL PARTIES ----------------------------------------------------#
  data.table::fwrite(x = body_id_full[
    grepl(pattern = "NATIONAL_POLITICAL_GROUP", x = classification),
    -c("classification")],
    here::here("data_out", "bodies", "national_parties_all.csv"))
  # write POLITICAL GROUPS ----------------------------------------------------#
  data.table::fwrite(x = body_id_full[
    grepl(pattern = "EU_POLITICAL_GROUP", x = classification),
    -c("classification") ],
    here::here("data_out", "bodies", "political_groups_all.csv") )
} else if (mandate_starts == "2024-07-14") {
  data.table::fwrite(x = body_id_full,
                     here::here("data_out", "bodies", "body_id_full_10.csv"))
  # write NATIONAL PARTIES ----------------------------------------------------#
  data.table::fwrite(x = body_id_full[
    grepl(pattern = "NATIONAL_POLITICAL_GROUP", x = classification),
    -c("classification")],
    here::here("data_out", "bodies", "national_parties_10.csv"))
  # write POLITICAL GROUPS ----------------------------------------------------#
  data.table::fwrite(x = body_id_full[
    grepl(pattern = "EU_POLITICAL_GROUP", x = classification),
    -c("classification") ],
    here::here("data_out", "bodies", "political_groups_10.csv") )
} else {
  stop("Error: No mandate speficied")
}

# remove objects --------------------------------------------------------------#
rm(list_tmp, bodies_id)


#------------------------------------------------------------------------------#
## Committees ------------------------------------------------------------------
req <- httr2::request(
  base_url = "https://data.europarl.europa.eu/api/v2/corporate-bodies?body-classification=COMMITTEE_PARLIAMENTARY_STANDING,COMMITTEE_PARLIAMENTARY_TEMPORARY,COMMITTEE_PARLIAMENTARY_SPECIAL,COMMITTEE_PARLIAMENTARY_SUB,COMMITTEE_PARLIAMENTARY_JOINT&format=application%2Fld%2Bjson&offset=0") |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
  httr2::req_perform() |>
  httr2::resp_body_json(simplifyDataFrame = TRUE)

# Extract data from .json and unnest
# This is a long DF with all the Joint Committees unnested.
# Such long format is useful when we want to see which Joint Committees are linked to which Standing Committees.
committee_long <- req$data |>
  janitor::clean_names() |>
  tidyr::unnest(linked_to, keep_empty = TRUE) |>
  dplyr::left_join(
    y = req$data |>
      janitor::clean_names() |>
      dplyr::select(id, joint_committees_labs = label),
    by = c("linked_to" = "id") )

# This is the wide version of the same data.
committee_wide <- committee_long |>
  dplyr::group_by(id, type, identifier, label, classification) |>
  dplyr::mutate(
    joint_committees_labs = ifelse(
      test = is.na(joint_committees_labs),
      yes = joint_committees_labs,
      no = paste(joint_committees_labs, collapse = "_") ),
    linked_to = ifelse(
      test = is.na(linked_to),
      yes = linked_to,
      no = paste(linked_to, collapse = "_") ) ) |>
  dplyr::ungroup() |>
  dplyr::distinct()


#------------------------------------------------------------------------------#
## Committee IDs ---------------------------------------------------------------
# Get Committees' IDs
committee_ids <- sort(unique(committee_wide$identifier))
# Split the vector in chunks of size 50 each
chunk_size <- 50L
cmts_ids_chunks <- split(
  x = committee_ids, ceiling(seq_along(committee_ids) / chunk_size) )
# Empty repository
list_tmp <- vector(mode = "list", length = length(cmts_ids_chunks))

for (i_chunk in seq_along(list_tmp) ) {
  print(i_chunk)
  # Create an API request
  req <- httr2::request( paste0(
    "https://data.europarl.europa.eu/api/v2/corporate-bodies/",
    paste0(cmts_ids_chunks[[i_chunk]], collapse = ","),
    "?format=application%2Fld%2Bjson") )
  # Add time-out and ignore error
  resp <- req |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
    httr2::req_error(is_error = ~FALSE) |>
    httr2::req_throttle(60 / 60) |>
    httr2::req_perform()
  # If not an error, download and make available in ENV
  if ( httr2::resp_status(resp) == 200L) {
    resp_body <- resp |>
      httr2::resp_body_json( simplifyDataFrame = TRUE )
    list_tmp[[i_chunk]] = resp_body$data }
}

# append list and transform into DF -------------------------------------------#
cmt_labels = lapply(X = list_tmp, FUN = function(x) {
  # Subset data
  df_tmp = x[, c("id", "altLabel", "prefLabel")]

  # Unnest cols
  df_tmp$altlabel_en = unlist(df_tmp$altLabel$en)
  df_tmp$preflabel_en = unlist(df_tmp$prefLabel$en)

  # Return DF
  return(df_tmp[, c("id", "altlabel_en", "preflabel_en")])
} ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)


# temporal coverage
committee_temporal <- lapply(X = list_tmp, FUN = function(x) {
  if ("temporal" %in% names(x)) {
    x |>
      dplyr::select(id, temporal) |>
      tidyr::unnest(temporal, names_sep = "_") |>
      dplyr::select(-temporal_type) } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  janitor::clean_names()

committee_wide <- committee_wide |>
  dplyr::left_join(y = cmt_labels, by = "id") |>
  dplyr::left_join(y = committee_temporal, by = "id")

committee_long <- committee_long |>
  dplyr::left_join(y = cmt_labels, by = "id") |>
  dplyr::left_join(y = committee_temporal, by = "id")


# Write file to disk ----------------------------------------------------------#
data.table::fwrite(x = committee_wide, here::here(
  "data_out", "bodies", "committee_wide.csv") )
data.table::fwrite(x = committee_long, here::here(
  "data_out", "bodies", "committee_long.csv") )


# remove objects --------------------------------------------------------------#
rm(list_tmp, bodies_id)
