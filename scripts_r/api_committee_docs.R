#------------------------------------------------------------------------------#
# EP Plenary Session Documents -------------------------------------------------
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
#' DESCRIPTION.
#' The script grabs all Committee Documents from the EP API.
#' REF: https://data.europarl.europa.eu/en/home; https://data.europarl.europa.eu/en/developer-corner/opendata-api
#------------------------------------------------------------------------------#

# rm(list = ls())

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
## GET/committee-documents -----------------------------------------------------
# Returns the list of all EP Committee Documents

# EXAMPLE: https://data.europarl.europa.eu/api/v2/committee-documents?parliamentary-term=9&format=application%2Fld%2Bjson&offset=0&limit=50

# 9th mandate -----------------------------------------------------------------#
req <- httr2::request("https://data.europarl.europa.eu/api/v2/committee-documents?parliamentary-term=9&format=application%2Fld%2Bjson&offset=0") |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
  httr2::req_perform() |>
  httr2::resp_body_json(simplifyDataFrame = TRUE)

committee_docs_9 <- req$data |>
  dplyr::mutate(mandate = 9L)
rm(req)

# 10th mandate ----------------------------------------------------------------#
req <- httr2::request("https://data.europarl.europa.eu/api/v2/committee-documents?parliamentary-term=10&format=application%2Fld%2Bjson&offset=0") |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
  httr2::req_perform() |>
  httr2::resp_body_json(simplifyDataFrame = TRUE)

committee_docs_10 <- req$data |>
  dplyr::mutate(mandate = 10L)
rm(req)

# Append DFs ------------------------------------------------------------------#
committee_docs <- dplyr::bind_rows(committee_docs_9, committee_docs_10) |>
  # select(-c(mandate, type)) |>
  # dplyr::distinct() |>
  dplyr::mutate(
    work_type = stringi::stri_replace(
      str = work_type, replacement = "", regex = "def/ep-document-types/") )

# write to disk
data.table::fwrite(committee_docs, here::here(
  "data_out", "docs_cmt", "committee_docs_append.csv"))


###--------------------------------------------------------------------------###
## GET/committee-documents/{doc-id} --------------------------------------------
# Returns a single EP Committee Document for the specified doc ID

# EXAMPLE: https://data.europarl.europa.eu/api/v2/committee-documents/AFCO-PA-745315?format=application%2Fld%2Bjson

committee_docs_ids <- sort(unique(committee_docs$identifier))

# loop to get all decisions
list_tmp <- vector(mode = "list", length = length(committee_docs_ids) )
for ( i_param in seq_along(committee_docs_ids) ) {
  print(committee_docs_ids[i_param])
  # Create an API request
  req <- httr2::request("https://data.europarl.europa.eu/api/v2") |>
    httr2::req_url_path_append("committee-documents") |>
    httr2::req_url_path_append(committee_docs_ids[i_param]) |>
    httr2::req_url_path_append("?format=application%2Fld%2Bjson")
  # Add time-out and ignore error
  resp <- req |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
    httr2::req_error(is_error = ~FALSE) |>
    httr2::req_throttle(60 / 60) |>
    httr2::req_perform()
  # If not an error, download and make available in ENV
  if ( httr2::resp_status(resp) == 200L) {
    resp_body <- resp |>
      httr2::resp_body_json(simplifyDataFrame = TRUE) |>
      purrr::pluck("data")
    list_tmp[[i_param]] <- resp_body }
  # remove objects
  rm(req, resp, resp_body)
}
# store tmp list if code breaks down the line ---------------------------------#
readr::write_rds(list_tmp, here::here(
  "data_out", "docs_cmt", "committee_docs_list.rds") )


###--------------------------------------------------------------------------###
### Process list ---------------------------------------------------------------
title_dcterms <- lapply(list_tmp, function(x) {
  if ( "title_dcterms" %in% names(x) ) {
    x |>
      dplyr::select(id, title_dcterms) |>
      tidyr::unnest(title_dcterms, names_sep = "_") |>
      dplyr::rename(title_en = title_dcterms_en) |>
      dplyr::select(-starts_with("title_dcterms"))
  } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)

dt_tmp <- lapply(X = list_tmp, FUN = function(x) {
  x |>
    dplyr::select(-title_dcterms) } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::select(-c(is_realized_by, publisher, originalLanguage)) |>
  tidyr::unnest(adopts, keep_empty = TRUE)

committee_docs_dt <- dt_tmp |>
  dplyr::select(dplyr::where(is.character) ) |>
  dplyr::left_join(
    y = title_dcterms,
    by = "id")
data.table::fwrite(x = committee_docs_dt,
                   file = here::here("data_out", "docs_cmt", "committee_docs_dt.csv"))

creator <- lapply(list_tmp, function(x) {
  if ( "creator" %in% names(x) ) {
    x |>
      dplyr::select(id, creator) |>
      tidyr::unnest(creator, names_sep = "_") } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::mutate(
    entity = ifelse(test = grepl(pattern = "person", x = creator),
                    yes = "person", no = NA),
    entity = ifelse(test = grepl(pattern = "org", x = creator),
                    yes = "org", no = entity),
    entity = ifelse(test = grepl(pattern = "corporate-body", x = creator),
                    yes = "org", no = entity),
    creator = gsub(
      pattern = "person/|org/|http://publications.europa.eu/resource/authority/corporate-body/EP_",
      replacement = "", x = creator)) |>
  tidyr::pivot_wider(names_from = entity, values_from = creator,
                     values_fn = ~paste(.x, collapse = "; "))

workHadParticipation <- lapply(list_tmp, function(x) {
  if ( "workHadParticipation" %in% names(x) ) {
    x |>
      dplyr::select(id, workHadParticipation) |>
      tidyr::unnest(workHadParticipation, names_sep = "_") } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::select(- workHadParticipation_type) |>
  tidyr::unnest(workHadParticipation_had_participant_person, keep_empty = TRUE)
