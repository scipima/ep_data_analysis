###--------------------------------------------------------------------------###
# EP Plenary Session Documents IDs ---------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' The script grabs all Plenary Sessions Documents IDs from the EP API.
#' Plenary Part-Session Date: This parameter refers to the date of the first day of a given part-session.
#' Parliament sits monthly in Strasbourg in a four-day part-session (Monday to Thursday).
#' Additional part-sessions are held in Brussels.
#' The format of the value is YYYY-MM-DD.
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
packages <- c("data.table", "dplyr", "here", "httr2", "janitor",
              "jsonlite", "tidyr", "xml2")
if ( any( !packages %in% pacman::p_loaded() ) ) {
  pacman::p_load(char = packages[ !packages %in% pacman::p_loaded() ] ) }

source(file = here::here("scripts_r", "repo_setup.R") )
source(file = here::here("scripts_r", "parallel_api_calls.R"))


#------------------------------------------------------------------------------#
## GET/plenary-session-documents -----------------------------------------------
# Save to disk conditional on mandate -----------------------------------------#

if ( !exists("today_date") && mandate_starts == "2019-07-01" ) {
  get_api_data(
    path = here::here("data_out", "docs_pl", "pl_session_docs_all.csv"),
    script = here::here("scripts_r", "api_pl_session_docs.R"),
    varname = "pl_session_docs",
  )
} else if (!exists("today_date") && mandate_starts == "2024-07-14" ) {
  get_api_data(
    path = here::here("data_out", "docs_pl", "pl_session_docs_10.csv"),
    script = here::here("scripts_r", "api_pl_session_docs.R"),
    varname = "pl_session_docs",
  )
} else (
  stop("You need to specify a valid timeframe. This could be either the starting date for a mandate (currently: '2019-07-01', '2024-07-14'), or today's date")
)

# remove objects (no longer needed with parallel function)


#------------------------------------------------------------------------------#
## RCV -------------------------------------------------------------------------
# Returns a single EP Plenary Session document for the specified doc ID

# EXAMPLE: https://data.europarl.europa.eu/api/v2/plenary-session-documents/PV-9-2021-02-10-RCV?format=application%2Fld%2Bjson&language=fr

### Loop ----------------------------------------------------------------------#
# RCV ids
rcv_ids <- unique( pl_session_docs$identifier[
  grepl(pattern = "RCV", x = pl_session_docs$identifier)
] )

# Build URLs for RCV documents in chunks of 45
chunk_size <- 45L
ids_chunks <- split(x = rcv_ids, ceiling(seq_along(rcv_ids) / chunk_size))

# Build API URLs for each chunk
api_urls <- sapply(ids_chunks, function(chunk) {
  paste0(
    "https://data.europarl.europa.eu/api/v2/plenary-session-documents/",
    paste0(chunk, collapse = ","),
    "?format=application%2Fld%2Bjson&language=fr"
  )
})

# Use parallel API calls
use_parallel <- length(api_urls) > 1
if (use_parallel) {
  cat("Multiple RCV API calls detected - using parallel processing\n")
  results <- parallel_api_calls(
    urls = api_urls,
    capacity = 495,
    fill_time_s = 300,
    show_progress = TRUE,
    extract_data = TRUE
  )
  list_tmp <- results$responses[!results$failed_calls]
} else {
  cat("Single or no RCV API calls - using sequential processing\n")
  results <- parallel_api_calls(
    urls = api_urls,
    show_progress = FALSE,
    extract_data = TRUE
  )
  list_tmp <- results$responses[!results$failed_calls]
}

if (length(list_tmp) == 0) {
  stop("\nWARNING: All API requests failed for Plenary Session Documents: RCV.\n")
}

# Append list -----------------------------------------------------------------#
pl_session_docs_rcv <- data.table::rbindlist(
  l = list_tmp, use.names = TRUE, fill = TRUE
)

### Unnest list items ---------------------------------------------------------#
# Treat Titles separately -----------------------------------------------------#
#' This will give us the strings attached to the RCVs.
#' For instance: "Ordre du jour de mercredi - Demande du groupe PfE"; "RC-B10-0459/2025 - Proposition de résolution (ensemble du texte)"; "B10-0007/2024 - Après le § 1 - Am 1"

# Loop
title_dcterms = vector(mode = "list",
                       length = length(pl_session_docs_rcv$inverse_is_part_of)
)
for (i_title in seq_along(pl_session_docs_rcv$inverse_is_part_of) ) {
  dt_tmp = pl_session_docs_rcv$inverse_is_part_of[[i_title]]
  if ("title_dcterms" %in% names(dt_tmp) ) {
    dt_tmp = dt_tmp[, c("id" , "title_dcterms")]
    dt_tmp$title_dcterms = unlist(dt_tmp$title_dcterms, use.names = FALSE)
    title_dcterms[[i_title]] = dt_tmp
  }
  rm(dt_tmp)
}
# Append
title_dcterms = data.table::rbindlist(l = title_dcterms,
                                      use.names = TRUE, fill = TRUE)
# Rename cols
data.table::setnames(x = title_dcterms,
                     old = c("id", "title_dcterms"),
                     new = c("doc_rcv_itm_id", "title_dcterms_fr"))

# Get reference to other documents --------------------------------------------#
inverse_is_part_of = data.table::rbindlist(
  l = pl_session_docs_rcv$inverse_is_part_of,
  use.names = TRUE, fill = TRUE
)
inverse_is_part_of[, title_dcterms := NULL] # drop the col we extract above

# Unnest DEC ID
inverse_recorded_in_a_realization_of = inverse_is_part_of[, list(
  inverse_recorded_in_a_realization_of = as.character( unlist(
    inverse_recorded_in_a_realization_of
  ) ) ),
  by = list(id)]
# Rename
data.table::setnames(x = inverse_recorded_in_a_realization_of,
                     old = c("id", "inverse_recorded_in_a_realization_of"),
                     new = c("doc_rcv_itm_id", "event_dec_itm_id"))

# Subset ----------------------------------------------------------------------#
pl_session_docs_rcv = pl_session_docs_rcv[, list(
  doc_rcv_id = id, document_date, epNumber, parliamentary_term) ]
inverse_is_part_of = inverse_is_part_of[, list(doc_rcv_itm_id = id,
                                               doc_rcv_id = is_part_of,
                                               rcv_itm_number = number)]

# Merge -----------------------------------------------------------------------#
pl_session_docs_rcv = inverse_is_part_of[
  pl_session_docs_rcv,
  on = "doc_rcv_id"
]

pl_session_docs_rcv = inverse_recorded_in_a_realization_of[
  pl_session_docs_rcv,
  on = "doc_rcv_itm_id"
]
pl_session_docs_rcv = title_dcterms[
  pl_session_docs_rcv,
  on = "doc_rcv_itm_id"
]
# Reorder DT cols
data.table::setcolorder(x = pl_session_docs_rcv,
                        neworder = c("doc_rcv_id", "parliamentary_term",
                                     "document_date", "epNumber",
                                     "doc_rcv_itm_id", "rcv_itm_number",
                                     "event_dec_itm_id"))


# Save to disk conditional on mandate -----------------------------------------#
if (mandate_starts == as.Date("2019-07-01")) {
  data.table::fwrite(x = pl_session_docs_rcv, file = here::here(
    "data_out", "rcv", "pl_session_docs_rcv_all.csv"))
} else {
  data.table::fwrite(x = pl_session_docs_rcv, file = here::here(
    "data_out", "rcv", "pl_session_docs_rcv_10.csv")) }

# Remove objects
rm(inverse_is_part_of, inverse_recorded_in_a_realization_of, list_tmp,
   title_dcterms)


#------------------------------------------------------------------------------#
## VOTES -----------------------------------------------------------------------
# Returns a single EP Plenary Session document for the specified doc ID

# EXAMPLE: https://data.europarl.europa.eu/api/v2/plenary-session-documents/PV-9-2021-02-10-RCV?format=application%2Fld%2Bjson&language=fr

### Loop ----------------------------------------------------------------------#
# Votes ids
votes_ids <- sort( unique( pl_session_docs$identifier[
  grepl(pattern = "VOT", x = pl_session_docs$identifier)
] ) )

# Build URLs for VOTES documents in chunks of 45
chunk_size <- 45L
ids_chunks <- split(x = votes_ids, ceiling(seq_along(votes_ids) / chunk_size))

# Build API URLs for each chunk
api_urls <- sapply(ids_chunks, function(chunk) {
  paste0(
    "https://data.europarl.europa.eu/api/v2/plenary-session-documents/",
    paste0(chunk, collapse = ","),
    "?format=application%2Fld%2Bjson&language=fr"
  )
})

# Use parallel API calls
use_parallel <- length(api_urls) > 1
if (use_parallel) {
  cat("Multiple VOTES API calls detected - using parallel processing\n")
  results <- parallel_api_calls(
    urls = api_urls,
    capacity = 495,
    fill_time_s = 300,
    show_progress = TRUE,
    extract_data = TRUE
  )
  list_tmp <- results$responses[!results$failed_calls]
} else {
  cat("Single or no VOTES API calls - using sequential processing\n")
  results <- parallel_api_calls(
    urls = api_urls,
    show_progress = FALSE,
    extract_data = TRUE
  )
  list_tmp <- results$responses[!results$failed_calls]
}

if (length(list_tmp) == 0) {
  stop("\nWARNING: All API requests failed for Plenary Session Documents: Votes.\n")
}

# Append list -----------------------------------------------------------------#
pl_session_docs_votes = data.table::rbindlist(
  l = list_tmp, use.names = TRUE, fill = TRUE
)

# Unnest list items -----------------------------------------------------------#
# treat titles separately
title_dcterms = vector(
  mode = "list",
  length = length(pl_session_docs_votes$inverse_is_part_of)
)
for (i_title in seq_along(pl_session_docs_votes$inverse_is_part_of) ) {
  dt_tmp = pl_session_docs_votes$inverse_is_part_of[[i_title]]
  if ("title_dcterms" %in% names(dt_tmp) ) {
    dt_tmp = dt_tmp[, c("id" , "title_dcterms")]
    dt_tmp$title_dcterms = unlist(dt_tmp$title_dcterms, use.names = FALSE)
    title_dcterms[[i_title]] = dt_tmp
  }
  rm(dt_tmp)
}
title_dcterms = data.table::rbindlist(l = title_dcterms,
                                      use.names = TRUE, fill = TRUE)
data.table::setnames(x = title_dcterms,
                     old = c("id", "title_dcterms"),
                     new = c("doc_vot_itm_id", "title_dcterms_fr"))

# Get reference to other documents
inverse_is_part_of = data.table::rbindlist(
  l = pl_session_docs_votes$inverse_is_part_of, use.names = TRUE, fill = TRUE
)
inverse_is_part_of[, title_dcterms := NULL]

inverse_refers_to = inverse_is_part_of[, list(
  doc_pl_itm_id = as.character( unlist(inverse_refers_to) ) ),
  by = list(doc_vot_itm_id = id)]

inverse_recorded_in_a_realization_of = inverse_is_part_of[, list(
  inverse_recorded_in_a_realization_of = as.character( unlist(
    inverse_recorded_in_a_realization_of) ) ),
  by = list(doc_vot_itm_id = id)]
data.table::setnames(x = inverse_recorded_in_a_realization_of,
                     old = c("inverse_recorded_in_a_realization_of"),
                     new = c("event_vot_itm_id") )

inverse_is_part_of = inverse_is_part_of[, list(doc_vot_itm_id = id,
                                               vot_itm_number = number,
                                               doc_vot_id = is_part_of)]
pl_session_docs_votes = pl_session_docs_votes[, list(
  doc_vot_id = id, parliamentary_term, document_date, epNumber)]

# Merge
pl_session_docs_votes = inverse_is_part_of[
  pl_session_docs_votes, on = "doc_vot_id"]

pl_session_docs_votes = inverse_refers_to[
  pl_session_docs_votes, on = "doc_vot_itm_id"]

pl_session_docs_votes = inverse_recorded_in_a_realization_of[
  pl_session_docs_votes, on = "doc_vot_itm_id"]

pl_session_docs_votes = title_dcterms[
  pl_session_docs_votes, on = "doc_vot_itm_id"
]

# Reorder DT cols
data.table::setcolorder(x = pl_session_docs_votes,
                        neworder = c("doc_vot_id", "parliamentary_term",
                                     "document_date", "epNumber",
                                     "doc_pl_itm_id",  "doc_vot_itm_id",
                                     "vot_itm_number", "event_vot_itm_id",
                                     "title_dcterms_fr"))

# Save to disk conditional on mandate -----------------------------------------#
if (mandate_starts == as.Date("2019-07-01")) {
  data.table::fwrite(x = pl_session_docs_votes, file = here::here(
    "data_out", "votes", "pl_session_docs_votes_all.csv"))
} else {
  data.table::fwrite(x = pl_session_docs_votes, file = here::here(
    "data_out", "votes", "pl_session_docs_votes_10.csv")) }

# Remove objects
rm(inverse_is_part_of, inverse_recorded_in_a_realization_of, inverse_refers_to,
   list_tmp, title_dcterms)

#------------------------------------------------------------------------------#
# Get list of final votes
source(file = here::here("scripts_r", "get_final_votes.R"))

#------------------------------------------------------------------------------#
# Clean up before exiting -----------------------------------------------------#
# Run time
script_ends <- Sys.time()
script_lapsed = script_ends - script_starts
cat("\nThe script run in:", script_lapsed, "\n")
