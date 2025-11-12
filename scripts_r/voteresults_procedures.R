###--------------------------------------------------------------------------###
# Grab additional info on votes based on the Procedures endpoint ---------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' This script is intended to run AFTER the `scripts_r/api_meetings_voteresults.R`.
#' It collects additional pieces of information regarding the Votes Results.
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
cat("\n=====\nStart of processing Procedures identified via Meetings/Vote_Results.\n=====\n")


#------------------------------------------------------------------------------#
# Load parallel API function --------------------------------------------------#
source(file = here::here("scripts_r", "parallel_api_calls.R"))

# Read data conditional on mandate --------------------------------------------#
if ( exists("today_date")) {
  votes_inverse_consists_of = data.table::fread(file = here::here(
    "data_out", "votes", "votes_inverse_consists_of_today.csv") )
} else if ( !exists("today_date")
            && mandate_starts == as.character("2019-07-01") ) {
  votes_inverse_consists_of = data.table::fread(file = here::here(
    "data_out", "votes", "votes_inverse_consists_of_all.csv") )
} else if ( !exists("today_date")
            && mandate_starts == as.character("2024-07-14") ) {
  votes_inverse_consists_of = data.table::fread(file = here::here(
    "data_out", "votes", "votes_inverse_consists_of_10.csv") ) }


#------------------------------------------------------------------------------#
## GET/procedures/{proc-id} ----------------------------------------------------
# get procedures IDs and build API URLs
process_ids <- sort(unique(gsub(pattern = "eli/dl/proc/", replacement = "",
                                x = (votes_inverse_consists_of$process_id))))

# Build URLs for procedure chunks
chunk_size <- 10L # Some of these procedures are heavy, use small chunks otherwise the call will fail due to timeout
process_ids_chunks <- split(x = process_ids, ceiling(seq_along(process_ids) / chunk_size))

# Build API URLs for each chunk
api_urls <- sapply(process_ids_chunks, function(chunk) {
  paste0(
    "https://data.europarl.europa.eu/api/v2/procedures/",
    paste0(chunk, collapse = ","),
    "?format=application%2Fld%2Bjson"
  )
})

# Use parallel API calls with conditional parallelization
use_parallel <- !exists("today_date") && length(api_urls) > 1

if (use_parallel) {
  cat("Multiple procedure API calls detected - using parallel processing\n")
  results <- parallel_api_calls(
    urls = api_urls,
    capacity = 495,
    fill_time_s = 60,
    timeout_s = 600,
    show_progress = TRUE,
    extract_data = TRUE
  )
} else {
  cat("Single API call or daily run detected - using sequential processing\n")
  results <- parallel_api_calls(
    urls = api_urls,
    capacity = 495,
    fill_time_s = 60,
    timeout_s = 600,
    show_progress = TRUE,
    extract_data = TRUE,
    force_sequential = TRUE
  )
}

list_tmp <- results$responses[!results$failed_calls]

if (length(list_tmp) == 0) {
  stop("\nWARNING: All API requests failed for procedures. Try again later.\n")
}


#------------------------------------------------------------------------------#
### Extract Committees ---------------------------------------------------------
# Extract data ----------------------------------------------------------------#
procedures_dt <- lapply(X = list_tmp,
                           FUN = data.table::as.data.table) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)


#------------------------------------------------------------------------------#
## Clean Data ------------------------------------------------------------------
### Extract Committees ---------------------------------------------------------
# Check that all cells are indeed DFs
row_idx = sapply(X = procedures_dt$had_participation, FUN = is.data.frame)
# Unnest
procedures_cmt = data.table::rbindlist(
  l = setNames(object = procedures_dt$had_participation[row_idx],
               nm = procedures_dt$id[row_idx]),
  use.names = TRUE, fill = TRUE, idcol = "process_id") |>
  dplyr::filter(
    participation_role %in% c("def/ep-roles/COMMITTEE_LEAD")
  ) |>
  dplyr::select(process_id,
                committee_lab = had_participant_organization) |>
  tidyr::unnest(committee_lab) |>
  dplyr::mutate(
    committee_lab = gsub(pattern = "org/", replacement = "",
                         x = committee_lab, fixed = TRUE)
  ) |>
  dplyr::distinct()

# Save to disk
if ( !exists("today_date")
     && mandate_starts == as.character("2019-07-01") ) {
  data.table::fwrite(x = procedures_cmt, file = here::here(
    "data_out", "procedures", "procedures_cmt_all.csv") )
} else if ( !exists("today_date")
            && mandate_starts == as.character("2024-07-14") ) {
  data.table::fwrite(x = procedures_cmt, file = here::here(
    "data_out", "procedures", "procedures_cmt_10.csv") ) }


#------------------------------------------------------------------------------#
### Extract Doc_IDs ------------------------------------------------------------
procedures_docid = data.table::rbindlist(
  l = setNames(object = procedures_dt$consists_of,
               nm = procedures_dt$id),
  use.names = TRUE, fill = TRUE, idcol = "process_id")
procedures_docid[, activity_date := as.Date(activity_date)]

# Unnest
procedures_docid = procedures_docid[
  had_activity_type == "def/ep-activities/TABLING_PLENARY",
  list(based_on_a_realization_of = as.character(unlist(based_on_a_realization_of))),
  by = list(process_id, activity_date)] |>
  unique()

#' WATCH OUT! You cannot filter out Amendments at this stage.
#' Indeed, many Resolutions only show up as Amendments.

# Create a DOC identifier just for non-AM
procedures_docid[, `:=`(
  identifier = gsub(pattern = "eli/dl/doc/|-AM-.*", replacement = "",
                    x = based_on_a_realization_of, perl = TRUE)
)]
# Subset cols & get rid of amendments
procedures_docid = unique(procedures_docid[, list(process_id, activity_date, identifier)])

#------------------------------------------------------------------------------#
#' Now the Amendments will be discarded as a consequence of taking the unique combinations of these columns.
#------------------------------------------------------------------------------#

# Fix docs' labels ------------------------------------------------------------#
# create temporary duplicate col for string processing
procedures_docid[, identifier2 := identifier]
# invert the orders of the groups for A-, B-, C- files
procedures_docid[
  grepl(pattern = "^[ABC]{1}.\\d{1,2}.", x = identifier2),
  doc_id := gsub(
    pattern = "(^[ABC]{1}.\\d{1,2}.)(\\d{4}).(\\d{4})",
    replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
# treat RC separately
procedures_docid[
  grepl(pattern = "^RC.\\d{1,2}", x = identifier2),
  doc_id := gsub(
    pattern = "(^RC.\\d{1,2}.)(\\d{4}).(\\d{4})",
    replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
procedures_docid[, doc_id := gsub(pattern = "^RC", replacement = "RC-B",
                                   x = doc_id, perl = T)]
# delete - between doc_id LETTER and MANDATE NUMBER
procedures_docid[, doc_id := gsub(pattern = "([A-Z]{1}).(\\d{1,2})",
                                   replacement = "\\1\\2", x = doc_id, perl = T)]
# slash at the end
procedures_docid[, doc_id := gsub(pattern = "(?<=.\\d{4}).",
                                   replacement = "/", x = doc_id, perl = T)]
# sapply(procedures_docid, function(x) sum(is.na(x))) # check

# Delete cols
procedures_docid[, identifier2 := NULL]

# Drop NAs in DOC ID
procedures_docid <- procedures_docid[!is.na(doc_id)]

# Save to disk
if ( !exists("today_date")
     && mandate_starts == as.character("2019-07-01") ) {
  data.table::fwrite(x = procedures_docid, file = here::here(
    "data_out", "procedures", "procedures_docid_all.csv") )
} else if ( !exists("today_date")
            && mandate_starts == as.character("2024-07-14") ) {
  data.table::fwrite(x = procedures_docid, file = here::here(
    "data_out", "procedures", "procedures_docid_10.csv") ) }

##----------------------------------------------------------------------------##
cat("\n=====\nEnd of processing Procedures identified via Meetings/Vote_Results.\n=====\n")
