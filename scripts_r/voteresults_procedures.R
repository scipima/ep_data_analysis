###--------------------------------------------------------------------------###
# Grab additional info on votes based on the Procedures endpoint ---------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' This script is intended to be run AFTER the `scripts_r/api_meetings_voteresults.R`.
#' It collects additional pieces of information regarding the Votes  Results.
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
chunk_size <- 50L
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
    capacity = 220, # Conservative rate: ~55 calls per minute (matching original)
    fill_time_s = 60,
    show_progress = TRUE,
    extract_data = TRUE
  )
} else {
  cat("Single API call or daily run detected - using sequential processing\n")
  results <- parallel_api_calls(
    urls = api_urls,
    capacity = 220,
    fill_time_s = 60,
    show_progress = FALSE,
    extract_data = TRUE
  )
}

list_tmp <- results$responses[!results$failed_calls]

if (length(list_tmp) == 0) {
  stop("\nWARNING: All API requests failed for procedures. Try again later.\n")
}


#------------------------------------------------------------------------------#
### Extract Committees ---------------------------------------------------------
procedures_cmt = vector(mode = "list", length = length(list_tmp))
for (i_df in seq_along(list_tmp) ) {
    print(i_df)
    df_tmp = list_tmp[[i_df]]
    if ( "had_participation" %in% names(df_tmp)) {
    df_tmp = df_tmp[sapply(X = df_tmp$had_participation, FUN = is.data.frame), ]
    procedures_cmt[[i_df]] = data.table::rbindlist(
        l = setNames(object = df_tmp$had_participation,
                     nm = df_tmp$id),
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
        dplyr::arrange(process_id)
    }
    rm(df_tmp)
}

# Append
procedures_cmt = data.table::rbindlist(l = procedures_cmt,
                                       use.names = TRUE, fill = TRUE) |>
    dplyr::distinct() |>
    dplyr::arrange(process_id)

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
# procedures_docid_tmp = data.table::rbindlist(
#     l = setNames(object = votes_procedures$consists_of,
#                  nm = votes_procedures$id),
#     use.names = TRUE, fill = TRUE, idcol = "process_id") |>
#     dplyr::mutate(activity_date = as.Date(activity_date)) |>
#     dplyr::filter(
#         had_activity_type == "def/ep-activities/TABLING_PLENARY"
#     ) |>
#     dplyr::select(process_id, activity_date, based_on_a_realization_of) |>
#     tidyr::unnest(based_on_a_realization_of, names_sep = "_") |>
#     dplyr::mutate(identifier = ifelse(
#         test = !grepl(pattern = "-AM-",
#                       x = based_on_a_realization_of,
#                       fixed = TRUE),
#         yes = gsub(pattern = "eli/dl/doc/", replacement = "",
#                    x = based_on_a_realization_of),
#         no = NA
#     )) |>
#     data.table::as.data.table()

# based_on_a_realization_of <- votes_foreseen |>
#     dplyr::select(inverse_was_scheduled_in, based_on_a_realization_of) |>
#     tidyr::unnest_longer(
#         col = c(inverse_was_scheduled_in, based_on_a_realization_of),
#         indices_include = TRUE) |>
#     dplyr::mutate(is_c_doc = ifelse(
#         test = grepl(pattern = "/C-", x = based_on_a_realization_of, fixed = TRUE),
#         yes = 1L, no = 0L),
#         is_multiple = ifelse(
#             test = max(based_on_a_realization_of_id) > 1L,
#             yes = 1L, no = 0L),
#         .by = inverse_was_scheduled_in) |>
#     dplyr::filter(
#         ! (is_c_doc == 1L & is_multiple == 1L)
#         & !grepl(pattern = "O-\\d{1,2}-", x = based_on_a_realization_of, perl = TRUE)
#     ) |>
#     dplyr::select(inverse_was_scheduled_in, based_on_a_realization_of)


# DEFENSIVE: Last minute data, or URGENT PROCEDURES, may not be fully populated.
# Thus, we import part of the data from other sources.
# procedures_doc_id <- procedures_docid_tmp |>
#     dplyr::full_join(
#         y = based_on_a_realization_of,
#         by = c("process_id" = "inverse_was_scheduled_in")
#     ) |>
#     dplyr::mutate(identifier = ifelse(
#         test = is.na(identifier),
#         yes = gsub(pattern = "eli/dl/doc/", replacement = "",
#                    x = based_on_a_realization_of.y),
#         no = identifier)
#     ) |>
#     data.table::as.data.table()


# Fix docs' labels ------------------------------------------------------------#
# create temporary duplicate col for string processing
# procedures_doc_id[, identifier2 := identifier]
# # invert the orders of the groups for A-, B-, C- files
# procedures_doc_id[
#     grepl(pattern = "^[ABC]{1}.\\d{1,2}.", x = identifier2),
#     doc_id := gsub(
#         pattern = "(^[ABC]{1}.\\d{1,2}.)(\\d{4}).(\\d{4})",
#         replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
# # treat RC separately
# procedures_doc_id[
#     grepl(pattern = "^RC.\\d{1,2}", x = identifier2),
#     doc_id := gsub(
#         pattern = "(^RC.\\d{1,2}.)(\\d{4}).(\\d{4})",
#         replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
# procedures_doc_id[, doc_id := gsub(pattern = "^RC", replacement = "RC-B",
#                                    x = doc_id, perl = T)]
# # delete - between doc_id LETTER and MANDATE NUMBER
# procedures_doc_id[, doc_id := gsub(pattern = "([A-Z]{1}).(\\d{1,2})",
#                                    replacement = "\\1\\2", x = doc_id, perl = T)]
# # slash at the end
# procedures_doc_id[, doc_id := gsub(pattern = "(?<=.\\d{4}).",
#                                    replacement = "/", x = doc_id, perl = T)]
# # Delete cols
# procedures_doc_id[, c("based_on_a_realization_of.x", "based_on_a_realization_of.y",
#                       "identifier", "identifier2") := NULL]
# # sapply(procedures_doc_id, function(x) sum(is.na(x))) # check

# Clean and filter data
# procedures_doc_id <- procedures_doc_id |>
#     dplyr::filter(!is.na(doc_id)) |>
#     dplyr::mutate(nchar_doc = nchar(doc_id)) |>
#     dplyr::arrange(process_id, -nchar_doc) |>
#     dplyr::select(-c(nchar_doc, activity_date) ) |>
#     dplyr::distinct() |>
#     tidyr::nest(doc_id = doc_id)
