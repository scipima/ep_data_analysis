###--------------------------------------------------------------------------###
# EP Procedures ----------------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' The script grabs all procedural identifiers from the EP API.
#' REF: https://data.europarl.europa.eu/en/home; https://data.europarl.europa.eu/en/developer-corner/opendata-api
#------------------------------------------------------------------------------#

# rm(list = ls())

###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c("curl", "data.table", "dplyr", "here", "httr2", "lubridate",
                        "janitor", "jsonlite", "readr", "stringi", "tidyr",
                        "tidyselect") )

# Load parallel API function --------------------------------------------------#
source(file = here::here("scripts_r", "parallel_api_calls.R"))

# Hard code the start of the mandate ------------------------------------------#
if ( !exists("mandate_starts") ) {
  mandate_starts <- as.Date("2024-07-14") }


###--------------------------------------------------------------------------###
## GET/procedures --------------------------------------------------------------
# Returns the list of all EP Procedures
# EXAMPLE: https://data.europarl.europa.eu/api/v2/procedures?format=application%2Fld%2Bjson&offset=0&limit=50

# Vector of procedure types
process_types = c("ACI", "APP", "AVC", "BUD", "CNS", "COD", "DEC", "NLE", "SYN",
                  "BUI", "COS", "DEA", "DCE", "IMM", "INI", "INL", "INS", "REG",
                  "RPS", "RSO", "RSP")

# Empty repository
list_tmp = vector(mode = "list", length = length(process_types))

for (i_process in seq_along(process_types)) {
  print(process_types[i_process]) # check
  req <- httr2::request(base_url = paste0(
    "https://data.europarl.europa.eu/api/v2/procedures?process-type=",
    process_types[i_process],
    "&format=application%2Fld%2Bjson&offset=0") )

  # Add time-out and ignore error
  resp <- req |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
    httr2::req_error(is_error = ~FALSE) |>
    httr2::req_throttle(30 / 60) |> # politely, 1 every 2 seconds
    httr2::req_perform()

  # If not an error, download and make available in ENV
  if ( httr2::resp_status(resp) == 200L) {
    resp_body <- resp |>
      httr2::resp_body_json( simplifyDataFrame = TRUE )
    list_tmp[[i_process]] = resp_body$data }
}

names(list_tmp) <- process_types
procedures_df = data.table::rbindlist(l = list_tmp,
                                      use.names = TRUE, fill = TRUE,
                                      idcol = "process_type")

procedures_df[, .N, by = id][order(N)]

# check
# sapply(procedures_df, function(x) sum(is.na(x)))

# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = procedures_df,
                   file = here::here("data_out", "procedures", "procedures_df.csv") )

# Remove API objects ----------------------------------------------------------#
rm(i_process, list_tmp, req, resp, resp_body)


#------------------------------------------------------------------------------#
## GET/procedures/{proc-id} ----------------------------------------------------
# Returns a single EP Procedure for the specified proc-ID
# EXAMPLE: https://data.europarl.europa.eu/api/v2/procedures/2018-0218?format=application%2Fld%2Bjson

# get procedures IDs
process_ids <- sort(unique(procedures_df$process_id))

# Split the vector in chunks of size 50 each
chunk_size <- 50L
process_ids_chunks <- split(
  x = process_ids, ceiling(seq_along(process_ids) / chunk_size)
)

# Build API URLs for batched procedure calls
api_urls <- sapply(
  X = process_ids_chunks,
  FUN = function(chunk) {
    paste0(
      "https://data.europarl.europa.eu/api/v2/procedures/",
      paste0(chunk, collapse = ","),
      "?format=application%2Fld%2Bjson"
    )
  }
)

# Use parallel processing for multiple calls
use_parallel <- length(api_urls) > 1

if (use_parallel) {
  cat("Multiple procedure API calls detected - using parallel processing\n")
  cat("Total procedures:", length(process_ids), "| Batched into", length(api_urls), "API calls\n")
  results <- parallel_api_calls(
    urls = api_urls,
    capacity = 220,  # Conservative rate: ~55 calls per minute
    fill_time_s = 60,
    show_progress = TRUE,
    extract_data = TRUE
  )
  list_tmp <- results$responses
} else {
  cat("Single API call detected - using sequential processing\n")
  results <- parallel_api_calls(
    urls = api_urls,
    capacity = 220,
    fill_time_s = 60,
    show_progress = FALSE,
    extract_data = TRUE,
    force_sequential = TRUE
  )
  list_tmp <- results$responses
}

# store tmp list if code breaks down the line
readr::write_rds(x = list_tmp, file = here::here(
  "data_out", "procedures", "procedures_list.rds") )
# list_tmp <- readr::read_rds(file = here::here(
#   "data_out", "procedures", "procedures_list.rds") )
# list_tmp <- readr::read_rds(file = here::here(
#   "data_out", "procedures", "process_ids_dt.rds") )


process_stage <- lapply(
  X = list_tmp,
  FUN = function(i_process) {
    i_process[, c("process_id", "current_stage")]
  }
) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)



# #------------------------------------------------------------------------------#
# ### Clean list -----------------------------------------------------------------
# # procedures_consists_of ------------------------------------------------------#
# procedures_consists_of <- lapply(
#   X = list_tmp, FUN = function(x) {
#     if ( "consists_of" %in% names(x)) {
#       x |>
#         dplyr::select(id, consists_of) |>
#         tidyr::unnest(cols = consists_of, keep_empty = TRUE, names_sep = "_") } } ) |>
#   data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
#   tidyr::unnest(consists_of_decided_on_a_realization_of, keep_empty = TRUE) |>
#   tidyr::unnest(consists_of_based_on_a_realization_of, keep_empty = TRUE) |>
#   tidyr::unnest(consists_of_recorded_in_a_realization_of, keep_empty = TRUE) |>
#   data.table::as.data.table()
# data.table::fwrite(x = procedures_consists_of,
#                    here::here("data_out", "procedures", "procedures_consists_of.csv"))
# # length(unique(procedures_consists_of$id))


# # procedures_involved_work ----------------------------------------------------#
# procedures_involved_work <- lapply(
#   X = list_tmp, FUN = function(x) {
#     if ( "involved_work" %in% names(x)) {
#       x |>
#         dplyr::select(id, involved_work) |>
#         tidyr::unnest(cols = involved_work) } } ) |>
#   data.table::rbindlist(use.names = TRUE, fill = TRUE)
# data.table::fwrite(x = procedures_involved_work, here::here(
#   "data_out", "procedures", "procedures_involved_work.csv"))


# procedures_created_a_realization_of -----------------------------------------#
procedures_created_a_realization_of <- lapply(
  X = list_tmp, FUN = function(x) {
    if ( "created_a_realization_of" %in% names(x)) {
      x |>
        dplyr::select(id, created_a_realization_of) |>
        tidyr::unnest(cols = created_a_realization_of) } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)
# length(unique(procedures_created_a_realization_of$id)) # check

# Clean string
procedures_created_a_realization_of[, created_a_realization_of := gsub(
  pattern = "eli/dl/doc/", replacement = "", x = created_a_realization_of) ]
procedures_created_a_realization_of[
  grepl(pattern = "^A.\\d{1,2}.\\d{4}-\\d{4}|^B.\\d{1,2}.\\d{4}-\\d{4}|^RC.\\d{1,2}.\\d{4}-\\d{4}",
        x = created_a_realization_of),
  doc_type := "plenary_doc"
]
procedures_created_a_realization_of[
  grepl(pattern = "^TA.\\d{1,2}.\\d{4}-\\d{4}",
        x = created_a_realization_of),
  doc_type := "adopted_doc"
]
procedures_created_a_realization_of[
  grepl(pattern = "^C.\\d{1,2}.\\d{4}-\\d{4}",
        x = created_a_realization_of),
  doc_type := "procedural_doc"
]
procedures_created_a_realization_of[
  grepl(pattern = "^[A-Z]{4}.[A-Z]{2}.\\d{6}|^[A-Z]{2}\\d{2}.[A-Z]{2}.\\d{6}",
        x = created_a_realization_of),
  doc_type := "committee_doc"
]
procedures_created_a_realization_of[
  grepl(pattern = "^SP.\\d{4}",
        x = created_a_realization_of),
  doc_type := "commission_response"
]

# Save data to disk
data.table::fwrite(x = procedures_created_a_realization_of, here::here(
  "data_out", "procedures", "procedures_created_a_realization_of.csv") )


#------------------------------------------------------------------------------#
### process_title --------------------------------------------------------------
procedures_title = lapply(
  X = list_tmp,
  FUN = function(x){
    if ("process_title" %in% names(x)) {
      df_tmp = x[, c("id", "process_title")]
      if ("en" %in% names(df_tmp$process_title)){
        df_tmp$process_title_en = df_tmp$process_title$en
      }
      # if ("fr" %in% names(df_tmp$process_title)){
      #   df_tmp$process_title_fr = df_tmp$process_title$fr
      # }
      df_tmp$process_title = NULL
      return( df_tmp )
    }
  }) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)

# Checks for buggy data
procedures_title[
  which(sapply(procedures_title$process_title_en, length) > 1)
  ]$process_title_en
# procedures_title[
#   which(sapply(procedures_title$process_title_fr, length) > 1)
#   ]$process_title_fr
rows_todrop = which(sapply(procedures_title$process_title_en, length) > 1)

procedures_title = procedures_title[, list(
 process_title_en = unlist(process_title_en)
 ),
 by = list(id)
]

# Save data to disk
data.table::fwrite(x = procedures_title[-rows_todrop], here::here(
  "data_out", "procedures", "procedures_title.csv") )


# procedures_scheduledIn <- lapply(
#   X = list_tmp, FUN = function(x) {
#     if ( "scheduledIn" %in% names(x)) {
#       x |>
#         dplyr::select(id, scheduledIn)
#     } } ) |>
#   data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
#   tidyr::unnest(cols = scheduledIn, keep_empty = TRUE, names_sep = "_") |>
#   tidyr::unnest(cols = scheduledIn_headingLabel, keep_empty = TRUE, names_sep = "_") |>
#   dplyr::rename(scheduledIn_label_en = scheduledIn_headingLabel_en,
#                 scheduledIn_label_fr = scheduledIn_headingLabel_fr) |>
#   dplyr::select(-dplyr::starts_with("scheduledIn_headingLabel"))
# data.table::fwrite(x = procedures_scheduledIn, here::here(
#   "data_out", "procedures", "procedures_scheduledIn.csv"))


# # append list
# process_ids_dt <- lapply(
#   X = list_tmp, FUN = function(x) {
#     x[, names(x) %in% c("id", "", "current_stage", "process_id", "label",
#                         "process_type", "identifierYear") ] |>
#       tidyr::unnest(cols = c(process_type, label) ) } ) |>
#   data.table::rbindlist(use.names = TRUE, fill = TRUE)
# data.table::fwrite(x = process_ids_dt,
#                    here::here("data_out", "procedures", "process_ids_dt.csv"))

# procedure_stage_dict <- data.table::fread(
#   here::here("data_out", "procedures", "procedure_stage_dict.csv"))

# process_ids_dt |>
#   dplyr::select(id, current_stage, process_id, label) |>
#   dplyr::mutate(current_stage = gsub(
#     pattern = "http://publications.europa.eu/resource/authority/procedure-phase/",
#     replacement = "", x = current_stage) ) |>
#   # merge with stage dictionary -----------------------------------------------#
#   dplyr::left_join(
#     y = procedure_stage_dict,
#     by = c("current_stage" = "procedure_phase") ) |>
#   # merge with docs associated with procedure ---------------------------------#
#   dplyr::left_join(
#     y = procedures_created_a_realization_of,
#     by = "id")

