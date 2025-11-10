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


# Hard code the start of the mandate ----------------------------------------#
if ( !exists("mandate_starts") ) {
  mandate_starts <- as.Date("2024-07-14") }


###--------------------------------------------------------------------------###
## GET/parliamentary-questions -------------------------------------------------
# Returns the list of all EP Questions
# EXAMPLE: https://data.europarl.europa.eu/api/v2/parliamentary-questions?year=2022,2019&format=application%2Fld%2Bjson&offset=0&limit=50

# Vector of procedure types
years = 2019 : data.table::year(Sys.Date())

### QUESTION_WRITTEN_PRIORITY --------------------------------------------------
req <- httr2::request(base_url = paste0(
  "https://data.europarl.europa.eu/api/v2/parliamentary-questions?year=",
  paste0(years, collapse = ","),
  "&work-type=QUESTION_WRITTEN_PRIORITY&format=application%2Fld%2Bjson&offset=0") ) |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
  httr2::req_error(is_error = ~FALSE) |>
  httr2::req_throttle(30 / 60) |> # politely, 1 every 2 seconds
  httr2::req_perform()

# If not an error, download and make available in ENV
parl_questions_priority <- req |>
  httr2::resp_body_json( simplifyDataFrame = TRUE ) |>
  purrr::pluck("data")
# check
# sapply(procedures_df, function(x) sum(is.na(x)))
# Remove API objects ----------------------------------------------------------#
rm(req)


## QUESTION_WRITTEN ------------------------------------------------------------
req <- httr2::request(base_url = paste0(
  "https://data.europarl.europa.eu/api/v2/parliamentary-questions?year=",
  paste0(years, collapse = ","),
  "&work-type=QUESTION_WRITTEN&format=application%2Fld%2Bjson&offset=0") ) |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
  httr2::req_error(is_error = ~FALSE) |>
  httr2::req_throttle(30 / 60) |> # politely, 1 every 2 seconds
  httr2::req_perform()

# If not an error, download and make available in ENV
parl_questions_no_priority <- req |>
  httr2::resp_body_json( simplifyDataFrame = TRUE ) |>
  purrr::pluck("data")

parl_questions <- data.table::rbindlist(
  l = list(parl_questions_priority, parl_questions_no_priority)
)

# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = parl_questions, file = here::here(
  "data_out", "parl_questions", "parl_questions.csv") )

# Remove API objects ----------------------------------------------------------#
rm(req, resp, years)

# Check
parl_questions_csv = data.table::rbindlist(
  l = lapply(
    X = c(
      "https://data.europarl.europa.eu/distribution/parliamentary-questions_2025_19_en.csv",
      "https://data.europarl.europa.eu/distribution/parliamentary-questions_2024_51_en.csv",
      "https://data.europarl.europa.eu/distribution/parliamentary-questions_2023_56_en.csv",
      "https://data.europarl.europa.eu/distribution/parliamentary-questions_2022_9_en.csv",
      "https://data.europarl.europa.eu/distribution/parliamentary-questions_2021_4_en.csv",
      "https://data.europarl.europa.eu/distribution/parliamentary-questions_2020_5_en.csv",
      "https://data.europarl.europa.eu/distribution/parliamentary-questions_2019_4_en.csv"
    ),
    FUN = data.table::fread),
    use.names = TRUE, fill = TRUE
)
sapply(X = parl_questions_csv, FUN = function(x) sum(is.na(x)))
data.table::fwrite(x = parl_questions_csv, file = here::here(
  "data_out", "parl_questions", "parl_questions_csv.csv"))


#------------------------------------------------------------------------------#
## GET/procedures/{proc-id} ----------------------------------------------------
# Returns a single EP Procedure for the specified proc-ID
# EXAMPLE: https://data.europarl.europa.eu/api/v2/parliamentary-questions/E-9-2022-000342?format=application%2Fld%2Bjson

# get procedures IDs
# question_ids <- sort(unique(parl_questions_priority$identifier))
question_ids <- sort(unique(parl_questions$identifier))

# Split the vector in chunks of size 50 each
chunk_size <- 50L
question_ids_chunks <- split(
  x = question_ids, ceiling(seq_along(question_ids) / chunk_size)
)

# Empty repository
list_tmp <- vector(mode = "list", length = length(question_ids_chunks))

# Loop
for (i_chunk in seq_along(question_ids_chunks) ) {
  print(i_chunk) # check

  # Create an API request
  req <- httr2::request("https://data.europarl.europa.eu/api/v2") |>
    httr2::req_url_path_append("parliamentary-questions") |>
    httr2::req_url_path_append(
      paste0(question_ids_chunks[[i_chunk]], collapse = ",")
    ) |>
    httr2::req_url_path_append("?format=application%2Fld%2Bjson")

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
    list_tmp[[i_chunk]] = resp_body$data }
}

# store tmp list if code breaks down the line
readr::write_rds(x = list_tmp, file = here::here(
  "data_out", "parl_questions", "parl_questions_list.rds") )


### inverse_answers_to ---------------------------------------------------------
parl_question_inverse_answers_to <- lapply(
  X = list_tmp,
  FUN = function(i_question) {
    if ( "inverse_answers_to" %in% names(i_question) ) {
      i_question[, c("id", "inverse_answers_to")] #
    }
  }
) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)

document_date_answer <- lapply(
  X = parl_question_inverse_answers_to$inverse_answers_to,
  FUN = function(i_answer) {
    if ("document_date" %in% names(i_answer))
    i_answer[, c("id", "answers_to", "document_date")]
  }) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)
document_date_answer = document_date_answer[, lapply(
  X = .SD, FUN = unlist),
  by = list(id, document_date),
  .SDcols = c("answers_to")]

# parl_question_inverse_answers_to_unnest

document_date_answer_creator <- lapply(
  X = parl_question_inverse_answers_to$inverse_answers_to,
  FUN = function(i_answer) {
    if ("document_date" %in% names(i_answer))
      i_answer[, c("id", "document_date")]
  }) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)

parl_question_inverse_answers_to <- parl_question_inverse_answers_to |>
  tidyr::unnest_wider(inverse_answers_to, names_sep = "_") |>
  dplyr::select(id, inverse_answers_to_id, inverse_answers_to_document_date,
                inverse_answers_to_creator) |>
  tidyr::unnest(inverse_answers_to_creator) |>
  data.table::as.data.table()


### work_had_participation -----------------------------------------------------
parl_question_work_had_participation <- lapply(
  X = list_tmp,
  FUN = function(i_question) {
    if ( "workHadParticipation" %in% names(i_question) ) {
      i_question[, c("id", "identifier", "document_date", "workHadParticipation")] #
    }
  }
) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)

# Unnest data.frame-column ----------------------------------------------------#
parl_question_work_had_participation_unnest <- data.table::rbindlist(
  l = parl_question_work_had_participation$workHadParticipation,
  use.names = TRUE, fill = TRUE)
# Rename ID
data.table::setnames(x = parl_question_work_had_participation_unnest,
                     old = "id", new = "participation_id")
# Get ID
parl_question_work_had_participation_unnest[, `:=`(
  identifier = gsub(pattern = "(^.*)([A-Z].\\d{1,2}.\\d{4}.\\d{6})(.*$)",
                    replacement = "\\2", x = participation_id )
)]
# Drop data.frame-column
parl_question_work_had_participation[, workHadParticipation := NULL]
# Merge
parl_question_work_had_participation <- parl_question_work_had_participation[
  parl_question_work_had_participation_unnest,
  on = "identifier"
]
# Remove object
rm(parl_question_work_had_participation_unnest)


#------------------------------------------------------------------------------#
### Test time difference -------------------------------------------------------
dt_tmp = unique(parl_question_work_had_participation[, list(id, document_date)]) |>
  full_join(
    y = parl_question_inverse_answers_to |>
      dplyr::select(id, inverse_answers_to_document_date) |>
      tidyr::unnest_longer(inverse_answers_to_document_date) |>
      dplyr::filter(!is.na(inverse_answers_to_document_date)) |>
      dplyr::distinct(),
    by = "id"
  )
dt_tmp[, .N, by = id][order(N)]

dt_tmp[, `:=`(
  inverse_answers_to_document_date = data.table::as.IDate(inverse_answers_to_document_date),
  document_date = data.table::as.IDate(document_date)
)]
dt_tmp[, time_diff := inverse_answers_to_document_date - document_date]
summary(dt_tmp$time_diff)

questions_priority <- unique(parl_questions$id[
  parl_questions$work_type == "def/ep-document-types/QUESTION_WRITTEN_PRIORITY"])
questions_no_priority <- unique(parl_questions$id[
  parl_questions$work_type == "def/ep-document-types/QUESTION_WRITTEN"])
addressee_iscom <- unique(parl_question_work_had_participation$id[
  grepl(pattern = "_COM",
        x = parl_question_work_had_participation$participation_id)])

no_priority_10 <- unique(dt_tmp[
  document_date >= as.IDate("2024-07-01")
  & id %in% questions_no_priority
  & id %in% addressee_iscom
][,
  is_above21 := ifelse(test = time_diff > 42L, yes = 1L, no = 0L)
])
sum(no_priority_10$is_above21, na.rm = TRUE) # 1481

priority_10 <- unique(dt_tmp[
  document_date >= as.IDate("2024-07-01")
  & id %in% questions_priority
  & id %in% addressee_iscom
][,
  is_above21 := ifelse(test = time_diff > 42L, yes = 1L, no = 0L)
])
sum(no_priority_10$is_above21, na.rm = TRUE) # 1481


