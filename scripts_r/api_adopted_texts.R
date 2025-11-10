###--------------------------------------------------------------------------###
# EP API: Adopted Texts --------------------------------------------------------
###--------------------------------------------------------------------------###

#' This scripts grabs the list as well as the associated info on all adopted texts since the start of the EP's 9th mandate.
#' We split the script in 2, depending on whether the full dataset has already been built once or not.
#' This is because it is unlikely that data relative to adopted texts in previous years is will change often (if any).
#' Thus, if the dataset has been built once, we only retrieve the current year worth of data.
#' REF: https://data.europarl.europa.eu/en/home

###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
library(httr2)
library(here)
library(jsonlite)
library(xml2)
library(future.apply)
library(dplyr)
library(tidyr)
library(data.table)


###--------------------------------------------------------------------------###
## GET adopted-documents -------------------------------------------------------
# https://data.europarl.europa.eu/api/v2/adopted-texts?year=2019,2020&format=application%2Fld%2Bjson&offset=0

years <- paste(2016L : data.table::year(Sys.Date()), collapse="," )

req <- httr2::request(
  paste0("https://data.europarl.europa.eu/api/v2/adopted-texts?year=",
         years,
         "&format=application%2Fld%2Bjson&offset=0") ) |>
  httr2::req_perform() |>
  httr2::resp_body_json(simplifyDataFrame = TRUE)
# head(req$data)

# check)
adopted_texts <- req$data |>
  dplyr::mutate(work_type = gsub(pattern = "def/ep-document-types/",
                                 replacement = "", x = work_type) ) |>
  dplyr::select(-type)
# check
# sapply(procedures_df, function(x) sum(is.na(x)))

if ( file.exists( here::here("data_out", "docs", "adopted_texts.csv") ) ) {
  adopted_texts_old <- data.table::fread(
    file = here::here("data_out", "docs", "adopted_texts.csv") )
  doc_ids <- adopted_texts$identifier[
    ! adopted_texts$identifier %in% adopted_texts_old$identifier ]
  doc_ids <- sort(doc_ids) # for tidier printout
} else {
  # write data to disk --------------------------------------------------------#
  data.table::fwrite(x = adopted_texts,
                     file = here::here("data_out", "docs", "adopted_texts.csv") )
  # get doc ids
  doc_ids <- sort(unique(adopted_texts$identifier))
}

# Remove API objects ----------------------------------------------------------#
rm(req, years)


###--------------------------------------------------------------------------###
## GET/adopted-texts/{doc-id} --------------------------------------------------
# Returns a single EP Adopted Text document for the specified doc ID

#' We only execute this if we have new docs OR if it's the first time.
#' EXAMPLE: https://data.europarl.europa.eu/api/v2/adopted-texts/TA-9-2022-0201?format=application%2Fld%2Bjson

if ( length(doc_ids) > 0 ) {
  # loop to get all decisions
  adopted_docs_list <- lapply(
    X = setNames(object = doc_ids,
                 nm = doc_ids),
    FUN = function(i_param) {
      print(i_param)

      # Create an API request -------------------------------------------------#
      req <- httr2::request("https://data.europarl.europa.eu/api/v2") |>
        httr2::req_url_path_append("adopted-texts") |>
        httr2::req_url_path_append(i_param) |>
        httr2::req_url_path_append("?format=application%2Fld%2Bjson")

      # Add time-out and ignore error -----------------------------------------#
      resp <- req |>
        httr2::req_error(is_error = ~FALSE) |>
        httr2::req_throttle(30 / 60) |> # 1 call every 2 seconds
        httr2::req_perform()

      # If not an error, download and make available in ENV -------------------#
      if ( httr2::resp_status(resp) == 200L) {
        resp_body <- resp |>
          httr2::resp_body_json( simplifyDataFrame = TRUE )
        return(resp_body$data)
      } } )
}
# store tmp list if code breaks down the line
readr::write_rds(x = adopted_docs_list,
  file = here::here("data_out", "docs", "adopted_docs_list.rds"))

title_dcterms <- lapply(adopted_docs_list, function(x) {
  if ( "title_dcterms" %in% names(x) ) {
    x |>
      dplyr::select(id, title_dcterms) |>
      tidyr::unnest(title_dcterms, names_sep = "_") |>
      dplyr::rename(title_en = title_dcterms_en) |>
      dplyr::select(-starts_with("title_dcterms"))
  } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)

# procedures
inverse_created_a_realization_of_proc <- lapply(adopted_docs_list, function(x) {
  if ( "inverse_created_a_realization_of" %in% names(x) ) {
    x |>
      dplyr::select(id, inverse_created_a_realization_of) |>
      tidyr::unnest(inverse_created_a_realization_of)
  } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::filter(grepl("proc", inverse_created_a_realization_of) )
# check - is this a 1-to-1 relationship?
inverse_created_a_realization_of_proc[, .N, by = id][order(N)] # No
# write to disk
data.table::fwrite(x = inverse_created_a_realization_of_proc,
                   file = here::here("data_out", "docs", "adopted_texts_process.csv"))


# dt_tmp <- lapply(X = adopted_docs_list, FUN = function(x) {
#   x[, !names(x) %in% c("title_dcterms", "is_about") ] } ) |>
#   data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
#   dplyr::select(dplyr::where(is.character), adopts )
# data.table::fwrite(x = dt_tmp,
#                    file = here::here("data_out", "docs", "adopted_texts.csv"))


# dt_tmp <- lapply(X = adopted_docs_list, FUN = function(x) {
#   x[, !names(x) %in% c("title_dcterms", "is_about") ] } ) |>
#   data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
#   dplyr::select(-c(is_realized_by, publisher)) |>
#   data.table::as.data.table()
# dt_tmp[, .N, by = id][order(N)]


adopted_texts_ids <- data.table::rbindlist(adopted_docs_list,
   use.names = TRUE, fill = TRUE)
