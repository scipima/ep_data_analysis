###--------------------------------------------------------------------------###
# Webscrape MEPs by political group, country, and their history ################
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' This script seeks to collect all reports that were assigned to MEPs.
#' To achieve that, it scrapes the full list of MEPs for the 9th legislature, in the 3 different blocks in which they are presented on the [EP website](https://www.europarl.europa.eu/meps/en/full-list), namely current, incoming, outgoing.
#' Having done that, with the appended dataframe of the those MEPs, the scripts also scrapes their history in terms of national political parties and EP political groups.
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
library(XML)
library(xml2)
library(tidyverse)
library(data.table)
library(rvest)


#------------------------------------------------------------------------------#
## Load join functions ---------------------------------------------------------

#' This brings in a set of convenience functions to merge data.

source(file = here::here("source_scripts_r", "join_functions.R"))


#------------------------------------------------------------------------------#
## Adopted texts & clean -------------------------------------------------------
adopted_texts_fromcsv <- data.table::fread(
  here::here("data_out", "docs_pl", "adopted_texts_fromcsv.csv") )

# create temporary duplicate col for string processing
adopted_texts_fromcsv[, identifier2 := document_adopts]
# Delete prefix in Parliamentary Questions
adopted_texts_fromcsv[, identifier2 := gsub(pattern = "^QOB-",
                                            replacement = "B-",
                                            x = identifier2,
                                            perl = T)]
# invert the orders of the groups
adopted_texts_fromcsv[, label := gsub(pattern = "(^[A-Z]{1,2}-\\d{1,2}-)(\\d{4})-(\\d{4})",
                                      replacement = "\\1\\3-\\2",
                                      x = identifier2, perl = T)]
# delete -
adopted_texts_fromcsv[, label := gsub(pattern = "(?<=[A-Z])-",
                                      replacement = "",
                                      x = label, perl = T)]
# treat RC separately
adopted_texts_fromcsv[, label := gsub(pattern = "RC",
                                      replacement = "RC-B",
                                      x = label, perl = T)]
# slash at the end
adopted_texts_fromcsv[, label := gsub(pattern = "(?<=-\\d{4})-",
                                      replacement = "/",
                                      x = label, perl = T)]
# Delete cols
adopted_texts_fromcsv[, c("identifier2") := NULL]
# sapply(meps_rapporteurs, function(x) sum(is.na(x))) # check


#------------------------------------------------------------------------------#
## Procedures ------------------------------------------------------------------
procedures_created_a_realization_of <- data.table::fread(
  here::here("data_out", "procedures", "procedures_created_a_realization_of.csv") ) |>
  dplyr::filter( grepl(pattern = "eli/dl/doc/A-",
                       x = created_a_realization_of, fixed = TRUE ) ) |>
  data.table::as.data.table()


# Fix docs' labels ------------------------------------------------------------#
# create temporary duplicate col for string processing
procedures_created_a_realization_of[, identifier2 := created_a_realization_of]
# invert the orders of the groups for A-, B-, C- files
procedures_created_a_realization_of[
  grepl(pattern = "^[ABC]{1}.\\d{1,2}.", x = identifier2),
  doc_id := gsub(
    pattern = "(^[ABC]{1}.\\d{1,2}.)(\\d{4}).(\\d{4})",
    replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
# treat RC separately
procedures_created_a_realization_of[
  grepl(pattern = "^RC.\\d{1,2}", x = identifier2),
  doc_id := gsub(
    pattern = "(^RC.\\d{1,2}.)(\\d{4}).(\\d{4})",
    replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
procedures_created_a_realization_of[, doc_id := gsub(pattern = "^RC", replacement = "RC-B",
                                   x = doc_id, perl = T)]
# delete - between doc_id LETTER and MANDATE NUMBER
procedures_created_a_realization_of[, doc_id := gsub(pattern = "([A-Z]{1}).(\\d{1,2})",
                                   replacement = "\\1\\2", x = doc_id, perl = T)]
# slash at the end
procedures_created_a_realization_of[, doc_id := gsub(pattern = "(?<=.\\d{4}).",
                                   replacement = "/", x = doc_id, perl = T)]
# Delete cols
procedures_created_a_realization_of[, c("identifier2") := NULL]
# sapply(procedures_created_a_realization_of, function(x) sum(is.na(x))) # check


#------------------------------------------------------------------------------#
## Get last affiliation for all MEPS -------------------------------------------
meps_dates_ids <- read_csv(
  here::here("data_out", "meps", "meps_dates_ids_all.csv") ) |>
  dplyr::group_by(pers_id) |>
  dplyr::slice_max(order_by = activity_date, n = 1, with_ties = FALSE) |>
  dplyr::ungroup() |>
  dplyr::select(- activity_date)


#------------------------------------------------------------------------------#
## Get MEPs by Mandate ---------------------------------------------------------
# 9th mandate -----------------------------------------------------------------#
req <- httr2::request("https://data.europarl.europa.eu/api/v2/meps?parliamentary-term=9&format=application%2Fld%2Bjson") |>
  httr2::req_perform() |>
  httr2::resp_body_json(simplifyDataFrame = TRUE)

meps_9 <- req$data |>
  dplyr::mutate(mandate = 9L)
rm(req) # remove object


# 10th mandate -----------------------------------------------------------------#
req <- httr2::request("https://data.europarl.europa.eu/api/v2/meps?parliamentary-term=10&format=application%2Fld%2Bjson") |>
  httr2::req_perform() |>
  httr2::resp_body_json(simplifyDataFrame = TRUE)

meps_10 <- req$data |>
  dplyr::mutate(mandate = 10L)
rm(req) # remove object


###--------------------------------------------------------------------------###
## MEPs' Reports & Opinions ----------------------------------------------------
### Construct MEP links --------------------------------------------------------
meps_links <- dplyr::bind_rows( meps_9, meps_10 ) |>
  # as of 2024-07-16, there is no report for the 10th mandate - delete the following row later on
  # dplyr::filter(mandate == 9L) |>
  dplyr::select(pers_id = id, mandate, givenName, familyName) |>
  dplyr::mutate(
    pers_id = as.integer( gsub(pattern = "person/", replacement = "", x = pers_id) ),
    surname = toupper( gsub(pattern = "\\s", replacement = "+", x = familyName) ),
    fist_name = toupper( gsub(pattern = "\\s", replacement = "+", x = givenName) ),
    link_report = paste0(
      "https://www.europarl.europa.eu/meps/en/",
      pers_id, "/",
      paste(fist_name, surname, sep = "_"),
      "/all-activities/reports/",
      mandate),
    link_opinion = paste0(
      "https://www.europarl.europa.eu/meps/en/",
      pers_id, "/",
      paste(fist_name, surname, sep = "_"),
      "/all-activities/opinions/",
      mandate) ) |>
  arrange(familyName) |>
  as.data.frame()


#------------------------------------------------------------------------------#
### Grab MEPs' Reports ---------------------------------------------------------
# initialise loop -------------------------------------------------------------#
meps_reports_list <- vector(mode = "list", length = length(meps_links[, "link_report"]))

# loop ------------------------------------------------------------------------#
for (i_row in seq_along(meps_links[, "pers_id"]) ) {

  # create link ---------------------------------------------------------------#
  link_report <- meps_links$link_report[i_row]
  # check
  print(link_report)

  # Create an API request -----------------------------------------------------#
  req <- httr2::request(link_report)

  # Ignore error, if any ------------------------------------------------------#
  resp <- req |>
    httr2::req_error(is_error = ~FALSE) |>
    httr2::req_perform()

  # If not an error, download and make available in ENV -----------------------#
  if ( httr2::resp_status(resp) == 200L) {
    webpage <- resp |>
      httr2::resp_body_html()

    # reports -----------------------------------------------------------------#
    reports_raw <- webpage |>
      rvest::html_nodes(".erpl_search-results-list-expandable-block") |>
      rvest::html_text2()
    # reports_raw_list <- stringr::str_split(string = reports_raw, pattern = "\n")

    # store data in list
    meps_reports_list[[i_row]] <- data.frame(
      pers_id = meps_links$pers_id[i_row],
      reports_raw = reports_raw)
  }

  # Remove objects
  rm(i_row, link_report, req, resp, resp_body, webpage, reports_raw)
}


#------------------------------------------------------------------------------#
#### Append loop results & clean -----------------------------------------------
meps_rapporteurs <- data.table::rbindlist(l = meps_reports_list,
                                          use.names = TRUE, fill = TRUE) |>
  tidyr::separate_wider_delim(
    cols = reports_raw, delim = "\n",
    names = c("doc_title", "ids", "todrop", "meps_names") ) |>
  dplyr::mutate(
    date = lubridate::dmy(
      stringr::str_extract(string = ids,
                           pattern = "\\d{2}-\\d{2}-\\d{4}") ),
    doc_id = stringr::str_extract(string = ids,
                                  pattern = "[A-Z]\\d{1,2}-\\d{4}/\\d{4}"),
    ep_doc_ref = trimws( stringr::str_extract(string = ids,
                                              pattern = "PE\\d{3}.\\d{3}v\\d{2}-\\d{2}") ),
    committees = trimws( stringr::str_extract(string = ids,
                                              pattern = "[A-Z]{4,5}.*") ),
    committees = gsub(pattern = " ", replacement = "; ", x = committees) ) |>
  # dplyr::distinct(doc_id, .keep_all = TRUE) |> # remove duplicate reports
  dplyr::select(-ids, -todrop) |>
  data.table::as.data.table()


#------------------------------------------------------------------------------#
meps_rapporteurs_adopted <- meps_rapporteurs |>
  dplyr::left_join(
    y = meps_dates_ids,
    by = c("pers_id" = "pers_id") ) |>
  join_meps_names() |>
  join_polit_labs() |>
  join_meps_countries() |>
  dplyr::select(-c(country_id, polgroup_id, natparty_id )) |>
  dplyr::distinct() |>
  # dplyr::group_by(date_commtt_doc_tabled_plenary = date, doc_id, doc_title,
  #                 ep_doc_ref, committees) |>
  # dplyr::summarise(
  #   across(.cols = c(mep_name, political_group),
  #          .fns = \(x) paste0(x, collapse = "; ") ) ) |>
  # dplyr::ungroup() |>
  # dplyr::distinct(doc_id, .keep_all = TRUE) |> # remove duplicate reports
  dplyr::left_join(
    y = adopted_texts_fromcsv |>
      select(label, document_identifier, document_date_adopted = document_date,
             document_public_register_notation),
    by = c("doc_id" = "label") ) |>
  dplyr::left_join(
    y = procedures_created_a_realization_of,
    by = c("doc_id" = "label") )

#------------------------------------------------------------------------------#
#' This results in a many-to-many relationship because of some files (being amended, or having errata)
#' As of 2024-07-19, the list of offending items is:
#' A8-0171/2018; eli/dl/proc/2016-0224A
#' A9-0016/2021; eli/dl/proc/2018-0193
#------------------------------------------------------------------------------#

meps_renew_rapporteurs_adopted <- meps_rapporteurs |>
  dplyr::left_join(
    y = meps_dates_ids,
    by = c("pers_id" = "pers_id") ) |>
  join_meps_names() |>
  join_polit_labs() |>
  join_meps_countries() |>
  # filter for just RENEW and NOT ADOPTED -------------------------------------#
  dplyr::filter(political_group == "Renew") |>
  dplyr::select( -c(country_id, polgroup_id, natparty_id) ) |>
  dplyr::distinct() |>
  dplyr::group_by(date_commtt_doc_tabled_plenary = date, doc_id, doc_title,
                  ep_doc_ref, committees) |>
  dplyr::summarise(
    across(.cols = c(mep_name, political_group),
           .fns = \(x) paste0(x, collapse = "; ") ) ) |>
  dplyr::ungroup() |>
  # dplyr::distinct(doc_id, .keep_all = TRUE) |> # remove duplicate reports
  dplyr::left_join(
    y = adopted_texts_fromcsv |>
      select(label, document_identifier, document_date_adopted = document_date,
             document_public_register_notation),
    by = c("doc_id" = "label") ) |>
  dplyr::left_join(
    y = procedures_created_a_realization_of,
    by = c("doc_id" = "label") )


meps_renew_rapporteurs_adopted |>
  filter(is.na(document_identifier)) |>
  left_join( plenary_docs_process )


# doc_adopts <- data.table::fread("data_out/docs/doc_adopts.csv")|>
#   dplyr::mutate(adopts = gsub(pattern = "eli/dl/doc/", replacement = "", x = adopts))
#
#
# committee_docs_list <- readRDS("~/github/ep_data_collect_analyse/data_out/docs/committee_docs_list.rds")
# cmmtt_doc_inverse_created_a_realization_of <- lapply(committee_docs_list, function(x) {
#   if ( "inverse_created_a_realization_of" %in% names(x) ) {
#     x |>
#       dplyr::select(id, inverse_created_a_realization_of) |>
#       tidyr::unnest(inverse_created_a_realization_of, names_sep = "_") } } ) |>
#   data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
#   dplyr::mutate(
#     inverse_created_a_realization_of = gsub(pattern = "eli/dl/proc/",
#                                             replacement = "",
#                                             x = inverse_created_a_realization_of),
#     id = gsub(pattern = "eli/dl/doc/", replacement = "", x = id))

# plenary_docs_process <- doc_adopts |>
#   left_join(cmmtt_doc_inverse_created_a_realization_of,
#             by = c("adopts" = "id"))



#------------------------------------------------------------------------------#
### Grab MEPs' Opinions --------------------------------------------------------
# initialise loop -------------------------------------------------------------#
meps_opinions_list <- vector(mode = "list", length = length(meps_links[, "link_opinion"]))

# loop ------------------------------------------------------------------------#
for (i_row in seq_along(meps_links[, "pers_id"]) ) {

  # create link ---------------------------------------------------------------#
  link_opinion <- meps_links$link_opinion[i_row]
  # check
  print(link_opinion)

  # Create an API request -----------------------------------------------------#
  req <- httr2::request(link_opinion)

  # Ignore error, if any ------------------------------------------------------#
  resp <- req |>
    httr2::req_error(is_error = ~FALSE) |>
    httr2::req_perform()

  # If not an error, download and make available in ENV -----------------------#
  if ( httr2::resp_status(resp) == 200L) {
    webpage <- resp |>
      httr2::resp_body_html()

    # reports -----------------------------------------------------------------#
    reports_raw <- webpage |>
      rvest::html_nodes(".erpl_search-results-list-expandable-block") |>
      rvest::html_text2()
    # reports_raw_list <- stringr::str_split(string = reports_raw, pattern = "\n")

    # store data in list
    meps_opinions_list[[i_row]] <- data.frame(
      pers_id = meps_links$pers_id[i_row],
      reports_raw = reports_raw)
  }

  # Remove objects
  rm(i_row, link_opinion, req, resp, resp_body, webpage, reports_raw)
}


#------------------------------------------------------------------------------#
#### Append loop results & clean ------------------------------------------------
meps_opinions <- data.table::rbindlist(l = meps_opinions_list,
                                       use.names = TRUE, fill = TRUE) |>
  tidyr::separate_wider_delim(
    cols = reports_raw, delim = "\n",
    names = c("doc_title", "ids", "todrop", "meps_names") ) |>
  dplyr::mutate(
    date = lubridate::dmy(
      stringr::str_extract(string = ids,
                           pattern = "\\d{2}-\\d{2}-\\d{4}") ),
    doc_id = stringr::str_extract(string = ids,
                                  pattern = "[A-Z]{4,5}_[A-Z]{2}\\(\\d{4}\\)\\d{6}"),
    ep_doc_ref = trimws( stringr::str_extract(string = ids,
                                              pattern = "PE\\d{3}.\\d{3}v\\d{2}-\\d{2}") ),
    committees = trimws( stringr::str_extract(string = ids,
                                              pattern = "\\s[A-Z]{4,5}") ),
    committees = gsub(pattern = " ", replacement = "; ", x = committees)
  ) |>
  # dplyr::distinct(doc_id, .keep_all = TRUE) |> # remove duplicate reports
  dplyr::select(-ids, -todrop) |>
  data.table::as.data.table()
