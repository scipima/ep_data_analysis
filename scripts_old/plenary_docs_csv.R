#------------------------------------------------------------------------------#
# Download and Process EP Open Data on Plenary Docs ----------------------------
#------------------------------------------------------------------------------#

#' Collects .csv files from the [EP Open Data Portal](https://data.europarl.europa.eu/en/datasets?language=en&order=RELEVANCE&dataThemeFamily=dataset.theme.EP_PLEN_DOC).
#' The code then proceeds to tidy such data and write to disk.
#' REF: https://data.europarl.europa.eu/en/home

###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
library(tidyverse)
library(data.table)


## Texts tabled: read csv & append it all together -----------------------------
docs_csv <- c(
  "https://data.europarl.europa.eu/distribution/plenary-documents_2025_35_en.csv",
  "https://data.europarl.europa.eu/distribution/plenary-documents_2024_74_en.csv",
  "https://data.europarl.europa.eu/distribution/plenary-documents_2023_87_en.csv",
  "https://data.europarl.europa.eu/distribution/plenary-documents_2022_53_en.csv",
  "https://data.europarl.europa.eu/distribution/plenary-documents_2021_51_en.csv",
  "https://data.europarl.europa.eu/distribution/plenary-documents_2020_31_en.csv",
  "https://data.europarl.europa.eu/distribution/plenary-documents_2019_33_en.csv",
  "https://data.europarl.europa.eu/distribution/plenary-documents_2018_41_en.csv",
  "https://data.europarl.europa.eu/distribution/plenary-documents_2017_22_en.csv",
  "https://data.europarl.europa.eu/distribution/plenary-documents_2016_7_en.csv")

# read all .csv at once
docs_list <- lapply(X = docs_csv, data.table::fread)
# append all .csv together ----------------------------------------------------#
plenary_docs <- data.table::rbindlist(l = docs_list, use.names = T, fill = T)

#------------------------------------------------------------------------------#
# Clean env
rm(docs_list, docs_csv) ; gc()

# Extract the year feature
plenary_docs$year <- lubridate::year(plenary_docs$document_date)
# unique(plenary_docs$year) # check

#------------------------------------------------------------------------------#
# check
# plenary_docs |>
#   dplyr::filter(
#     grepl(
#       pattern = "Committee on ",
#       x = document_creator_organization
#     )
#   ) |>
#   dplyr::group_by(document_creator_organization) |>
#   dplyr::summarise(count = dplyr::n()) |>
#   dplyr::arrange(dplyr::desc(count))


# Fix docs' doc_ids ------------------------------------------------------------#
# create temporary duplicate col for string processing
plenary_docs[, identifier2 := document_identifier]
# invert the orders of the groups for A-, B-, C- files
plenary_docs[
  grepl(pattern = "^[ABC]{1}.\\d{1,2}.", x = identifier2),
  doc_id := gsub(
    pattern = "(^[ABC]{1}.\\d{1,2}.)(\\d{4}).(\\d{4})",
    replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
# treat RC separately
plenary_docs[
  grepl(pattern = "^RC.\\d{1,2}", x = identifier2),
  doc_id := gsub(
    pattern = "(^RC.\\d{1,2}.)(\\d{4}).(\\d{4})",
    replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
plenary_docs[, doc_id := gsub(pattern = "^RC", replacement = "RC-B",
                                   x = doc_id, perl = T)]
# delete - between doc_id LETTER and MANDATE NUMBER
plenary_docs[, doc_id := gsub(pattern = "([A-Z]{1}).(\\d{1,2})",
                                   replacement = "\\1\\2", x = doc_id, perl = T)]
# slash at the end
plenary_docs[, doc_id := gsub(pattern = "(?<=.\\d{4}).",
                                   replacement = "/", x = doc_id, perl = T)]
# Delete cols
plenary_docs[, c("identifier2") := NULL]
# sapply(plenary_docs, function(x) sum(is.na(x))) # check


if ( any(table(plenary_docs$document_identifier, exclude = NULL) > 1) ) {
  print("!! You may have duplicate documents - CHECK !!")
}



# sort(table(plenary_docs$doc_id, exclude = NULL), decreasing = TRUE)
# plenary_docs <- plenary_docs[
#   !document_identifier %in% "B-8-2018-0021"]

# write to disk ---------------------------------------------------------------#
data.table::fwrite(x = plenary_docs,
                   file = here::here("data_out", "docs_pl", "plenary_docs_fromcsv.csv"))


#------------------------------------------------------------------------------#
## Extract Committee-Doc table in LONG shape -----------------------------------
doc_committee <- plenary_docs |>
  dplyr::select(doc_id, document_parliamentary_term, document_creator_organization) |>
  dplyr::filter(
    grepl(pattern = "Committee on ", x = document_creator_organization)
    ) |>
  tidyr::separate_longer_delim(
    cols = document_creator_organization, delim = ";"
    ) |>
  dplyr::mutate(
    document_creator_organization = trimws(document_creator_organization)
    ) |>
  dplyr::filter(
    grepl(pattern = "Committee on ",
          x = document_creator_organization, perl = TRUE)
    ) |>
  data.table::as.data.table()
# Check
sort( unique(doc_committee$document_creator_organization) )
doc_committee[
  # document_parliamentary_term >= 9
  !grepl(pattern = "Special", x = document_creator_organization),
  .N,
  keyby = list(document_creator_organization)]

plenary_docs_committee <- fread(here::here(
  "data_out", "docs_pl", "plenary_docs_committee.csv") )
plenary_docs_committee[, .N, keyby = committee_lab]


# Fix data entry mistakes -----------------------------------------------------#
# doc_committee[
#   document_creator_organization == "Committee on the Environment, Public Health and Food Safety",
#   document_creator_organization := "Committee on Environment, Public Health and Food Safety"]
# doc_committee[
#   document_creator_organization == "Committee on the Internal Market and Consumer Protection",
#   document_creator_organization := "Committee on Internal Market and Consumer Protection"]
# doc_committee[
#   document_creator_organization == "Committee on Women’s Rights and Gender Equality",
#   document_creator_organization := "Committee on Women's Rights and Gender Equality"]
#
# doc_committee[, .N, by = doc_id][order(N)][N > 1]


# merge with Committee doc_ids -------------------------------------------------#
# doc_committee <- doc_committee |>
#   dplyr::left_join(
#     y = data.table::fread(here::here("data_reference", "ep_committees_doc_ids.csv") ) |>
#       dplyr::select(committee_lab, committee_long),
#     by = c("document_creator_organization" = "committee_long"))
# sort(unique(doc_committee$document_creator_organization))

# write to disk ---------------------------------------------------------------#
# data.table::fwrite(x = doc_committee[, list(doc_id, committee_lab)],
#                    file = here::here(
#                      "data_out", "docs", "plenary_docs_committee_fromcsv.csv"))
