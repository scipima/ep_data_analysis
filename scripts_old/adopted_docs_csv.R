#------------------------------------------------------------------------------#
# Download and Process EP Open Data on adopted Docs ----------------------------
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
  "https://data.europarl.europa.eu/distribution/adopted-texts_2025_2_en.csv",
  "https://data.europarl.europa.eu/distribution/adopted-texts_2024_33_en.csv"
  # "https://data.europarl.europa.eu/distribution/adopted-texts_2023_55_en.csv",
  # "https://data.europarl.europa.eu/distribution/adopted-texts_2022_13_en.csv",
  # "https://data.europarl.europa.eu/distribution/adopted-texts_2021_8_en.csv",
  # "https://data.europarl.europa.eu/distribution/adopted-texts_2020_5_en.csv",
  # "https://data.europarl.europa.eu/distribution/adopted-texts_2019_7_en.csv"
  )

# read all .csv at once
docs_list <- lapply(X = docs_csv, data.table::fread)
# append all .csv together ----------------------------------------------------#
adopted_texts <- data.table::rbindlist(l = docs_list, use.names = T, fill = T) |>
  dplyr::mutate(
    document_adopts = gsub(pattern = "https://data.europarl.europa.eu/eli/dl/doc/",
                            replacement = "", x = document_adopts),
    document_procedure = gsub(pattern = "https://data.europarl.europa.eu/eli/dl/event/",
                              replacement = "", x = document_procedure) )

# write to disk ---------------------------------------------------------------#
data.table::fwrite(x = adopted_texts,
                   file = here::here("data_out", "docs_pl", "adopted_texts_fromcsv.csv"))

# dt_tmp = plenary_docs[
#   document_identifier %in% unique(adopted_texts$document_adopts),
#   list(document_identifier, document_creator_person, document_creator_organization)]
#
# dt_tmp2 = plenary_docs |>
#   inner_join(
#     y = adopted_texts,
#     by = c("document_identifier" = "document_adopts")
#   )
