###--------------------------------------------------------------------------###
# Libraries -----------------------------------------------------------------
pak::pak("pacman")

library(rvest)
library(tidyverse)
library(here)
library(httr)
library(future.apply)
library(rvest)
library(xml2)


#------------------------------------------------------------------------------#
## adopted_docs_list -----------------------------------------------------------
adopted_docs_list <- readr::read_rds(
  file = here::here("data_out", "docs", "adopted_docs_list.rds"))
# procedures
ta_inverse_created_a_realization_of_proc <- lapply(
  X = adopted_docs_list, FUN = function(x) {
    if ( "inverse_created_a_realization_of" %in% names(x) ) {
      x |>
        dplyr::select(id, inverse_created_a_realization_of) |>
        tidyr::unnest(inverse_created_a_realization_of) } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::filter(grepl("proc", inverse_created_a_realization_of) )
# check - is this a 1-to-1 relationship?
ta_inverse_created_a_realization_of_proc[, .N, by = id][order(N)] # No


#------------------------------------------------------------------------------#
## committee_docs_list ---------------------------------------------------------
committee_docs_list <- readr::read_rds(
  file = here::here("data_out", "docs", "committee_docs_list.rds"))
# procedures
comm_docs_inverse_created_a_realization_of_proc <- lapply(
  X = committee_docs_list, FUN = function(x) {
    if ( "inverse_created_a_realization_of" %in% names(x) ) {
      x |>
        dplyr::select(id, inverse_created_a_realization_of) |>
        tidyr::unnest(inverse_created_a_realization_of) } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::filter(grepl("proc", inverse_created_a_realization_of) )
# check - is this a 1-to-1 relationship?
comm_docs_inverse_created_a_realization_of_proc[, .N, by = id][order(N)] # No


#------------------------------------------------------------------------------#
procedures_list <- readr::read_rds(
  file = here::here("data_out", "procedures", "procedures_list.rds"))

procedures <- read_csv("data_out/procedures/procedures.csv")

p = procedures |>
  full_join(ta_inverse_created_a_realization_of_proc,
            by = c("id" = "inverse_created_a_realization_of")) |>
  full_join(comm_docs_inverse_created_a_realization_of_proc,
            by = c("id" = "inverse_created_a_realization_of"))








###--------------------------------------------------------------------------###
# all files in folder
html_files <- list.files(path = here::here("data_in", "docs_oeil"))

###--------------------------------------------------------------------------###
# Funciton: get html -----------------------------------------------------------
extract_doc <- function(data_in = html_files) {
  future.apply::future_lapply(
    X = data_in,
    FUN = function(link) {

      # get webpage ---------------------------------------------------------###
      # puts this in a loop until condition is satisfied, as sometimes reading webpage fails
      webpage <- xml2::read_html(here::here("data_in", "docs_oeil", link) )

      # subject -------------------------------------------------------------###
      subject <- webpage |>
        rvest::html_nodes("#basic-information-data .ep_gridrow .ep_gridcolumn:nth-child(1) p~ p+ p") |>
        rvest::html_text2()
      # entire grid ---------------------------------------------------------###
      entire_grid <- webpage |>
        rvest::html_nodes("#basic-information-data .ep_gridrow") |>
        rvest::html_text2()

      # put it all into df --------------------------------------------------###
      data.frame(
        subject = subject,
        entire_grid = entire_grid) } ) }


###--------------------------------------------------------------------------###
# perform parallelised extraction
future::plan(strategy = multisession) ## Run in parallel on local computer
oeil_docs_list <- extract_doc()
future::plan(strategy = sequential) # revert to normal


###--------------------------------------------------------------------------###
# append all DFs into a single DF, and save data -------------------------------
all_oeil_docs <- data.table::rbindlist(
  l = setNames(object = oeil_docs_list, nm = html_files),
  use.names = T, fill = T, idcol = "procedure_id")
all_oeil_docs[, procedure_id := gsub(
  pattern = "ficheprocedure.do\\?lang=en\\&reference=",
  replacement = "", x = procedure_id, perl = T)]
all_oeil_docs[, procedure_id := gsub(
  pattern = "%2F", replacement = "/", x = procedure_id, perl = T)]


###--------------------------------------------------------------------------###
# processing -------------------------------------------------------------------
## get rid of status
all_oeil_docs[, entire_grid := sub(" Procedure completed.*", "", entire_grid)]
# unique(all_oeil_docs$entire_grid)
## procedure_rule
all_oeil_docs[, procedure_rule := sub(".*? ", "", entire_grid)]
all_oeil_docs[, procedure_rule := sub("\r\n\n\r.*", "", procedure_rule)]
## Geographical area
all_oeil_docs[grepl(pattern = "Geographical area", x = entire_grid),
              geographical_area := sub(".*?Geographical area\n\r ", "", entire_grid)]
all_oeil_docs[, geographical_area := sub("\n\n\n.*", "", geographical_area)]
# unique(all_oeil_docs$geographical_area)[1:10]
## subject
all_oeil_docs[, subject_tmp := sub(".*?Subject\n\r ", "", entire_grid)]
all_oeil_docs[, subject_tmp := sub("\n\n.*", "", subject_tmp)]
# unique(all_oeil_docs$subject_tmp)[1:10]
# how many fields can there be?
all_oeil_docs[, n_subjects := str_count(all_oeil_docs$subject_tmp, '\\d+.*? ')]
# what's the max?
max_subjects <- max(all_oeil_docs$n_subjects)
# split the suject col into max_sujects cols
all_oeil_docs <- tidyr::separate(data = all_oeil_docs, col = subject_tmp,
                                 into = paste0("subject_", 1:max_subjects),
                                 sep = "\n\r ", remove = FALSE)

###--------------------------------------------------------------------------###
## merge with dossier -------------------------------------------------------###
all_oeil_docs <- dplyr::full_join(x = all_oeil_docs,
                                  y = read.csv(here::here("data_out", "docs", "all_reports_docs.csv")) |>
                                    dplyr::select(-c(link, doc_date)),
                                  by = c("procedure_id" = "procedure") )


# write data and upload --------------------------------------------------------
data.table::fwrite(all_oeil_docs, here::here("data_out", "docs", "all_oeil_docs.csv"))
googledrive::drive_upload(
  media = here::here("data_out", "docs", "all_oeil_docs.csv"),
  path = "R_stuff/R_projects_data/ep_rollcall/data_out/all_oeil_docs.csv",
  overwrite = TRUE)
