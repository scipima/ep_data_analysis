###--------------------------------------------------------------------------###
# Google BigQuery --------------------------------------------------------------
###--------------------------------------------------------------------------###

start_script <- Sys.time()
mandate_starts <- as.Date("2024-07-14")

###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
library(tidyverse)
library(DBI)
library(bigrquery)

###--------------------------------------------------------------------------###
#' REF:
#' https://www.youtube.com/watch?v=WpRzA6okx68;
#' https://www.infoworld.com/article/3622926/how-to-use-r-with-bigquery.html;
#' https://www.youtube.com/watch?v=6j27h_17C1Q&t=1698s;
#' https://github.com/libjohn/casestudy_bigquery_dbplyr
###--------------------------------------------------------------------------###


###--------------------------------------------------------------------------###
# BigQuery authorisation ----------------------------------------------------###
bigrquery::bq_auth(email = "marco.scipioni05@gmail.com") # substitute with user's

## DB connection ---------------------------------------------------------------
db_conn <- DBI::dbConnect(
  bigrquery::bigquery(),
  project = "ep10-434611",
  dataset = "ep_vote_collect",
  billing = "ep10-434611")

###--------------------------------------------------------------------------###
# Get list of tables --------------------------------------------------------###
dbListTables(db_conn)


#------------------------------------------------------------------------------#
## Upload to BigQuery ----------------------------------------------------------
project_dataset <- bigrquery::bq_dataset(project = "ep10-434611",
                                         dataset = "ep_vote_collect")


### Political Groups -----------------------------------------------------------
# Read in data
political_groups <- data.table::fread( here::here("data_out", "bodies", "political_groups_10.csv") ) |>
  dplyr::arrange(label)

# Prep
bq_political_groups <- bigrquery::bq_table(
  project_dataset, "political_groups", type = "TABLE")

#------------------------------------------------------------------------------#
#' The function first checks whether the BigQuery Table already exists.
#' If it does, it checks the last modification.
#' If `time_diff` is bigger than a selected value (default is 1 day, in seconds), it deletes the existing Table and re-uploads it.
#' If the Table does not exists, it creates the scheme and then uploads it.
#------------------------------------------------------------------------------#

bq_table_update <- function(
    bq_tbl = bq_political_groups,
    data_in = political_groups,
    short_label = "EP Political Groups",
    long_label = "This is the dictionary of EP Political Groups during the EP 10th mandate",
    time_diff = 86400L) {
  
  # Check if Table already exists
  if ( bigrquery::bq_table_exists(bq_tbl) ) {
    
    lastModifiedTime <- bigrquery::bq_table_meta(bq_tbl)$lastModifiedTime
    lastModifiedTime <- lubridate::as_datetime(
      as.numeric(lastModifiedTime) / 1000, tz = "Europe/Brussels" )
    diff_seconds <- lubridate::time_length(
      lubridate::now(tzone = "Europe/Brussels") - lastModifiedTime )
    # If Table on BigQuery is older than time_diff, then replace it
    
    if ( diff_seconds > time_diff ) {
      # First delete old Table
      bigrquery::bq_table_delete(x = bq_tbl)
      # Then Upload Table
      bigrquery::bq_table_upload(x = bq_tbl, values = data_in,
                                 quiet = FALSE) }
  } else {
    
    # If Table doesn't exist, upload Table structure
    bigrquery::bq_table_create(
      bq_tbl,
      fields = data_in,
      friendly_name = short_label,
      description = long_label)
    # Upload Table
    bigrquery::bq_table_upload(x = bq_tbl, values = data_in,
                               quiet = FALSE)
  }
}

# Update Table if needed
bq_table_update()
# check
bq_table_size(bq_political_groups)


### National Parties -----------------------------------------------------------
# Read in data
national_parties <- data.table::fread( here::here("data_out", "bodies", "national_parties_10.csv") )

# Prep
bq_national_parties <- bigrquery::bq_table(
  project_dataset, "national_parties", type = "TABLE")

# Update Table if needed
bq_table_update(bq_tbl = bq_national_parties, data_in = national_parties,
                short_label = "National Parties",
                long_label = "This is the dictionary of national parties during the EP 10th mandate")
# check
bigrquery::bq_table_size(bq_national_parties)


### Countries ------------------------------------------------------------------
# Read in data
country_dict <- data.table::fread( here::here("data_reference", "country_dict.csv") )

# Prep
bq_country_dict <- bigrquery::bq_table(
  project_dataset, "country_dict", type = "TABLE")

# Update Table if needed
bq_table_update(bq_tbl = bq_country_dict, data_in = country_dict,
                short_label = "Countries dictionary",
                long_label = "This is the dictionary of contries")
# check
bigrquery::bq_table_size(bq_country_dict)


#------------------------------------------------------------------------------#
## MEPs ------------------------------------------------------------------------
### MEPs Mandate ---------------------------------------------------------------
# Read in data
meps_mandate <- data.table::fread( here::here("data_out", "meps", "meps_mandate_10.csv") )

# Prep
bq_meps_mandate <- bigrquery::bq_table(
  project_dataset, "meps_mandate", type = "TABLE")

# Update Table if needed
bq_table_update(bq_tbl = bq_meps_mandate, data_in = meps_mandate,
                short_label = "MEPs during 10th EP Mandate",
                long_label = "Dictionary for all MEPs during the EP 10th mandate")
# check
bigrquery::bq_table_size(bq_meps_mandate)


### MEPs Last Plenary ----------------------------------------------------------
# Read in data
meps_lastplenary <- data.table::fread( here::here("data_out", "meps", "meps_current.csv") )

# Prep
bq_meps_lastplenary <- bigrquery::bq_table(
  project_dataset, "meps_lastplenary", type = "TABLE")

# Update Table if needed
bq_table_update(bq_tbl = bq_meps_lastplenary, data_in = meps_lastplenary,
                short_label = "MEPs in last Plenary Session",
                long_label = "List of all MEPs who should have been present during the last Plenary Session")
# check
bigrquery::bq_table_size(bq_meps_lastplenary)


### MEPs Dates -----------------------------------------------------------------
# # Read in data
# meps_dates_ids <- data.table::fread( here::here("data_out", "meps", "meps_dates_ids_10.csv") )

# # Prep
# bq_meps_dates_ids <- bigrquery::bq_table(
#   project_dataset, "meps_dates_ids", type = "TABLE")

# # Update Table if needed
# bq_table_update(bq_tbl = bq_meps_dates_ids, data_in = meps_dates_ids,
#                 short_label = "MEPs' membership over time, 10th mandate",
#                 long_label = "MEPs' membership in terms of national parties, EP Political Groups, and countries, for each Plenary Session of the 10th mandate")
# # check
# bigrquery::bq_table_size(bq_meps_dates_ids)


#------------------------------------------------------------------------------#
## Votes -----------------------------------------------------------------------
### Votes ----------------------------------------------------------------------
# Read in data
votes_dt <- data.table::fread( here::here("data_out", "votes", "votes_dt_10.csv") )
# votes_dt[, c("activity_id", "doc_id") := NULL] # Remove unnecessary cols

# Prep
bq_votes_dt <- bigrquery::bq_table(
  project_dataset, "votes_dt", type = "TABLE")

# Update Table if needed
bq_table_update(bq_tbl = bq_votes_dt, data_in = votes_dt,
                short_label = "Votes 10th EP Mandate",
                long_label = "Full list of the voting records in Plenary during the EP 10th mandate")
# check
bigrquery::bq_table_size(bq_votes_dt)


### Final Votes ----------------------------------------------------------------
# Read in data
votes_final <- data.table::fread(
  here::here("data_out", "votes", "votes_final_10.csv") )

# Prep
bq_votes_final <- bigrquery::bq_table(
  project_dataset, "votes_final", type = "TABLE")

# Update Table if needed
bq_table_update(bq_tbl = bq_votes_final, data_in = votes_final,
                short_label = "Final Votes 10th EP Mandate",
                long_label = "Final votes in Plenary during the EP 10th mandate")
# check
bigrquery::bq_table_size(bq_votes_final)


### Votes Titles -----------------------------------------------------------
# Read in data
# meetings_voteresults_titles <- data.table::fread( here::here(
#   "data_out", "votes", "meetings_voteresults_titles_10.csv") )

# Prep
# bq_meetings_voteresults_titles <- bigrquery::bq_table(
#   project_dataset, "meetings_voteresults_titles", type = "TABLE")

# Update Table if needed
# bq_table_update(bq_tbl = bq_meetings_voteresults_titles, 
#                 data_in = meetings_voteresults_titles,
#                 short_label = "Plenary Votes Titles",
#                 long_label = "Plenary Votes Titles as they appeanr in the Voting Lists")
# check
# bigrquery::bq_table_size(bq_meetings_voteresults_titles)


### Votes Dictionary -----------------------------------------------------------
# Read in data
vote_dict <- data.table::fread( here::here("data_out", "votes", "vote_dict.csv") )

# Prep
bq_vote_dict <- bigrquery::bq_table(
  project_dataset, "vote_dict", type = "TABLE")

# Update Table if needed
bq_table_update(bq_tbl = bq_vote_dict, data_in = vote_dict,
                time_diff = 31104000, # change it once per year ...
                short_label = "Vote Dictionary",
                long_label = "Vote dictionary for both official and calculated votes in Plenary during the EP 10th mandate")
# check
bigrquery::bq_table_size(bq_vote_dict)


### RCV description ------------------------------------------------------------
plenary_session_docs_rcv <- data.table::fread(
  here::here("data_out", "rcv", "plenary_session_docs_rcv_10.csv") )

rcv_descriptiontext <- plenary_session_docs_rcv |>
  dplyr::select(rcv_id = inverse_is_part_of_inverse_recorded_in_a_realization_of_id,
                inverse_is_part_of_title_dcterms_fr) |>
  dplyr::mutate(rcv_id = as.integer(gsub("^.*-DEC-", "", rcv_id) ) )

# Prep
bq_rcv_descriptiontext <- bigrquery::bq_table(
  project_dataset, "rcv_descriptiontext", type = "TABLE")

# Update Table if needed
bq_table_update(
  bq_tbl = bq_rcv_descriptiontext, 
  data_in = rcv_descriptiontext,
  short_label = "RCVs description",
  long_label = "RCVs description text as recorded in .xml and .pdf files")
# check
bigrquery::bq_table_size(bq_rcv_descriptiontext)


### RCV ID & DOC ID -----------------------------------------------------------
# Read in data
rcvid_docid <- data.table::fread( here::here(
  "data_out", "rcv", "rcvid_docid_10.csv") )

# Prep
bq_rcvid_docid <- bigrquery::bq_table(
  project_dataset, "rcvid_docid", type = "TABLE")

# Update Table if needed
bq_table_update(bq_tbl = bq_rcvid_docid, data_in = rcvid_docid,
                short_label = "RCV id and DOC id",
                long_label = "Look up Table of the links between RCV id and DOC id")
# check
bigrquery::bq_table_size(bq_rcvid_docid)


### meps_rcv_mandate -----------------------------------------------------------
# Read in data
meps_rcv_mandate <- data.table::fread(
  file = here::here("data_out", "meps_rcv_mandate_10.csv") )

# Prep
bq_meps_rcv_mandate <- bq_table(project_dataset, "meps_rcv_mandate", type = "TABLE")
# Update Table if needed
bq_table_update(bq_tbl = bq_meps_rcv_mandate, data_in = meps_rcv_mandate,
                short_label = "RCVs 10th EP Mandate",
                long_label = "RCVs during the the EP 10th mandate")
# check
bigrquery::bq_table_size(bq_meps_rcv_mandate)


#------------------------------------------------------------------------------#
## Documents -------------------------------------------------------------------
### Plenary Documents & Committees ---------------------------------------------
# Read in data
plenary_docs_committee <- data.table::fread( here::here("data_out", "docs_pl", "plenary_docs_committee.csv") )

# Prep
bq_plenary_docs_committee <- bigrquery::bq_table(
  project_dataset, "plenary_docs_committee", type = "TABLE")

# Update Table if needed
bq_table_update(bq_tbl = bq_plenary_docs_committee,
                data_in = plenary_docs_committee,
                short_label = "Plenary Documents & Committee ",
                long_label = "The table connects Plenary Documents with the Committee labs. It is in long format, because the same doc may appear in several Committees (i.e. Joint Committees).")
# check
bigrquery::bq_table_size(bq_plenary_docs_committee)


### Plenary Documents ----------------------------------------------------------
# Read in data
plenary_docs <- data.table::fread( here::here("data_out", "docs_pl", "plenary_docs.csv") ) |> 
  janitor::clean_names()

# Prep
bq_plenary_docs <- bigrquery::bq_table(
  project_dataset, "plenary_docs", type = "TABLE")

# Update Table if needed
bq_table_update(bq_tbl = bq_plenary_docs, data_in = plenary_docs,
                short_label = "All Plenary Documents from API",
                long_label = "All Plenary Documents from API")
# check
bigrquery::bq_table_size(bq_plenary_docs)


# docs_inverse_created_a_realization_of ---------------------------------------#
#' Connects Plenary Documents to Procedures IDs.
docs_inverse_created_a_realization_of <- data.table::fread( here::here(
  "data_out", "docs_pl", "docs_inverse_created_a_realization_of.csv") )

# Prep
bq_docs_inverse_created_a_realization_of <- bigrquery::bq_table(
  project_dataset, "docs_inverse_created_a_realization_of", type = "TABLE")

# Update Table if needed
bq_table_update(bq_tbl = bq_docs_inverse_created_a_realization_of,
                data_in = docs_inverse_created_a_realization_of,
                short_label = "Plenary Documents and Procedures IDs",
                long_label = "Plenary Documents and Procedures IDs, one to many")
# check
bigrquery::bq_table_size(bq_docs_inverse_created_a_realization_of)


### Adopted Documents ----------------------------------------------------------
# Read in data
# adopted_texts <- data.table::fread( here::here("data_out", "docs_pl", "adopted_texts.csv") )

# # Prep
# bq_adopted_texts <- bigrquery::bq_table(
#   project_dataset, "adopted_texts", type = "TABLE")

# # Update Table if needed
# bq_table_update(bq_tbl = bq_adopted_texts,
#                 data_in = adopted_texts,
#                 short_label = "All Adopted Texts",
#                 long_label = "All Adopted Texts")
# # check
# bigrquery::bq_table_size(bq_adopted_texts)


### Committee Documents --------------------------------------------------------
# Read in data
# committee_docs_dt <- data.table::fread( here::here(
#   "data_out", "docs_cmt", "committee_docs_dt.csv") ) |> 
#   janitor::clean_names()

# Prep
# bq_committee_docs_dt <- bigrquery::bq_table(
#   project_dataset, "committee_docs_dt", type = "TABLE")

# Update Table if needed
# bq_table_update(bq_tbl = bq_committee_docs_dt,
#                 data_in = committee_docs_dt,
#                 short_label = "All Plenary Documents from API",
#                 long_label = "All Plenary Documents from API")
# check
# bigrquery::bq_table_size(bq_committee_docs_dt)



### Adopted Documents - Procedures ---------------------------------------------
# Read in data
# adopted_texts_process <- data.table::fread( here::here("data_out", "docs_pl", "adopted_texts_process.csv") )

# # Prep
# bq_adopted_texts_process <- bigrquery::bq_table(
#   project_dataset, "adopted_texts_process", type = "TABLE")

# # Update Table if needed
# bq_table_update(bq_tbl = bq_adopted_texts_process,
#                 data_in = adopted_texts_process,
#                 short_label = "All Adopted Texts Procedures",
#                 long_label = "All Adopted Texts Procedures. Beware that this is a 1-to-may relationship between id and doc_id")
# # check
# bigrquery::bq_table_size(bq_adopted_texts_process)


#------------------------------------------------------------------------------#
## Procedures ------------------------------------------------------------------
### Procedures -----------------------------------------------------------------

# # Read in data
# procedures <- data.table::fread(
#   here::here("data_out", "procedures", "procedures.csv") )

# # Prep
# bq_procedures <- bigrquery::bq_table(
#   project_dataset, "procedures", type = "TABLE")

# # Update Table if needed
# bq_table_update(bq_tbl = bq_procedures,
#                 data_in = procedures,
#                 short_label = "All Procedures.",
#                 long_label = "All Procedures.")
# # check
# bigrquery::bq_table_size(bq_procedures)


### Procedures Consist of ------------------------------------------------------

# # Read in data
# procedures_consists_of <- data.table::fread(
#   here::here("data_out", "procedures", "procedures_consists_of.csv") )

# # Prep
# bq_procedures_consists_of <- bigrquery::bq_table(
#   project_dataset, "procedures_consists_of", type = "TABLE")

# # Update Table if needed
# bq_table_update(bq_tbl = bq_procedures_consists_of,
#                 data_in = procedures_consists_of,
#                 short_label = "All Procedures-related events.",
#                 long_label = "All Procedures-related events.")
# # check
# bigrquery::bq_table_size(bq_procedures_consists_of)


### Procedures Created A Realization_of ----------------------------------------

# # Read in data
# procedures_created_a_realization_of <- data.table::fread( here::here(
#   "data_out", "procedures", "procedures_created_a_realization_of.csv") )

# # Prep
# bq_procedures_created_a_realization_of <- bigrquery::bq_table(
#   project_dataset, "procedures_created_a_realization_of", type = "TABLE")

# # Update Table if needed
# bq_table_update(bq_tbl = bq_procedures_created_a_realization_of,
#                 data_in = procedures_created_a_realization_of,
#                 short_label = "All Procedures-related docs.",
#                 long_label = "All Procedures-related docs.")
# # check
# bigrquery::bq_table_size(bq_procedures_created_a_realization_of)


#------------------------------------------------------------------------------#
## Aggregates ------------------------------------------------------------------
### tally_bygroup_byrcv --------------------------------------------------------
# Read in data
tally_bygroup_byrcv <- data.table::fread( file = here::here(
  "data_out", "aggregates", "tally_bygroup_byrcv_10.csv") ) 

# Prep
bq_tally_bygroup_byrcv <- bq_table(project_dataset, "tally_bygroup_byrcv", type = "TABLE")
# Update Table if needed
bq_table_update(bq_tbl = bq_tally_bygroup_byrcv, data_in = tally_bygroup_byrcv,
                short_label = "Tallies in RCVs 10th EP Mandate, by Political Groups",
                long_label = "Tallies in RCVs during the the EP 10th mandate, by Political Groups, including all types of votes")
# check
bigrquery::bq_table_size(bq_tally_bygroup_byrcv)


### tally_bygroup_byparty_byrcv ------------------------------------------------
# # Read in data
# tally_bygroup_byparty_byrcv <- data.table::fread(
#   file = here::here("data_out", "aggregates", "tally_bygroup_byparty_byrcv_10.csv") )

# # Prep
# bq_tally_bygroup_byparty_byrcv <- bq_table(project_dataset, "tally_bygroup_byparty_byrcv", type = "TABLE")
# # Update Table if needed
# bq_table_update(bq_tbl = bq_tally_bygroup_byparty_byrcv,
#                 data_in = tally_bygroup_byparty_byrcv,
#                 short_label = "Tallies in RCVs 10th EP Mandate, by national parties",
#                 long_label = "Tallies in RCVs during the the EP 10th mandate, by national parties, including all types of votes")
# # check
# bigrquery::bq_table_size(bq_tally_bygroup_byparty_byrcv)


### majority_bygroup_byrcv ------------------------------------------------
# Read in data
majority_bygroup_byrcv <- data.table::fread(
  file = here::here("data_out", "aggregates", "majority_bygroup_byrcv_10.csv") )

# Prep
bq_majority_bygroup_byrcv <- bq_table(project_dataset,
                                      "majority_bygroup_byrcv",
                                      type = "TABLE")
# Update Table if needed
bq_table_update(bq_tbl = bq_majority_bygroup_byrcv,
                data_in = majority_bygroup_byrcv,
                short_label = "Majorities by Political Groups, 10th EP Mandate",
                long_label = "Majorities in RCVs by Political Groups during the the EP 10th mandate, including only official votes")
# check
bigrquery::bq_table_size(bq_majority_bygroup_byrcv)


### whowon_house_polgroup ------------------------------------------------
# Read in data
whowon_house_polgroup <- data.table::fread(
  file = here::here("data_out", "aggregates", "whowon_house_polgroup_10.csv") ) |>
  janitor::clean_names()

# Prep
bq_whowon_house_polgroup <- bq_table(project_dataset,
                                     "whowon_house_polgroup",
                                     type = "TABLE")
# Update Table if needed
bq_table_update(bq_tbl = bq_whowon_house_polgroup,
                data_in = whowon_house_polgroup,
                short_label = "Wins by Political Groups, 10th EP Mandate",
                long_label = "Wins in RCVs by Political Groups during the the EP 10th mandate, including only official votes")
# check
bigrquery::bq_table_size(bq_whowon_house_polgroup)


### meps_eucldist ------------------------------------------------
# Read in data
meps_eucldist <- data.table::fread(
  file = here::here("data_out", "meps", "meps_eucldist.csv") ) |>
  janitor::clean_names()

# Prep
bq_meps_eucldist <- bq_table(project_dataset,
                             "meps_eucldist",
                             type = "TABLE")
# Update Table if needed
bq_table_update(bq_tbl = bq_meps_eucldist,
                data_in = meps_eucldist,
                short_label = "Euclidean Distances between MEPs",
                long_label = "Euclidean Distances between: MEPs-Groups; MEPs-Parties; MEPs-Renew. Including only official votes")
# check
bigrquery::bq_table_size(bq_meps_eucldist)


#------------------------------------------------------------------------------#
end_script <- Sys.time()
end_script - start_script
