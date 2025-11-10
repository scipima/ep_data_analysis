###--------------------------------------------------------------------------###
# Join Functions ---------------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' DESCRIPTION.
#' These are a set of convenience functions to left-join the unique ids in the EP databases with the data dictionaries.
#------------------------------------------------------------------------------#

# rm(list = ls())

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
library(data.table)
library(dplyr)
library(tidyr)

if ( !exists("mandate_starts") ) {
  # Hard code the start of the mandate ----------------------------------------#
  mandate_starts <- as.Date("2024-07-14") }

#------------------------------------------------------------------------------#
## Load Data -------------------------------------------------------------------
### Countries ------------------------------------------------------------------
country_dict <- data.table::fread(input = here::here(
  "data_reference", "country_dict.csv") )


#------------------------------------------------------------------------------#
### National Parties -----------------------------------------------------------
if ( !exists("national_parties") ) {
  # Read in data conditional on mandate ---------------------------------------#
  if (mandate_starts == as.Date("2019-07-01")) {
    national_parties <- data.table::fread(
      here::here("data_out", "bodies", "national_parties_all.csv"))
  } else {
    national_parties <- data.table::fread(
      here::here("data_out", "bodies", "national_parties_10.csv")) } }

# Drop cols
national_parties[, c("alt_label_en", "alt_label_fr", "pref_label_fr")
                 := NULL]

# Get all parties ID for Independents
natparties_independent_ids <- national_parties$identifier[
  grepl(
    pattern = "^-$|Indépendant|Indépendent|Independent|Independente|Independiente",
    x = national_parties$pref_label_en)]


#------------------------------------------------------------------------------#
### Political Groups -----------------------------------------------------------
if ( !exists("political_groups") ) {
  # Read in data conditional on mandate ---------------------------------------#
  if (mandate_starts == as.Date("2019-07-01")) {
    political_groups <- data.table::fread(
      here::here("data_out", "bodies", "political_groups_all.csv"))
  } else {
    political_groups <- data.table::fread(
      here::here("data_out", "bodies", "political_groups_10.csv")) } }

# Drop cols
political_groups[, c("alt_label_en", "alt_label_fr", "pref_label_fr")
                 := NULL]


#------------------------------------------------------------------------------#
### MEPs mandate ---------------------------------------------------------------
if ( !exists("meps_mandate") ) {
  # Read in data conditional on mandate ---------------------------------------#
  if (mandate_starts == as.Date("2019-07-01")) {
    meps_mandate <- data.table::fread(
      here::here("data_out", "meps", "meps_mandate_all.csv") )
  } else {
    meps_mandate <- data.table::fread(
      here::here("data_out", "meps", "meps_mandate_10.csv") ) } }


#------------------------------------------------------------------------------#
### Function to merge LHS data with political labels ---------------------------
join_polit_labs <- function(data_in) {
  if ( "natparty_id" %in% names(data_in) ) {
    # get national party labels
    data_in <- merge(x = data_in,
                     y = national_parties[, list(identifier, national_party = label)],
                     by.x = "natparty_id", by.y = "identifier",
                     all.x = TRUE, all.y = FALSE) }

  if ( "polgroup_id" %in% names(data_in) ) {
    # get political groups labels
    data_in <- merge(x = data_in,
                     y = political_groups[, list(identifier, political_group = label)],
                     by.x = "polgroup_id", by.y = "identifier",
                     all.x = TRUE, all.y = FALSE) }
  return(data_in) }

### Function to merge LHS data with MEPs' names --------------------------------
join_meps_names <- function(data_in) {
  # get MEPs' names
  data_in <- merge(x = data_in,
                   y = unique(meps_mandate[, list(pers_id, mep_name)]),
                   by = "pers_id",
                   all.x = TRUE, all.y = FALSE)
  return(data_in) }


### Function to merge LHS data with MEPs' countries ----------------------------
join_meps_countries <- function(data_in) {
  # get MEPs' names
  data_in <- merge(x = data_in,
                   y = country_dict,
                   by = "country_id",
                   all.x = TRUE, all.y = FALSE)
  return(data_in) }
