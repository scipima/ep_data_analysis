###--------------------------------------------------------------------------###
# Aggregate EP RCV -------------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' DESCRIPTION.
#' First, we check whether we have a `today` object in the environment.
#' If we do, we can run the daily aggregations, as that would mean that we are currently executing the RCV daily collection/analysis.
#' Else, we just run the aggregations for the entire mandate.
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
library(data.table)

# source functions ------------------------------------------------------------#
source(file = here::here("scripts_r", "get_majority.R"))
source(file = here::here("scripts_r", "join_functions.R") )
source(file = here::here("scripts_r", "cohesionrate_function.R") )


#------------------------------------------------------------------------------#
## Mandate's Data --------------------------------------------------------------
if ( !exists("meps_rcv_mandate") ) {
  # Read in data conditional on mandate ---------------------------------------#
  if (mandate_starts == as.Date("2019-07-01")) {
    meps_rcv_mandate <- data.table::fread(
      file = here::here("data_out", "meps_rcv_mandate_all.csv"),
      verbose = TRUE, key = c("rcv_id", "pers_id"))
    pl_votes <- data.table::fread(
      file = here::here("data_out", "votes", "pl_votes_all.csv") )
  } else {
    meps_rcv_mandate <- data.table::fread(
      file = here::here("data_out", "meps_rcv_mandate_10.csv"),
      verbose = TRUE, key = c("rcv_id", "pers_id") )
    pl_votes <- data.table::fread(
      file = here::here("data_out", "votes", "pl_votes_10.csv")
    )
  }
}


# Merge
meps_rcv_mandate <- merge(x = meps_rcv_mandate,
                          y = pl_votes[, list(activity_date, rcv_id)],
                          by = "rcv_id")
# meps_rcv_mandate[, country_id := NULL]
# sapply(meps_rcv_mandate, function(x) sum(is.na(x) ) )
meps_rcv_mandate[, result_fct := factor(
  result,
  levels = (-3 : 1),
  labels = c("absent", "no_vote", "against", "abstain", "for") ) ]

# Get the last configuration of the EP
data.table::fwrite(
  x = unique(meps_rcv_mandate[
    activity_date == max(activity_date, na.rm = TRUE),
    list(pers_id, natparty_id, polgroup_id, country_id) ] ),
  file = here::here("data_out", "meps", "meps_lastplenary.csv"), verbose = TRUE)


#------------------------------------------------------------------------------#
## Aggregate RCV results by Political Groups -----------------------------------

# Votes tallies by EP Groups
tally_bygroup_byrcv <- meps_rcv_mandate[
  , list(tally = length(unique(pers_id))),
  keyby = list(mandate, rcv_id, polgroup_id, result, result_fct) ]
sapply(tally_bygroup_byrcv, function(x) sum(is.na(x)))
# Votes tallies by National Parties
tally_bygroup_byparty_byrcv <- meps_rcv_mandate[
  , list(tally = length(unique(pers_id))),
  keyby = list(mandate, rcv_id, country_id, polgroup_id, natparty_id, result) ]

# Save to disk conditional on mandate -----------------------------------------#
if (mandate_starts == as.Date("2019-07-01")) {
  data.table::fwrite(x = tally_bygroup_byrcv, file = here::here(
    "data_out", "aggregates", "tally_bygroup_byrcv_all.csv"))
  data.table::fwrite(x = tally_bygroup_byparty_byrcv, file = here::here(
    "data_out", "aggregates", "tally_bygroup_byparty_byrcv_all.csv"))
} else {
  data.table::fwrite(x = tally_bygroup_byrcv, file = here::here(
    "data_out", "aggregates", "tally_bygroup_byrcv_10.csv"))
  data.table::fwrite(x = tally_bygroup_byparty_byrcv, file = here::here(
    "data_out", "aggregates", "tally_bygroup_byparty_byrcv_10.csv") ) }


#------------------------------------------------------------------------------#
## Groups' majorities ----------------------------------------------------------

# calculate majority
majority_bygroup_byrcv <- tally_bygroup_byrcv[
  result >= -1, # just consider regular votes
  list(vote_max = max(tally, na.rm = TRUE),
       votes_sum = sum(tally, na.rm = TRUE)),
  keyby = list(mandate, rcv_id, polgroup_id)]

# check for ties
majority_bygroup_byrcv[, .N, by = list(mandate, rcv_id, polgroup_id)][order(N)]

# Save to disk conditional on mandate -----------------------------------------#
if (mandate_starts == as.Date("2019-07-01")) {
  data.table::fwrite(x = majority_bygroup_byrcv, file = here::here(
    "data_out", "aggregates", "majority_bygroup_byrcv_all.csv"))
} else {
  data.table::fwrite(x = majority_bygroup_byrcv, file = here::here(
    "data_out", "aggregates", "majority_bygroup_byrcv_10.csv")) }


#------------------------------------------------------------------------------#
## Groups' wins shares ---------------------------------------------------------

whowon_house_polgroup <- merge(
  x = get_house_majority(data_in = meps_rcv_mandate[result >= -1] ),
  y = get_polgroup_majority(data_in = meps_rcv_mandate[result >= -1] ),
  by = c("rcv_id"), all = FALSE ) |>
  dplyr::mutate(is_same = as.integer( result.x == result.y ) ) |>
  dplyr::left_join(
    y = unique( meps_rcv_mandate[, list(rcv_id, mandate) ] ),
    by = "rcv_id")

# Save to disk conditional on mandate -----------------------------------------#
if (mandate_starts == as.Date("2019-07-01")) {
  data.table::fwrite(x = whowon_house_polgroup, file = here::here(
    "data_out", "aggregates", "whowon_house_polgroup_all.csv") )
} else {
  data.table::fwrite(x = whowon_house_polgroup, file = here::here(
    "data_out", "aggregates", "whowon_house_polgroup_10.csv") ) }

