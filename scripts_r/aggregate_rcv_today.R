###--------------------------------------------------------------------------###
# Aggregate EP RCV - Today -----------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
library(data.table)

# today_date <- "20250618" # test


# source functions ------------------------------------------------------------#
source(file = here::here("scripts_r", "get_majority.R") )
source(file = here::here("scripts_r", "join_functions.R") )
source(file = here::here("scripts_r", "cohesionrate_function.R") )


#------------------------------------------------------------------------------#
## Today's Data ----------------------------------------------------------------
# RCV
if ( !exists("meps_rcv_today") ) {
    ## Read in data -----------------------------------------------------------#
    meps_rcv_today <- data.table::fread(file = here::here(
        "data_out", "daily", paste0("rcv_today_", today_date, ".csv") ),
        na.strings = c(NA_character_, "") ) }
# Votes
if ( !exists("votes_dt") ) {
    ## Read in data -----------------------------------------------------------#
    votes_dt <- data.table::fread(file = here::here(
        "data_out", "daily", paste0("votes_today_", today_date, ".csv") ),
        na.strings = c(NA_character_, "") ) }


# convert to factor - essential for `tabulate` in COHESION function -----------#
meps_rcv_today[, result_fct := factor(
    result,
    levels = (-3 : 1),
    labels = c("absent", "no_vote", "against", "abstain", "for") ) ]

#------------------------------------------------------------------------------#
## RCV by Political Groups -----------------------------------------------------
rcv_tally_bygroup <- meps_rcv_today[, list(tally = length(unique(pers_id))),
                                    keyby = list(rcv_id, polgroup_id,
                                                 result_fct, result) ]

# write data to disk ----------------------------------------------------------#
data.table::fwrite(x = rcv_tally_bygroup, file = here::here(
    "data_out", "aggregates", "daily", "csv",
    paste0("rcv_tally_bygroup_", today_date, ".csv") ) )

# Calculate Cohesion ----------------------------------------------------------#
cohesion_polgroup_dt <- meps_rcv_today[
    result >= -1L, # only official votes
    list(
        cohesion = round(cohesion_hn(result_fct), digits = 1) ),
    keyby = list(rcv_id, polgroup_id) ]

# reshape data and select cols for excel --------------------------------------#
rcv_tally_bygroup_wide <- data.table::dcast(
    data = rcv_tally_bygroup,
    formula = rcv_id + polgroup_id ~ result_fct,
    value.var = "tally", fill = 0, drop = FALSE) |>
    join_polit_labs() |>
    dplyr::left_join(
        y = votes_dt |>
            dplyr::select(rcv_id, doc_id, is_final,
                          dplyr::starts_with("activity_label_"),
                          dplyr::starts_with("vote_label_")),
        by = "rcv_id") |>
    dplyr::left_join(
        y = cohesion_polgroup_dt,
        by = c("polgroup_id", "rcv_id") ) |>
    dplyr::mutate(date = Sys.Date()) |>
    dplyr::arrange(rcv_id, political_group)


# Clean cols ------------------------------------------------------------------#
# You never what col will pop up first
if ("vote_label_mul" %in% names(rcv_tally_bygroup_wide) ) {
    data.table::setnames(x = rcv_tally_bygroup_wide,
                         old = "vote_label_mul", new = "vote_label")
    rcv_tally_bygroup_wide[, c("vote_label_en", "vote_label_fr") := NULL]
} else if ("vote_label_en" %in% names(rcv_tally_bygroup_wide) ) {
    data.table::setnames(x = rcv_tally_bygroup_wide,
                         old = "vote_label_en", new = "vote_label")
    rcv_tally_bygroup_wide[, c("vote_label_fr", "vote_label_mul") := NULL]
} else if ("vote_label_fr" %in% names(rcv_tally_bygroup_wide) ) {
    data.table::setnames(x = rcv_tally_bygroup_wide,
                         old = "vote_label_fr", new = "vote_label")
    rcv_tally_bygroup_wide[, c("vote_label_en", "vote_label_mul") := NULL]
}


# You never know what col will pop up first
if ("activity_label_mul" %in% names(rcv_tally_bygroup_wide) ) {
    data.table::setnames(x = rcv_tally_bygroup_wide,
                         old = "activity_label_mul", new = "activity_label")
    rcv_tally_bygroup_wide[, c("activity_label_en", "activity_label_fr") := NULL]
} else if ("activity_label_fr" %in% names(rcv_tally_bygroup_wide) ) {
    data.table::setnames(x = rcv_tally_bygroup_wide,
                         old = "activity_label_fr", new = "activity_label")
    rcv_tally_bygroup_wide[, c("activity_label_en", "activity_label_mul") := NULL]
} else if ("activity_label_en" %in% names(rcv_tally_bygroup_wide) ) {
    data.table::setnames(x = rcv_tally_bygroup_wide,
                         old = "activity_label_en", new = "activity_label")
    rcv_tally_bygroup_wide[, c("activity_label_fr", "activity_label_mul") := NULL]
}

# reorder cols
data.table::setcolorder(
    x = rcv_tally_bygroup_wide,
    neworder = c("date", "rcv_id", "doc_id", "activity_label", "vote_label",
                 "political_group", "absent", "no_vote", "for",
                 "abstain", "against", "cohesion") )


#------------------------------------------------------------------------------#
## RCV by National Parties -----------------------------------------------------
rcv_tally_bynatparty <- meps_rcv_today[, list(tally = length(unique(pers_id))),
                                       keyby = list(rcv_id, country_id, polgroup_id,
                                                    natparty_id, result_fct, result) ]

# write data to disk ------------------------------------------------------#
data.table::fwrite(x = rcv_tally_bynatparty, file = here::here(
    "data_out", "aggregates", "daily", "csv",
    paste0("rct_tally_bynatparty_", today_date, ".csv") ) )

# Calculate Cohesion ------------------------------------------------------#
cohesion_natparty_dt <- meps_rcv_today[
    result >= -1L, # only official votes
    list(
        cohesion = round(cohesion_hn(result_fct), digits = 1) ),
    keyby = list(rcv_id, country_id, polgroup_id, natparty_id) ]

# reshape data and select cols for excel ----------------------------------#
rcv_tally_bynatparty_wide <- data.table::dcast(
    data = rcv_tally_bynatparty,
    formula = rcv_id + country_id + polgroup_id + natparty_id ~ result_fct,
    value.var = "tally", fill = 0) |>
    join_polit_labs() |>
    join_meps_countries() |>
    dplyr::left_join(
        y = votes_dt |>
            dplyr::select(rcv_id, doc_id, is_final,
                          dplyr::starts_with("activity_label_"),
                          dplyr::starts_with("vote_label_")),
        by = "rcv_id") |>
    dplyr::left_join(
        y = cohesion_natparty_dt,
        by = c("country_id", "polgroup_id", "natparty_id", "rcv_id") ) |>
    dplyr::mutate(date = Sys.Date()) |>
    dplyr::arrange(rcv_id, country_iso3c, political_group, national_party)


# Clean cols ------------------------------------------------------------------#
# On some voting session, pivoting is not possible due to the row limit in excel
if ( !"no_vote" %in% names(rcv_tally_bynatparty_wide) ) {
    rcv_tally_bynatparty_wide$no_vote <- 0L }

# You never what col will pop up first
if ("vote_label_en" %in% names(rcv_tally_bynatparty_wide) ) {
    data.table::setnames(x = rcv_tally_bynatparty_wide,
                         old = "vote_label_en", new = "vote_label")
    rcv_tally_bynatparty_wide[, c("vote_label_fr", "vote_label_mul") := NULL]
} else if ("vote_label_fr" %in% names(rcv_tally_bynatparty_wide) ) {
    data.table::setnames(x = rcv_tally_bynatparty_wide,
                         old = "vote_label_fr", new = "vote_label")
    rcv_tally_bynatparty_wide[, c("vote_label_en", "vote_label_mul") := NULL]
} else if ("vote_label_mul" %in% names(rcv_tally_bynatparty_wide) ) {
    data.table::setnames(x = rcv_tally_bynatparty_wide,
                         old = "vote_label_mul", new = "vote_label")
    rcv_tally_bynatparty_wide[, c("vote_label_en", "vote_label_fr") := NULL] }

# You never what col will pop up first
if ("activity_label_en" %in% names(rcv_tally_bynatparty_wide) ) {
    data.table::setnames(x = rcv_tally_bynatparty_wide,
                         old = "activity_label_en", new = "activity_label")
    rcv_tally_bynatparty_wide[, c("activity_label_fr", "activity_label_mul") := NULL]
} else if ("activity_label_fr" %in% names(rcv_tally_bynatparty_wide) ) {
    data.table::setnames(x = rcv_tally_bynatparty_wide,
                         old = "activity_label_fr", new = "activity_label")
    rcv_tally_bynatparty_wide[, c("activity_label_en", "activity_label_mul") := NULL]
} else if ("activity_label_mul" %in% names(rcv_tally_bynatparty_wide) ) {
    data.table::setnames(x = rcv_tally_bynatparty_wide,
                         old = "activity_label_mul", new = "activity_label")
    rcv_tally_bynatparty_wide[, c("activity_label_en", "activity_label_fr") := NULL] }

# reorder cols
data.table::setcolorder(
    x = rcv_tally_bynatparty_wide,
    neworder = c("date", "rcv_id", "doc_id", "activity_label", "vote_label",
                 "country_iso3c", "political_group", "national_party",
                 "absent", "no_vote", "for", "abstain", "against", "cohesion") )


#------------------------------------------------------------------------------#
# save to disk Excel and upload ------------------------------------------------
writexl::write_xlsx(
    x = list(rcv_tally_bygroup_wide = rcv_tally_bygroup_wide,
             rcv_tally_bynatparty_wide = rcv_tally_bynatparty_wide),
    path = here::here(
        "data_out", "aggregates", "daily", "xlsx",
        paste0("rcv_tally_", today_date, ".xlsx") ),
    format_headers = TRUE)
