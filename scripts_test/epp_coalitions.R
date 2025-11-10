

#------------------------------------------------------------------------------#
## Functions -------------------------------------------------------------------
# Load join functions ---------------------------------------------------------#
source(file = here::here("scripts_r", "join_functions.R") )

# Calculate Majorities --------------------------------------------------------#
source(file = here::here("scripts_r", "get_majority.R") )


#------------------------------------------------------------------------------#
# Get the Groups' majority by rcv_id ------------------------------------------#
who_won_bygroup <- get_polgroup_majority(
    data_in = meps_rcv_today[result >= -1] # exclude absent
)

epp_mjrt <- who_won_bygroup[
    polgroup_id == 7018L
][, c("polgroup_id", "who_won") := NULL]

epp_groups <- epp_mjrt[
    who_won_bygroup[polgroup_id != 7018L],
    on = "rcv_id", nomatch = NULL
][, who_won := NULL] |>
    join_polit_labs() |>
    dplyr::select(-polgroup_id)


#------------------------------------------------------------------------------#
epp_coalitions <- epp_groups |>
    tidyr::pivot_wider(names_from = political_group, values_from = i.result) |>
    dplyr::mutate(
        is_pfe_ecr = ifelse(
            test = (result != Renew) & (result != `S&D`)
            & (result == PfE & result == ECR),
            yes = 1L, no = 0L),
        is_pfe_ecr_esn = ifelse(
            test = (result != Renew) & (result != `S&D`)
            & (result == PfE & result == ECR & result == ESN),
            yes = 1L, no = 0L) ) |>
    dplyr::rename(PPE = result) |>
    data.table::as.data.table()


#------------------------------------------------------------------------------#
epp_pfe_ecr <- epp_coalitions[
    is_pfe_ecr == 1L] |>
    dplyr::left_join(
        y = pl_votes |>
            dplyr::select(dplyr::starts_with("activity_label"),
                          dplyr::starts_with("vote_label"),
                          rcv_id, doc_id, is_final),
        by = "rcv_id"
    )

# Print informative message
if (nrow(epp_pfe_ecr) > 0) {
    cat("\n=====\nToday, the EPP voted together with PfE and ECR in these RCVs:",
        unique(epp_pfe_ecr$rcv_id),
        "\n=====\n")
} else {
    cat("\n=====\nToday, the EPP did not vote together with PfE and ECR.\n=====\n")
}


#------------------------------------------------------------------------------#
epp_pfe_ecr_esn <- epp_coalitions[
    is_pfe_ecr_esn == 1L] |>
    dplyr::left_join(
        y = pl_votes |>
            dplyr::select(dplyr::starts_with("activity_label"),
                          dplyr::starts_with("vote_label"),
                          rcv_id, doc_id, is_final),
        by = "rcv_id"
    )

# Print informative message
if (nrow(epp_pfe_ecr_esn) > 0) {
    cat("\n=====\nToday, the EPP voted together with PfE and ECR and ESN in these RCVs:",
        unique(epp_pfe_ecr_esn$rcv_id),
        "\n=====\n")
} else {
    cat("\n=====\nToday, the EPP did not vote together with PfE and ECR.\n=====\n")
}


#------------------------------------------------------------------------------#
# Conditionally save output if any
if ( sum(epp_coalitions$is_pfe_ecr, na.rm = TRUE) > 0 ) {
    data.table::fwrite(x = epp_coalitions, file = here::here(
        "data_out", "daily", paste0("epp_coalitions_", today_date, ".csv") ) )
}
