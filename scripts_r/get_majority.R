###--------------------------------------------------------------------------###
# Functions to Calculate Majorities --------------------------------------------
###--------------------------------------------------------------------------###

#' The script calculates majorities within the Committees, Political Groups, and National Parties.
#' If the max is a tie - i.e. there is not majority - then we drop that RCV.
#' That means that if this occurs within the House, we have no winner and therefore it is impossible to calculate which Group or Party won (as there is not House majority).
#' If the tie occurs for either Groups or Parties, then there is no majority in that entity and thus nothing can be compared (no winner, no similarity/affinity).

#------------------------------------------------------------------------------#
## House majority --------------------------------------------------------------

get_house_majority <- function(data_in = meps_rcv_mandate) {
    # Check if data_in is DT
    if ( !data.table::is.data.table(data_in) ) {
        data_in <- data.table::as.data.table(data_in) }

    # Tabulate results by RCV ID
    who_won <- data_in[, list(who_won = .N), keyby = list(rcv_id, result) ]

    # ensure that data is sorted so that subsequent filtering is correct ------#
    data.table::setorder(x = who_won, rcv_id, -who_won)

    # Check if max vote is tied -----------------------------------------------#
    # Flag duplicated and max rows
    who_won[, `:=`(is_duplicated = duplicated(who_won),
                   is_max = who_won == max(who_won)),
            by = list(rcv_id)]

    # Is the row both duplicated and max? If yes, then tied
    who_won[, is_tied := data.table::fifelse(
        test = (is_duplicated + is_max == 2L),
        yes = 1L, no = 0L)]

    if ( any(who_won$is_tied == 1L) ){
        # Apply tied to all party rows
        who_won[, to_drop := mean(is_tied, na.rm = TRUE),
                by = list(rcv_id)]
        # Retain only RCVs that are not ties
        who_won <- who_won[to_drop == 0]
    }

    # Drop temporary cols
    who_won[, c("is_duplicated", "is_tied", "is_max", "to_drop") := NULL]

    # Retain only majorities --------------------------------------------------#
    who_won <- who_won[, head(.SD, 1L), keyby = list(rcv_id) ]
}

###--------------------------------------------------------------------------###
## Groups' majority ------------------------------------------------------------

get_polgroup_majority <- function(data_in = meps_rcv_mandate) {
    # Check if data_in is DT
    if (!data.table::is.data.table(data_in)) {
        data_in <- data.table::as.data.table(data_in) }

    # Get the Groups' majority by rcv_id --------------------------------------#
    who_won <- data_in[, list(who_won = .N),
                       keyby = list(rcv_id, polgroup_id, result) ]

    # ensure that data is sorted so that subsequent filtering is correct ------#
    data.table::setorder(x = who_won, rcv_id, polgroup_id, -who_won)

    # Check if max vote is tied WITHIN each Political Group -------------------#
    # Flag duplicated and max rows
    who_won[, `:=`(is_duplicated = duplicated(who_won),
                   is_max = who_won == max(who_won)),
            by = list(rcv_id, polgroup_id)]

    # Is the row both duplicated and max? If yes, then tied
    who_won[, is_tied := data.table::fifelse(
        test = (is_duplicated + is_max == 2L),
        yes = 1L, no = 0L)]

    if ( any(who_won$is_tied == 1L) ){
        # Apply tied to all party rows
        who_won[, to_drop := mean(is_tied, na.rm = TRUE),
                by = list(rcv_id, polgroup_id)]
        # Retain only RCVs that are not ties
        who_won <- who_won[to_drop == 0]
    }

    # Drop temporary cols
    who_won[, c("is_duplicated", "is_tied", "is_max", "to_drop") := NULL]

    # Retain only majorities --------------------------------------------------#
    who_won <- who_won[, head(.SD, 1L), keyby = list(rcv_id, polgroup_id)]
}


#------------------------------------------------------------------------------#
## Parties' majority -----------------------------------------------------------

get_natparty_majority <- function(data_in = meps_rcv_mandate) {
    # Check if data_in is DT
    if (!data.table::is.data.table(data_in)) {
        data_in <- data.table::as.data.table(data_in) }

    # Get the Groups' majority by rcv_id --------------------------------------#
    who_won <- data_in[, list(who_won = .N),
                       keyby = list(rcv_id, polgroup_id, natparty_id, result) ]

    # ensure that data is sorted so that subsequent filtering is correct ------#
    data.table::setorder(x = who_won, rcv_id, polgroup_id, natparty_id, -who_won)

    # Check if max vote is tied WITHIN each Political Group -------------------#
    # Flag duplicated and max rows
    who_won[, `:=`(is_duplicated = duplicated(who_won),
                   is_max = who_won == max(who_won)),
            by = list(rcv_id, polgroup_id, natparty_id)]

    # Is the row both duplicated and max? If yes, then tied
    who_won[, is_tied := data.table::fifelse(
        test = (is_duplicated + is_max == 2L),
        yes = 1L, no = 0L)]

    if ( any(who_won$is_tied == 1L) ){
        # Apply tied to all party rows
        who_won[, to_drop := mean(is_tied, na.rm = TRUE),
                by = list(rcv_id, polgroup_id, natparty_id)]
        # Retain only RCVs that are not ties
        who_won <- who_won[to_drop == 0]
    }

    # Drop temporary cols
    who_won[, c("is_duplicated", "is_tied", "is_max", "to_drop") := NULL]

    # Retain only majorities --------------------------------------------------#
    who_won <- who_won[, head(.SD, 1L), keyby = list(rcv_id, polgroup_id, natparty_id)]
}
