###--------------------------------------------------------------------------###
# Process RCV Data ------------------------------------------------------------
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
#' RCV are list-cols, which in the original records are appended one below the other.
#' We replicate the same structure here.
#' Intentions are instead provided as separate cols.
#' Please pay attention to the fact that intentions are not always expressed.
#' However, when they are, the data has to be merged in FULL JOIN.
#' This is because some MEPs may not have a vote for a given RCV, but only an intention.
###--------------------------------------------------------------------------###

process_rcv_day <- function( votes_raw = resp_list[[49]] ) {
  print(unique(votes_raw$activity_date)) # check where we are in the loop

  # Intentions ----------------------------------------------------------------#
  vote_intention_cols <- c("had_voter_intended_abstention", "had_voter_intended_against",
                           "had_voter_intended_favor")
  vote_intention_cols <- vote_intention_cols[vote_intention_cols %in% names(votes_raw)]
  if ( length(vote_intention_cols) > 0 ) {
    rcv_vote_intention <- lapply(
      X = setNames(object = vote_intention_cols, nm = vote_intention_cols),
      FUN = function(j_col) {
        votes_raw |>
          dplyr::filter( grepl(pattern = "ROLLCALL", x = decision_method) ) |>
          dplyr::select(id, notation_votingId, tidyselect::any_of(j_col) ) |>
          dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
          tidyr::unnest( tidyselect::any_of(j_col) ) } ) |>
      data.table::rbindlist(use.names = FALSE, fill = FALSE, idcol = "vote_intention")
    which_persid <- grep(pattern = "had_voter_intended", x = names(rcv_vote_intention))
    data.table::setnames(x = rcv_vote_intention, old = which_persid, "pers_id")
    rcv_vote_intention <- unique(rcv_vote_intention) # DEFENSIVE: there may be duplicate rows

    # check for duplicates
    df_check <- rcv_vote_intention[, .N, by = list(pers_id, notation_votingId)]
    if (mean(df_check$N) > 1) {
      warning("WATCH OUT: You may have duplicate records") }
    rm(df_check)

  }


  # recode & clean ------------------------------------------------------------#
  if ( length(vote_intention_cols) > 0 ) {
    rcv_vote_intention[vote_intention == "had_voter_intended_abstention", vote_intention := 0]
    rcv_vote_intention[vote_intention == "had_voter_intended_favor", vote_intention := 1]
    rcv_vote_intention[vote_intention == "had_voter_intended_against", vote_intention := -1]
    rcv_vote_intention[, vote_intention := as.integer(vote_intention)] }

  # return data.frame ---------------------------------------------------------#
  return(rcv_vote_intention)
}
