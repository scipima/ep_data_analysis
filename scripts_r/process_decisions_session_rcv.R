###--------------------------------------------------------------------------###
# Process RCV Data ------------------------------------------------------------
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
#' RCV are list-cols, which in the original records are appended one below the other.
#' We replicate the same structure here.
###--------------------------------------------------------------------------###

process_decisions_session_rcv <- function( votes_raw = resp_list[[49]] ) {
  print(unique(votes_raw$activity_date)) # check where we are in the loop

  # Votes ---------------------------------------------------------------------#
  vote_cols <- c("had_voter_against", "had_voter_abstention", "had_voter_favor")
  vote_cols <- vote_cols[vote_cols %in% names(votes_raw)]

  if ( !any( grepl(pattern = "VOTE_ELECTRONIC_ROLLCALL",
                   x = votes_raw$decision_method, fixed = TRUE) ) ){
    warning("Check that the flag for RCV has not changed!") }

  # process and flatten
  rcv_vote <- lapply(
    X = setNames(object = vote_cols, nm = vote_cols),
    FUN = function(j_col) {
      votes_raw |>
        dplyr::filter( grepl(pattern = "ROLLCALL", x = decision_method) ) |>
        dplyr::select(id, notation_votingId, tidyselect::any_of(j_col)) |>
        dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
        tidyr::unnest( tidyselect::any_of(j_col) ) } ) |>
    data.table::rbindlist(use.names = FALSE, fill = FALSE, idcol = "result")
  # Grab name of col to rename
  col_torename = names(rcv_vote)[names(rcv_vote) %in% vote_cols]
  # Rename
  data.table::setnames(x = rcv_vote, old = col_torename, new = "pers_id")
  # Drop duplicates if any
  rcv_vote = unique(rcv_vote)

  # check for duplicates
  df_check <- rcv_vote[, .N, by = list(pers_id, notation_votingId)]
  if (mean(df_check$N) > 1) {
    warning("WATCH OUT: You may have duplicate records")}
  rm(df_check)


  # recode & clean ------------------------------------------------------------#
  rcv_vote[, activity_date := as.Date(gsub(pattern = "eli/dl/event/MTG-PL-|-DEC-.*",
                                           replacement = "", x = id))]
  rcv_vote[, pers_id := as.integer(
    gsub(pattern = "person/", replacement = "", x = pers_id))]
  rcv_vote[result == "had_voter_abstention", result := 0]
  rcv_vote[result == "had_voter_favor", result := 1]
  rcv_vote[result == "had_voter_against", result := -1]
  rcv_vote[, `:=`(result = as.integer(result),
                  notation_votingId = as.integer(notation_votingId) ) ]


  # return data.frame ---------------------------------------------------------#
  return(rcv_vote)
}
