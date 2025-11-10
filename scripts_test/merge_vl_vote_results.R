###--------------------------------------------------------------------------###
# Merge Voting Lists from Policy Advisors with Vote Results --------------------
###--------------------------------------------------------------------------###


#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c(
  "data.table", "dplyr", "here", "lubridate", "janitor", "stringi", "stringr",
  "tidyr", "tidyselect") )


#------------------------------------------------------------------------------#
## PAs' VLs: Collect, Append, and Clean ----------------------------------------
# Paths to files --------------------------------------------------------------#
vl_files <- sort(
  list.files(path = "~/Documents/github/renew_parl_work/data_out/vl_h/",
             pattern = ".csv")
  )

# Read them all in and append them --------------------------------------------#
vl_dt <- lapply(X = vl_files, FUN = function(i_csv) {
  data.table::fread(input = paste0(
    "~/Documents/github/renew_parl_work/data_out/vl_h/", i_csv) )
} ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::mutate(date = as.character(date))

# Filter to just RCV, add IDs, and subset -------------------------------------#
vl_tmp <- vl_dt |>
  dplyr::filter(grepl(pattern = "RCV", x = rcv_etc)) |>
  dplyr::arrange(date, vt_order, row_id) |>
  dplyr::mutate(
    rcv_order = dplyr::row_number(),
    .by = date) |>
  dplyr::mutate(
    n_rcv = dplyr::n(),
    .by = c(date, vt_order) ) |>
  dplyr::select(date, vt_order, row_id, rcv_order, n_rcv, vt_title,
                subject_of_the_amendment, am_no, author)

# Get a vector of VLs dates ---------------------------------------------------#
activity_dates <- unique(vl_dt$date)


#------------------------------------------------------------------------------#
## Vote Results ----------------------------------------------------------------
## Read in all vote results
votes_dt <- data.table::fread(
  input = "data_out/votes/votes_dt_10.csv",
  key = c("activity_date", "activity_order"))

votes_dt[
  !headingLabel_en %in% c("", "Other motions for resolutions",
                          "Decision to refuse discharge", "Draft legislative act",
                          "Draft legislative resolution"),
  .N,
  keyby = list(headingLabel_en, inverse_consists_of)]

## Get the order of the VOTE ITEMS by DAY --------------------------------------
vote_order <- votes_dt |>
  # Look just at RCV
  dplyr::filter(
    grepl(pattern = "ROLLCALL", x = decision_method)
    & activity_date %in% activity_dates
    ) |>
  # 'activity_order' rank the votes within 'inverse_consists_of' - provided by EP
  dplyr::arrange(activity_date, inverse_consists_of, activity_order) |>
  dplyr::select(activity_date, activity_start_date, activity_order,
         inverse_consists_of) |>
  # Fill the timestamp
  dplyr::group_by(activity_date, inverse_consists_of) |>
  tidyr::fill(activity_start_date, .direction = "downup") |>
  dplyr::ungroup() |>
  # Just get the first item for each 'inverse_consists_of'
  dplyr::filter(activity_order == 1) |>
  # Now sort the VOTE ITEMS by time stamp
  dplyr::arrange(activity_start_date) |>
  # Get the order of the VOTE ITEMS WITHIN each DAY
  dplyr::mutate(vote_itm_order = dplyr::row_number(), .by = activity_date) |>
  dplyr::select(-c(activity_date,
                   activity_start_date, activity_order))

vote_order |>
  left_join(
    y = votes_dt[grepl(pattern = "ROLLCALL", x = decision_method),],
    by = "inverse_consists_of"
  ) |>
  dplyr::arrange(activity_date, vote_itm_order, activity_order) |> View()


votes_rcv <- votes_dt[
  grepl(pattern = "ROLLCALL", x = decision_method),
  list(plenary_id, activity_id, activity_date = as.character(activity_date),
       activity_start_date, activity_order,
       headingLabel_fr, referenceText_fr, activity_label_fr, comment_fr,
       decision_outcome, inverse_consists_of)
]

p = votes_rcv |>
  dplyr::arrange(activity_date, inverse_consists_of, activity_order) |>
  dplyr::mutate(
    activity_start_date2 = lubridate::ymd_hms(activity_start_date,
                                              tz = "Europe/Brussels") ) |>
  dplyr::group_by(activity_date, inverse_consists_of) |>
  tidyr::fill(activity_start_date2, .direction = "downup") |>
  dplyr::ungroup() |>
  dplyr::mutate(
    activity_start_date2 = data.table::fifelse(
      test = is.na(activity_start_date)
      & (activity_order != min(activity_order)),
      yes = dplyr::lag(activity_start_date2) + lubridate::seconds(1),
      no = activity_start_date2),
    .by = c(activity_date, inverse_consists_of))
p |>
  group_by(activity_start_date2) |>
  count() |>
  arrange(desc(n))

votes_rcv[
  order(activity_date, activity_order),
  ':='(
  rcv_order = seq_len(.N)
), by = list(activity_date)
][,':='(
  n_rcv = .N
), by = list(inverse_consists_of)
]





plenary_session_docs_votes <- data.table::fread(
  "data_out/votes/plenary_session_docs_votes.csv")
plenary_session_docs_votes |>
  select(document_date, inverse_is_part_of_number,
         inverse_is_part_of_inverse_recorded_in_a_realization_of_id)

plenary_session_docs_rcv <- data.table::fread(
  "data_out/rcv/plenary_session_docs_rcv_10.csv")
plenary_session_docs_rcv |>
  select(document_date, inverse_is_part_of_number,
         inverse_is_part_of_inverse_recorded_in_a_realization_of_id)








p = vl_tmp |>
  dplyr::full_join(
    y = votes_rcv[activity_date %in% activity_dates],
    by = c(
      "date" = "activity_date",
      "rcv_order" = "rcv_order"
      # "n_rcv" = "n_rcv"
      )
  )
