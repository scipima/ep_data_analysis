###--------------------------------------------------------------------------###
# Get Vote Files & Process -----------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' This script collects all the vote files.
#' Having collected them, the script proceeds to process them and store the data on disk.
#------------------------------------------------------------------------------#

# Hard code the start of the mandate ------------------------------------------#
if ( !exists("mandate_starts") ) {
  mandate_starts <- "2024-07-14"
}

# Load parallel API function --------------------------------------------------#
source(file = here::here("scripts_r", "parallel_api_calls.R"))

#------------------------------------------------------------------------------#
## GET/meetings/{event-id}//meetings/{event-id}/vote-results -------------------
# Returns all vote results in a single EP Meeting -----------------------------#
# EXAMPLE: "https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-2024-04-23/vote-results?format=application%2Fld%2Bjson&offset=0"

# Load data conditional on mandate --------------------------------------------#
if ( exists("today_date") ) {
  pl_meetings <- data.table::fread(
    file = here::here("data_out", "meetings", "pl_meetings_10.csv") ) |>
    dplyr::filter(activity_id == activity_id_today)
} else if ( !exists("today_date")
            && mandate_starts == as.character("2019-07-01") ) {
  pl_meetings <- data.table::fread(
    file = here::here("data_out", "meetings", "pl_meetings_all.csv"))
} else if ( !exists("today_date")
            && mandate_starts == as.character("2024-07-14") ) {
  pl_meetings <- data.table::fread(
    file = here::here("data_out", "meetings", "pl_meetings_10.csv") )
} else {
  stop("You need to specify a valid time period: today; or the starting day for an EP mandate (e.g. for the 10th mandate, '2024-07-14')")
}

### API calls with conditional parallelization --------------------------------
# Build URLs for vote results
api_urls <- paste0(
  "https://data.europarl.europa.eu/api/v2/meetings/",
  pl_meetings$activity_id,
  "/vote-results?format=application%2Fld%2Bjson&offset=0"
)

# Use parallel processing for multiple calls, sequential for single/daily runs
use_parallel <- !exists("today_date") && length(api_urls) > 1

if (use_parallel) {
  cat("Multiple vote results API calls detected - using parallel processing\n")
  results <- parallel_api_calls(
    urls = api_urls,
    capacity = 220, # Conservative rate: ~55 calls per minute
    fill_time_s = 60,
    show_progress = TRUE,
    extract_data = TRUE
  )
  resp_list <- results$responses
} else {
  cat("Single API call or daily run detected - using sequential processing\n")
  results <- parallel_api_calls(
    urls = api_urls,
    capacity = 220,
    fill_time_s = 60,
    show_progress = FALSE,
    extract_data = TRUE
  )
  resp_list <- results$responses
}


# store tmp list if code breaks down the line ---------------------------------#
if ( !exists("today_date")
     && mandate_starts == as.character("2024-07-14") ) {
  readr::write_rds(x = resp_list, file = here::here(
    "data_out", "votes", "meetings_voteresults.rds") )
  # list_tmp <- readr::read_rds(file = here::here(
  #   "data_out", "votes", "meetings_voteresults.rds") )
}


#------------------------------------------------------------------------------#
### activity_order: Extract Vote ID and Number ---------------------------------
voteids_number <- lapply(
  X = resp_list,
  FUN = function(i_df) {
    if ( any(grepl(pattern = "activity_order", x = names(i_df) ) ) ) {
      df_tmp <- i_df |>
        dplyr::select(id, vote_id = activity_id,
                      dplyr::contains("activity_order"))
    } }
) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::distinct()


#------------------------------------------------------------------------------#
### consists_of: Extract Vote ID and RCV ID -------------------------------------------------
voteids_rcvids <- lapply(
  X = resp_list,
  FUN = function(i_df) {
    if ("consists_of" %in% names(i_df) ) {
      df_tmp <- i_df |>
        dplyr::select(vot_evnt_id = id, vot_id = activity_id, consists_of) |>
        tidyr::unnest(consists_of) |>
        dplyr::mutate(
          rcv_id = as.integer(
            gsub(pattern = "^.*-DEC-", replacement = "", x = consists_of) )
        )
    } }
) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::distinct()

# Write data conditional on mandate -------------------------------------------#
if ( exists("today_date") ) {
  data.table::fwrite(x = voteids_rcvids, file = here::here(
    "data_out", "votes", "voteids_rcvids_today.csv") )
} else if ( !exists("today_date")
            && mandate_starts == as.character("2019-07-01") ) {
  data.table::fwrite(x = voteids_rcvids, file = here::here(
    "data_out", "votes", "voteids_rcvids_all.csv") )
} else if ( !exists("today_date")
            && mandate_starts == as.character("2024-07-14") ) {
  data.table::fwrite(x = voteids_rcvids, file = here::here(
    "data_out", "votes", "voteids_rcvids_10.csv") ) }


#------------------------------------------------------------------------------#
### Extract votes and their titles ---------------------------------------------
cols_languages_end <- c("_el", "_bg", "_de", "_hu", "_cs", "_ro", "_et", "_nl",
                        "_fi", "_mt", "_sl", "_sv", "_pt", "_it", "_lt", "_es",
                        "_da", "_pl", "_sk", "_lv", "_hr", "_ga")

votes_labels <- lapply(
  X = resp_list,
  FUN = function(i_df) {
    if ("activity_label" %in% names(i_df) ) {
      df_tmp <- i_df |>
        dplyr::select(activity_id, activity_label) |>
        tidyr::unnest_wider(col = activity_label, names_sep = "_") |>
        dplyr::select(-dplyr::ends_with( cols_languages_end ) ) |>
        dplyr::mutate(
          dplyr::across(
            .cols = tidyselect::everything(),
            .fns = as.character) )
    } } ) |>
  # To suppress class incompatibility warnings, add `, ignore.attr=TRUE` below
  data.table::rbindlist(use.names=TRUE, fill=TRUE) |>
  dplyr::distinct() |>
  janitor::clean_names()

# Clean cols
votes_labels[, vote_id := as.character(gsub(
  pattern = "(MTG.PL.\\d{4}.\\d{2}.\\d{2}.VOT.ITM.\\d{5,}).*",
  replacement = "\\1", x = activity_id, perl=TRUE) ) ]

if ( all( c("activity_label_fr", "activity_label_mul") %in% names(votes_labels) ) ) {
  votes_labels[, activity_label_fr := ifelse(
    test = is.na(activity_label_fr),
    yes = activity_label_mul, no = activity_label_fr) ]
}

# Rename cols for future merge
data.table::setnames(x = votes_labels,
                     old = c("activity_label_en", "activity_label_fr",
                             "activity_label_mul"),
                     new = c("vote_label_en", "vote_label_fr", "vote_label_mul"),
                     skip_absent = TRUE)

# Write data conditional on mandate -------------------------------------------#
if ( exists("today_date")) {
  data.table::fwrite(x = votes_labels, file = here::here(
    "data_out", "votes", "votes_labels_today.csv") )
} else if ( !exists("today_date")
            && mandate_starts == as.character("2019-07-01")) {
  data.table::fwrite(x = votes_labels, file = here::here(
    "data_out", "votes", "votes_labels_all.csv") )
} else if ( !exists("today_date")
            && mandate_starts == as.character("2024-07-14") ) {
  data.table::fwrite(x = votes_labels, file = here::here(
    "data_out", "votes", "votes_labels_10.csv") ) }


#------------------------------------------------------------------------------#
### Votes and Doc IDs ----------------------------------------------------------
votes_based_on_a_realization_of <- lapply(
  X = resp_list,
  FUN = function(i_df) {
    if ( "based_on_a_realization_of" %in% names(i_df)) {
      i_df |>
        dplyr::select(vote_id = activity_id, based_on_a_realization_of) |>
        tidyr::unnest(based_on_a_realization_of)
    } } ) |>
  data.table::rbindlist()
# Check
# votes_based_on_a_realization_of[, .N, by = list(vote_id)][order(N)]

if ( nrow(votes_based_on_a_realization_of) > 0L ) {
  # create temporary duplicate col for string processing
  votes_based_on_a_realization_of[, identifier2 := gsub(
    pattern="eli/dl/doc/", replacement = "",
    x = based_on_a_realization_of, fixed = TRUE)]
  # invert the orders of the groups
  votes_based_on_a_realization_of[, doc_id := gsub(
    pattern = "(^[A-Z]{1,2}.\\d{1,2}.)(\\d{4}).(\\d{4})",
    replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
  # delete -
  votes_based_on_a_realization_of[, doc_id := gsub(pattern = "(?<=[A-Z]).",
                                                   replacement = "", x = doc_id, perl = T)]
  # treat RC separately
  votes_based_on_a_realization_of[, doc_id := gsub(pattern = "^RC",
                                                   replacement = "RC.B", x = doc_id, perl = T)]
  # slash at the end
  votes_based_on_a_realization_of[, doc_id := gsub(pattern = "(?<=.\\d{4}).",
                                                   replacement = "/", x = doc_id, perl = T)]
  # Delete cols
  votes_based_on_a_realization_of[, c("identifier2") := NULL]
  # sapply(votes_based_on_a_realization_of, function(x) sum(is.na(x))) # check


  # Write data conditional on mandate -------------------------------------------#
  if ( exists("today_date")) {
    data.table::fwrite(x = votes_based_on_a_realization_of, file = here::here(
      "data_out", "votes", "votes_based_on_a_realization_of_today.csv") )
  } else if ( !exists("today_date")
              && mandate_starts == as.character("2019-07-01") ) {
    data.table::fwrite(x = votes_based_on_a_realization_of, file = here::here(
      "data_out", "votes", "votes_based_on_a_realization_of_all.csv") )
  } else if ( !exists("today_date")
              && mandate_starts == as.character("2024-07-14") ) {
    data.table::fwrite(x = votes_based_on_a_realization_of, file = here::here(
      "data_out", "votes", "votes_based_on_a_realization_of_10.csv") ) }
}



#------------------------------------------------------------------------------#
### Votes and Vote Order -------------------------------------------------------
votes_recorded_in_a_realization_of <- lapply(
  X = resp_list,
  FUN = function(i_df) {
    if ( "recorded_in_a_realization_of" %in% names(i_df)) {
      i_df |>
        dplyr::select(vote_id = activity_id, recorded_in_a_realization_of) |>
        tidyr::unnest(recorded_in_a_realization_of)
    } } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE)

if ( nrow(votes_recorded_in_a_realization_of) > 0L ) {
  votes_recorded_in_a_realization_of <- votes_recorded_in_a_realization_of |>
    dplyr::mutate(vote_order = as.integer(
      gsub(pattern = "^.*VOT.ITM.", replacement = "",
           x = recorded_in_a_realization_of) ) ) |>
    dplyr::arrange(vote_order)

  # Write data conditional on mandate -------------------------------------------#
  if ( exists("today_date")) {
    data.table::fwrite(x = votes_recorded_in_a_realization_of, file = here::here(
      "data_out", "votes", "votes_recorded_in_a_realization_of_today.csv") )
  } else if ( !exists("today_date")
              && mandate_starts == as.character("2019-07-01") ) {
    data.table::fwrite(x = votes_recorded_in_a_realization_of, file = here::here(
      "data_out", "votes", "votes_recorded_in_a_realization_of_all.csv") )
  } else if ( !exists("today_date")
              && mandate_starts == as.character("2024-07-14") ) {
    data.table::fwrite(x = votes_recorded_in_a_realization_of, file = here::here(
      "data_out", "votes", "votes_recorded_in_a_realization_of_10.csv") ) }
}


#------------------------------------------------------------------------------#
### Votes and Procedures -------------------------------------------------------
# Drop NULL items
resp_list = resp_list[!sapply(X = resp_list, is.null)]

inverse_consists_of = vector(mode = "list", length = length(resp_list))
for (i_vot in seq_along(resp_list) ) {
  if ("inverse_consists_of" %in% names(resp_list[[i_vot]]) ) {
    df_tmp = resp_list[[i_vot]][, c("id", "inverse_consists_of")]
    dt_tmp = data.table::rbindlist(
      l = setNames(object = df_tmp$inverse_consists_of, nm = df_tmp$id),
      use.names = TRUE, fill = TRUE, idcol = "event_vot_itm_id")
    dt_tmp = dt_tmp[ grepl(pattern = "eli/dl/proc/", x = id, fixed = TRUE),
                     list(process_id = id, event_vot_itm_id) ]
    inverse_consists_of[[i_vot]] = dt_tmp
  }
}
inverse_consists_of = data.table::rbindlist(inverse_consists_of,
                                            use.names = TRUE, fill = TRUE)

if ( nrow(inverse_consists_of) > 0L ) {

  # Write data conditional on mandate -------------------------------------------#
  if ( exists("today_date")) {
    data.table::fwrite(x = inverse_consists_of, file = here::here(
      "data_out", "votes", "votes_inverse_consists_of_today.csv") )
  } else if ( !exists("today_date")
              && mandate_starts == as.character("2019-07-01") ) {
    data.table::fwrite(x = inverse_consists_of, file = here::here(
      "data_out", "votes", "votes_inverse_consists_of_all.csv") )
  } else if ( !exists("today_date")
              && mandate_starts == as.character("2024-07-14") ) {
    data.table::fwrite(x = inverse_consists_of, file = here::here(
      "data_out", "votes", "votes_inverse_consists_of_10.csv") ) }

  source(file = here::here("scripts_r", "voteresults_procedures.R"), echo = TRUE)
}
