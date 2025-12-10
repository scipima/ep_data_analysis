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

# Repo setup
source(file = here::here("scripts_r", "repo_setup.R") )

#------------------------------------------------------------------------------#
## Functions -------------------------------------------------------------------
source(file = here::here("scripts_r", "unnest_functions.R"))

# Load parallel API function --------------------------------------------------#
source(file = here::here("scripts_r", "parallel_api_calls.R"))


#------------------------------------------------------------------------------#
## GET/meetings/{event-id}//meetings/{event-id}/vote-results -------------------
# Returns all vote results in a single EP Meeting -----------------------------#
# EXAMPLE: "https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-2024-04-23/vote-results?format=application%2Fld%2Bjson&offset=0"

# Load data conditional on mandate --------------------------------------------#
### Load list of RCVs Plenaries ------------------------------------------------
if ( !exists("today_date") && mandate_starts == "2019-07-01" ) {
  get_api_data(
    path = here::here("data_out", "docs_pl", "pl_session_docs_all.csv"),
    script = here::here("scripts_r", "api_pl_session_docs.R"),
    varname = "pl_session_docs",
  )
} else if (!exists("today_date") && mandate_starts == "2024-07-14" ) {
  get_api_data(
    path = here::here("data_out", "docs_pl", "pl_session_docs_10.csv"),
    script = here::here("scripts_r", "api_pl_session_docs.R"),
    varname = "pl_session_docs",
  )
} else if ( !exists("today_date") # not a daily run
            && ( !exists(mandate_starts) # no mandate specified
                 | !mandate_starts %in% c("2024-07-14", "2019-07-01") ) ) {
  stop("\n=====\nYou need to specify a valid timeframe.\nThis could be either the starting date for a mandate (currently: '2019-07-01', '2024-07-14'), or today's date.\n=====\n")
}


# Extract vector of Plenary Dates ---------------------------------------------#
if ( exists("today_date") ) {
  meet_pl_ids = activity_id_today
} else {
  # Extract date
  pl_session_docs[, `:=`(
    activity_date = data.table::as.IDate(
      gsub(pattern = "PV-\\d{1,2}-|-VOT",
           replacement = "", x = identifier, perl = TRUE) ) )
  ]
  # Subset to relevant Plenary dates
  pl_session_docs_rcv = pl_session_docs[
    activity_date >= mandate_starts
    & grepl(pattern = "-VOT", x = id, fixed = TRUE)
  ]
  # Create Plenary ID
  pl_session_docs_rcv[, activity_id := paste0("MTG-PL-", activity_date)]

  # Get MEETINGS IDs
  meet_pl_ids <- sort( unique(pl_session_docs_rcv$activity_id) )
}


### API calls with conditional parallelization --------------------------------
# Build URLs for vote results
api_urls <- paste0(
  "https://data.europarl.europa.eu/api/v2/meetings/",
  meet_pl_ids,
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
    extract_data = TRUE,
    force_sequential = TRUE
  )
  resp_list <- results$responses
}

# store tmp list if code breaks down the line ---------------------------------#
if ( !exists("today_date")
     && mandate_starts == as.character("2024-07-14") ) {
  readr::write_rds(x = resp_list, file = here::here(
    "data_out", "votes", "meetings_voteresults_10.rds") )
} else if ( !exists("today_date")
            && mandate_starts == as.character("2019-07-01") ) {
  readr::write_rds(x = resp_list, file = here::here(
    "data_out", "votes", "meetings_voteresults_all.rds") )
}


#------------------------------------------------------------------------------#
### activity_order: Extract Vote ID and Number ---------------------------------
voteids_number <- lapply(
  X = resp_list,
  FUN = function(i_df) {
    if ( any(grepl(pattern = "activity_order", x = names(i_df) ) ) ) {
      i_df[, c("id", "activity_id", "activity_order")]
    } }
) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  unique()
if (nrow(voteids_number) > 0){
  data.table::setnames(x = voteids_number,
                     old = c("id", "activity_id"),
                     new = c("vot_evnt_id", "vot_id"))
}


#------------------------------------------------------------------------------#
### consists_of: Extract Vote ID and RCV ID ------------------------------------
voteids_rcvids = unnest_nested_list(
  data_list = resp_list,
  group_cols = c("id", "activity_id"),
  unnest_col = "consists_of"
) |>
  unique()
voteids_rcvids[, rcv_id := as.integer(
  gsub(pattern = "^.*-DEC-", replacement = "", x = consists_of)
)]
data.table::setnames(x = voteids_rcvids,
                     old = c("id", "activity_id"),
                     new = c("vot_evnt_id", "vot_id"))

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
### Titles ---------------------------------------------------------------------
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
votes_labels[, vot_id := as.character(gsub(
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
    "data_out", "votes", "votes_labels_10.csv") )
}


#------------------------------------------------------------------------------#
### Votes and Doc IDs ----------------------------------------------------------
votes_based_on_a_realization_of <- unnest_nested_list(
  data_list = resp_list,
  group_cols = c("id", "activity_id"),
  unnest_col = "based_on_a_realization_of"
)

# Check
# votes_based_on_a_realization_of[, .N, by = list(vot_id)][order(N)]

if ( nrow(votes_based_on_a_realization_of) > 0L ) {
  data.table::setnames(x = votes_based_on_a_realization_of,
                       old = c("id", "activity_id"),
                       new = c("vot_evnt_id", "vot_id"))

  # create temporary duplicate col for string processing
  votes_based_on_a_realization_of[, `:=`(
    identifier2 = gsub(pattern="eli/dl/doc/", replacement = "",
                        x = based_on_a_realization_of, fixed = TRUE)
    )]
  # invert the orders of the groups
  votes_based_on_a_realization_of[, `:=`(
    doc_id = gsub(pattern = "(^[A-Z]{1,2}.\\d{1,2}.)(\\d{4}).(\\d{4})",
                  replacement = "\\1\\3-\\2", x = identifier2, perl = T)
    )]
  # delete -
  votes_based_on_a_realization_of[, `:=`(
    doc_id = gsub(pattern = "(?<=[A-Z]).", replacement = "", x = doc_id, perl = T)
    )]
  # treat RC separately
  votes_based_on_a_realization_of[, `:=`(
    doc_id = gsub(pattern = "^RC", replacement = "RC.B", x = doc_id, perl = T)
    )]
  # slash at the end
  votes_based_on_a_realization_of[, `:=`(
    doc_id = gsub(pattern = "(?<=.\\d{4}).",
                  replacement = "/", x = doc_id, perl = T)
    )]
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
votes_recorded_in_a_realization_of = unnest_nested_list(
  data_list = resp_list,
  group_cols = c("id", "activity_id"),
  unnest_col = "recorded_in_a_realization_of"
) |>
  unique()


if ( nrow(votes_recorded_in_a_realization_of) > 0L ) {
  votes_recorded_in_a_realization_of[, `:=`(
    vote_order = as.integer(gsub(pattern = "^.*VOT.ITM.", replacement = "",
                                 x = recorded_in_a_realization_of) ) )
    ]
  data.table::setnames(x = votes_recorded_in_a_realization_of,
                       old = c("id", "activity_id"),
                       new = c("vot_evnt_id", "vot_id"))

  # Write data conditional on mandate -----------------------------------------#
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

votes_inverse_consists_of = vector(mode = "list", length = length(resp_list))
for (i_vot in seq_along(resp_list) ) {
  if ("inverse_consists_of" %in% names(resp_list[[i_vot]]) ) {
    df_tmp = resp_list[[i_vot]][, c("id", "inverse_consists_of")]
    dt_tmp = data.table::rbindlist(
      l = setNames(object = df_tmp$inverse_consists_of, nm = df_tmp$id),
      use.names = TRUE, fill = TRUE, idcol = "event_vot_itm_id")
    dt_tmp = dt_tmp[ grepl(pattern = "eli/dl/proc/", x = id, fixed = TRUE),
                     list(process_id = id, event_vot_itm_id) ]
    votes_inverse_consists_of[[i_vot]] = dt_tmp
  }
}
votes_inverse_consists_of = data.table::rbindlist(votes_inverse_consists_of,
                                            use.names = TRUE, fill = TRUE)

if ( nrow(votes_inverse_consists_of) > 0L ) {

  # Write data conditional on mandate -----------------------------------------#
  if ( exists("today_date")) {
    data.table::fwrite(x = votes_inverse_consists_of, file = here::here(
      "data_out", "votes", "votes_inverse_consists_of_today.csv") )
  } else if ( !exists("today_date")
              && mandate_starts == as.character("2019-07-01") ) {
    data.table::fwrite(x = votes_inverse_consists_of, file = here::here(
      "data_out", "votes", "votes_inverse_consists_of_all.csv") )
  } else if ( !exists("today_date")
              && mandate_starts == as.character("2024-07-14") ) {
    data.table::fwrite(x = votes_inverse_consists_of, file = here::here(
      "data_out", "votes", "votes_inverse_consists_of_10.csv") ) }

  source(file = here::here("scripts_r", "voteresults_procedures.R"), echo = TRUE)
}

