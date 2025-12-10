###--------------------------------------------------------------------------###
# EP MEETINGS Activities -------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' The script grabs the list of all Plenary Sessions Meetings Activities from the EP API.
#------------------------------------------------------------------------------#

# rm(list = ls())


#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c(
  "data.table", "dplyr", "ggplot2", "httr2", "here", "lubridate",
  "janitor", "jsonlite", "stringi", "tidyr", "tidyselect") )


#------------------------------------------------------------------------------#
## Functions -------------------------------------------------------------------
# Load join functions ---------------------------------------------------------#
source(file = here::here("scripts_r", "join_functions.R") )

# Calculate Majorities --------------------------------------------------------#
source(file = here::here("scripts_r", "get_majority.R") )


#------------------------------------------------------------------------------#
## Parameters ------------------------------------------------------------------
# Hard code the start of the mandate ------------------------------------------#
if ( !exists("mandate_starts") ) {
  mandate_starts <- as.Date("2024-07-14") }

#------------------------------------------------------------------------------#
# Hard code Groups
renew_group_ids <- political_groups$identifier[
  political_groups$label == "Renew"]


#------------------------------------------------------------------------------#
## Data ------------------------------------------------------------------------
# Load data conditional on mandate --------------------------------------------#
meps_current <- data.table::fread(here::here(
  "data_out", "meps", "meps_current.csv") )

persids_current_renew = meps_current$pers_id[
  meps_current$polgroup_id %in% renew_group_ids
]


#------------------------------------------------------------------------------#
## GET/meetings/{event-id} - Attendance ----------------------------------------
# Returns a single EP Plenary Session document for the specified doc ID
# EXAMPLE: https://data.europarl.europa.eu/api/v2/meetings/MTG-PL-2023-07-12?format=application%2Fld%2Bjson


# Split the vector in chunks of size 50 each
chunk_size <- 50L
pers_ids_chunks <- split(
  x = persids_current_renew,
  f = ceiling(seq_along(persids_current_renew) / chunk_size)
)

# loop to get all decisions ---------------------------------------------------#
list_tmp <- vector(mode = "list", length = length(pers_ids_chunks) )
for ( i_param in seq_along(pers_ids_chunks) ) {
  print(i_param)
  # Create an API request
  req <- httr2::request(
    paste0("https://data.europarl.europa.eu/api/v2/speeches?parliamentary-term=10&person-id=",
           paste0(pers_ids_chunks[[i_param]], collapse = ","),
           "&search-language=en&language=en&format=application%2Fld%2Bjson&offset=0") )
  # Add time-out and ignore error
  resp <- req |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
    httr2::req_error(is_error = ~FALSE) |>
    httr2::req_throttle(30 / 60) |>
    httr2::req_perform()
  # If not an error, download and make available in ENV
  if ( httr2::resp_status(resp) == 200L) {
    resp_body <- resp |>
      httr2::resp_body_json(simplifyDataFrame = TRUE) |>
      purrr::pluck("data")
    list_tmp[[i_param]] <- resp_body
  } else if ( exists("today_date") ) {
    stop("API request failed. Data is not available yet. Please try again later.")
  }
  # remove objects
  rm(req, resp, resp_body)
}


speech_duration_list = vector(mode = "list", length = length(list_tmp))
for (i_tmp in seq_along(list_tmp)) {
  df_tmp = list_tmp[[i_tmp]]

  if ( is.data.frame(df_tmp$activity_label)
       && dim(df_tmp$activity_label)[2] == 1 ) {
    df_tmp$activity_label = unlist(df_tmp$activity_label)
  }

  if( is.data.frame( df_tmp$had_participation ) &&
      dim(df_tmp$had_participation)[2] > 1 ) {
    had_participation = df_tmp$had_participation
    had_participation$id = gsub(pattern = "eli/dl/participation/|_.*",
                                replacement = "", x = had_participation$id,
                                perl = TRUE)
  }

  df_tmp = df_tmp[, c("activity_id", "had_activity_type", "activity_date",
                      "activity_start_date", "activity_end_date", "activity_label")]

  # Merge
  df_tmp = merge(x = df_tmp, y = had_participation,
                 by.x = "activity_id", by.y = "id", all = TRUE)

  # Store results
  speech_duration_list[[i_tmp]] = df_tmp

  # Remove objects
  rm(df_tmp, had_participation)
}

speech_duration = data.table::rbindlist(l = speech_duration_list,
                                        use.names = TRUE, fill = TRUE)
speech_duration = speech_duration |>
  dplyr::mutate(
    activity_start_date = lubridate::as_datetime(activity_start_date,
                                                 tz = "Europe/Brussels"),
    activity_end_date = lubridate::as_datetime(activity_end_date,
                                               tz = "Europe/Brussels"),
    time_diff_seconds = round(activity_end_date - activity_start_date, digits = 1),
    pers_id = as.integer( gsub(pattern = "person/", replacement = "",
                               x = had_participant_person, fixed = TRUE) ) ) |>
  join_meps_names() |>
  dplyr::filter(
    !participation_role %in% "def/ep-roles/CHAIR" # no CHAIR
    & !grepl(pattern = "One-minute speeches", x = activity_label) # no one_minute speeches
    )

# Aggregate
speech_duration_tot = speech_duration |>
  dplyr::filter(pers_id %in% persids_current_renew) |>
  dplyr::summarise(total_duration = sum(time_diff_seconds, na.rm = TRUE),
                   .by = c(pers_id, mep_name))

# Check that time diff is in seconds
if ( attr(x = speech_duration_tot$total_duration, which = "units") == "secs") {
  print("The time difference is in seconds - You're good to go")
} else {
  stop("The time difference is NOT in seconds - Check again!")
}


#------------------------------------------------------------------------------#
## Plenary Data ----------------------------------------------------------------
### RCV ------------------------------------------------------------------------
meps_rcv_mandate <- data.table::fread(
  file = here::here("data_out", "meps_rcv_mandate_10.csv"),
  verbose = TRUE, key = c("rcv_id", "pers_id"),
  na.strings = c(NA_character_, "") )
# Create a flag for whether MEP was in the House
meps_rcv_mandate[, `:=`(
  has_voted = data.table::fifelse(
    test = result >= -2L,
    yes = 1L, no = 0L) ) ]

### Votes ----------------------------------------------------------------------
pl_votes <- data.table::fread(
  file = here::here("data_out", "votes", "pl_votes_10.csv"),
  select = c("activity_date", "rcv_id", "mandate", "number_of_attendees",
             "number_of_votes_abstention", "number_of_votes_against",
             "number_of_votes_favor"),
  na.strings = c(NA_character_, "") )

### Attendance -----------------------------------------------------------------
pl_attendance <- data.table::fread(
  file = here::here("data_out", "attendance", "pl_attendance_10.csv"),
  verbose = TRUE, key = c("activity_date", "pers_id"),
  na.strings = c(NA_character_, "") )


#------------------------------------------------------------------------------#
# Calculate n. RCVs, days where RCVs were held, and tot RCV days --------------#
has_voted = meps_rcv_mandate |>
  # Get activity date from VOTES
  dplyr::left_join(
    y = pl_votes[, list(rcv_id, activity_date = as.Date(activity_date))],
    by = "rcv_id"
  ) |>
  dplyr::mutate(
    # Get total number of days where there was at least 1 RCV
    tot_rcv_days = n_distinct(activity_date),
    # Create a flag for EP official votes
    is_truevote = ifelse(test = result >= -1L, yes = 1L, no = 0L) ) |>
  # Get total number of EP official votes
  dplyr::mutate(sum_rcv = sum(is_truevote, na.rm = TRUE), .by = pers_id) |>
  # Get unique combination of these vars
  dplyr::distinct(activity_date, pers_id, has_voted, sum_rcv, tot_rcv_days) |>
  # Aggregate vars of interest
  dplyr::summarise(sum_rcv = mean(sum_rcv),
                   sum_days_rcv = sum(has_voted, na.rm = TRUE),
                   tot_rcv_days = mean(tot_rcv_days),
                   .by = pers_id)

# Calculate presence from Attendance Registers --------------------------------#
is_present = pl_attendance[, list(pers_id, is_present,
                     activity_date = as.Date(activity_date))] |>
  dplyr::mutate(tot_attendance_days = n_distinct(activity_date)) |>
  dplyr::summarise(sum_present = sum(is_present, na.rm = TRUE),
                   tot_attendance_days = mean(tot_attendance_days),
                   .by = pers_id)

# Merge -----------------------------------------------------------------------#
voted_present = has_voted |>
  dplyr::filter(pers_id %in% persids_current_renew) |>
  dplyr::left_join(
    y = is_present,
    by = "pers_id"
  ) |>
  dplyr::arrange(desc(sum_days_rcv))


#------------------------------------------------------------------------------#
voted_present = voted_present |>
  full_join(
    y = speech_duration_tot,
    by = "pers_id"
  ) |>
  dplyr::mutate(total_duration = round(as.numeric(total_duration), digits = 1)) |>
  left_join(
    y = meps_current[, list(pers_id, country_id, polgroup_id, natparty_id)],
    by = "pers_id"
  ) |>
  join_polit_labs() |>
  join_meps_countries() |>
  dplyr::select(mep_name, national_party, country_iso3c, sum_rcv, sum_days_rcv,
                tot_rcv_days, sum_present, tot_attendance_days, total_duration) |>
  dplyr::arrange(desc(sum_days_rcv))



#------------------------------------------------------------------------------#
# save to disk Excel and upload ------------------------------------------------
dir.create(path = here::here("data_out", "speeches", "xlsx"),
           showWarnings = FALSE, recursive = TRUE)
writexl::write_xlsx(
  x = list(
    voted_present = voted_present,
    speech_duration = speech_duration[, list(mep_name, activity_id,
                                             had_activity_type, activity_date,
                                             activity_start_date, activity_end_date,
                                             activity_label, time_diff_seconds) ]
    ),
  path = here::here("data_out", "speeches", "xlsx", "speeches.xlsx"),
  format_headers = TRUE)


#------------------------------------------------------------------------------#
# MEPs' votes by day  ----------------------------------------------------------

has_voted_byday = meps_rcv_mandate |>
  dplyr::filter(pers_id %in% persids_current_renew) |>
  # Get activity date from VOTES
  dplyr::left_join(
    y = pl_votes[, list(rcv_id, activity_date = as.Date(activity_date))],
    by = "rcv_id"
  ) |>
  dplyr::mutate(
    # Create a flag for EP official votes
    is_truevote = ifelse(test = result >= -1L, yes = 1L, no = 0L)
    ) |>
  # Get total votes by MEPs by day
  dplyr::mutate(sum_rcv_byday = sum(is_truevote, na.rm = TRUE),
                .by = c(pers_id, activity_date) ) |>
  dplyr::mutate(
    # Get total number of days where there was at least 1 RCV
    tot_rcv_byday = n_distinct(rcv_id), .by = activity_date
  ) |>
  dplyr::mutate(vote_rate_byday = sum_rcv_byday / tot_rcv_byday) |>
  # Get unique combination of these vars
  dplyr::distinct(activity_date, pers_id, has_voted, sum_rcv_byday, tot_rcv_byday,
                  vote_rate_byday) |>
  join_meps_names()

has_voted_byday |>
  ggplot(aes(x = factor(activity_date), y = forcats::fct_rev(mep_name),
             fill = vote_rate_byday)) +
  geom_tile() +
  scale_fill_distiller(direction = 1, type = "div", palette = 7) +
  labs(x="Plenary Dates", y="", fill = "Vote Rate\nby Date") +
  theme_minimal() +
  theme(
    legend.title = element_text(size = 7),
    legend.text = element_text(size = 6),
    axis.text.y = element_text(size = 5),
    axis.text.x = element_text(angle = 45, hjust = 1, vjust = 1, size = 5),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank()
  )
ggsave(filename = here::here("figures", "meps_votes_byday.pdf"), dpi = 300,
       height = 19, width = 26.7, device = "pdf", units = "cm")


#------------------------------------------------------------------------------#
# MEPs' votes by day  ----------------------------------------------------------
renew_voting_meps_share = has_voted_byday |>
  dplyr::mutate(
    renew_total_meps = dplyr::n_distinct(pers_id), .by = activity_date
  ) |>
  filter(
    # Since September 2025
    activity_date >= as.Date("2025-09-01")
    # Just Presence
    & has_voted == 1L
    ) |>
  dplyr::summarise(
    renew_voting_meps = dplyr::n_distinct(pers_id),
    renew_total_meps = mean(renew_total_meps),
    .by = activity_date
  ) |>
  dplyr::mutate(
    renew_voting_meps_share = round(renew_voting_meps / renew_total_meps * 100, digits = 1)
    ) |>
  dplyr::arrange(activity_date)

renew_voting_meps_share |>
  knitr::kable(align = "c")


#------------------------------------------------------------------------------#
# Print summary statistics -----------------------------------------------------
cat("\n=====\nPlenary Attendance Summary\n=====\n")
cat("First attendance date:", as.character(min(pl_attendance$activity_date)), "\n")
cat("Last attendance date:", as.character(max(pl_attendance$activity_date)), "\n")
cat("Total number of days:", length(unique(pl_attendance$activity_date)), "\n")
cat("=====\n")
