
#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c(
  "data.table", "dplyr", "here", "lubridate", "janitor", "tidyr") )


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
  meps_current$polgroup_id == renew_group_ids
]


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
    test = result >= -1L,
    yes = 1L, no = 0L) ) ]

### Votes ----------------------------------------------------------------------
votes_dt <- data.table::fread(
  file = here::here("data_out", "votes", "votes_dt_10.csv"),
  select = c("activity_date", "rcv_id", "mandate", "number_of_attendees",
             "number_of_votes_abstention", "number_of_votes_against",
             "number_of_votes_favor"),
  na.strings = c(NA_character_, "") )

### Attendance -----------------------------------------------------------------
attendance_dt <- data.table::fread(
  file = here::here("data_out", "attendance", "attendance_dt_10.csv"),
  verbose = TRUE, key = c("activity_date", "pers_id"),
  na.strings = c(NA_character_, "") )


#------------------------------------------------------------------------------#
# Calculate n. RCVs, days where RCVs were held, and tot RCV days --------------#
has_voted = meps_rcv_mandate |>
  # Get activity date from VOTES
  dplyr::left_join(
    y = votes_dt[, list(rcv_id, activity_date = as.Date(activity_date))],
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
is_present = attendance_dt[, `:=`(
  tot_attendance_days = length( unique(activity_date) ) )
][, list(
  N = .N,
  tot_attendance_days = mean(tot_attendance_days) ),
  by = list(pers_id, is_present)
][, `:=`(
  is_present = ifelse(test = is_present == 1,
                      yes = "present", no = "excused") ) ] |>
  tidyr::pivot_wider(names_from = is_present, values_from = N, values_fill = 0)



# Merge: VOTES and ATTENDANCE -------------------------------------------------#
voted_present = has_voted |>
  dplyr::filter(pers_id %in% persids_current_renew) |>
  dplyr::left_join(
    y = is_present,
    by = "pers_id"
  ) |>
  dplyr::arrange(desc(sum_days_rcv))


# Get Group Majority
mjrt_polgroups = get_polgroup_majority(data_in = meps_rcv_mandate[
  polgroup_id %in% renew_group_ids
  & result >= -1L,
])

# Merge Group MJRT with individual MEPs votes & check for identity
renew_align = mjrt_polgroups[
  meps_rcv_mandate[
    pers_id %in% persids_current_renew
    & result >= -1L
  ],
  on = "rcv_id"
][, `:=`(
  is_same = as.integer(result == i.result)
)
][]

# Get average alignment by MEP
renew_align_avg = renew_align[, list(
  avg_align = round(mean(is_same, na.rm = TRUE)*100, digits = 1) ),
  by = list(pers_id)
]


#------------------------------------------------------------------------------#
attendance_align = voted_present |>
  dplyr::full_join(
    y = renew_align_avg,
    by = "pers_id"
  ) |>
  dplyr::left_join(
    y = meps_current[, list(pers_id, country_id, polgroup_id, natparty_id)],
    by = "pers_id"
  ) |>
  join_meps_names() |>
  join_polit_labs() |>
  join_meps_countries() |>
  dplyr::select(mep_name, country_iso3c, national_party, sum_rcv, sum_days_rcv,
                tot_rcv_days, present, excused, tot_attendance_days, avg_align) |>
  dplyr::arrange(country_iso3c, national_party, desc(avg_align) )


#------------------------------------------------------------------------------#
# save to disk -----------------------------------------------------------------
data.table::fwrite(x = attendance_align, file = here::here(
  "analyses", "attendance_align", "attendance_align.csv") )


#------------------------------------------------------------------------------#
## Loop for Reports ------------------------------------------------------------
# Create directory to store pdf
dir.create(path = here::here("analyses", "attendance_align", "out_pdf"),
           showWarnings = FALSE )

# Grid for loop
country_renewparty_grid = unique(
  attendance_align[, c("country_iso3c", "national_party")]
)

# you need to multiply the 1 row the Irish independent party for the 2 Irish MEPs
if ( length(attendance_align$national_party == "Ind."
            & attendance_align$country_iso3c == "IRL") > 1 ) {

  irl_ind_meps = data.frame(
    country_iso3c = "IRL",
    national_party = "Ind.",
    mep_name = c("Ciaran MULLOOLY", "Michael MCNAMARA")
  )

  country_renewparty_grid = country_renewparty_grid |>
    dplyr::left_join(
      y = irl_ind_meps,
      by = c("country_iso3c", "national_party")
    ) |>
    dplyr::mutate(
      national_party = ifelse(
        test = (national_party == "Ind." & country_iso3c == "IRL"),
        yes = paste0("Independent - ", mep_name),
        no = national_party
      )
    ) |>
    dplyr::select(-mep_name)
}


# loop to knit pdf ------------------------------------------------------------#
for ( i_row in seq_len( nrow(country_renewparty_grid) ) ) { #[23:24]
  quarto::quarto_render(
    input = here::here("analyses", "attendance_align", "attendance_align.qmd"),
    output_format = "pdf",
    execute_params = list(
      # Country ---------------------------------------------------------#
      country_iso3c = country_renewparty_grid$country_iso3c[i_row],
      # Party -----------------------------------------------------------#
      national_party = country_renewparty_grid$national_party[i_row] )
  )

  # Move file from main branch to sub-folder --------------------------------#
  file.rename(
    from = here::here("analyses", "attendance_align", "attendance_align.pdf"),
    to = here::here("analyses", "attendance_align", "out_pdf",
                    paste0(country_renewparty_grid$country_iso3c[i_row],
                           "_",
                           country_renewparty_grid$national_party[i_row],
                           ".pdf")
    ) )
}
