###--------------------------------------------------------------------------###
# MEPs Euclidean Distance ------------------------------------------------------
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c("data.table", "dplyr", "ggplot2", "ggrepel", "here") )


# Hard code the start of the mandate ------------------------------------------#
mandate_starts <- as.Date("2024-07-14")


###--------------------------------------------------------------------------###
## Functions -------------------------------------------------------------------
# https://stackoverflow.com/questions/2547402/how-to-find-the-statistical-mode
stat_mode <- function(x) {
  if ( length(x) <= 2 ) return(x[1])
  if ( anyNA(x) ) x = x[!is.na(x)]
  ux <- unique(x)
  ux[ which.max( tabulate( match(x, ux) ) ) ] }

# Calculate Majorities --------------------------------------------------------#
source(file = here::here("scripts_r", "get_majority.R") )

# Load join functions ---------------------------------------------------------#
source(file = here::here("scripts_r", "join_functions.R") )


###--------------------------------------------------------------------------###
## Read data -------------------------------------------------------------------

if (file.exists( here::here("data_out", "meps_rcv_mandate_10.csv") ) ) {
  # RCVs ----------------------------------------------------------------------#
  meps_rcv_mandate <- data.table::fread(
    file = here::here("data_out", "meps_rcv_mandate_10.csv"),
    verbose = TRUE, key = c("rcv_id", "pers_id") )
} else {
  # Download via Google Drive -------------------------------------------------#
  ## have you gone through authentication? ------------------------------------#
  data_id <- googledrive::drive_get("data_out/meps_rcv_mandate_10.csv")
  ## download -----------------------------------------------------------------#
  googledrive::drive_download(file = data_id, path = here::here(
    "data_out", "meps_rcv_mandate_10.csv"), overwrite = T)
  meps_rcv_mandate <- data.table::fread(
    file = here::here("data_out", "meps_rcv_mandate_10.csv"),
    verbose = TRUE, key = c("rcv_id", "pers_id") )
}

### MEP last Plenary day -------------------------------------------------------
meps_current <- data.table::fread( file = here::here(
  "data_out", "meps", "meps_current.csv") )

# Hard code Renew IDs
renew_polgroup_ids <- c(5704L, 7035L)


# full list of Renew MEPs
renew_meps_full <- unique(meps_rcv_mandate$pers_id[
  meps_rcv_mandate$polgroup_id %in% renew_polgroup_ids])


###--------------------------------------------------------------------------###
### MEPs mandate ---------------------------------------------------------------
meps_mandate <- data.table::fread(
  here::here("data_out", "meps", "meps_mandate_10.csv"))

# Get the Groups' majority by rcv_id ------------------------------------------#
mjrt_groups <- get_polgroup_majority(
  data_in = meps_rcv_mandate[result >= -1]) # exclude absent

# Get Party majority ----------------------------------------------------------#
mjrt_parties <- get_natparty_majority(
  data_in = meps_rcv_mandate[result >= -1]) # exclude absent

## Calculate Euclidean Distance ------------------------------------------------
### MEP - Political Group ------------------------------------------------------
# Merge Group line with MEPs vote
mjrt_groups_meps <- mjrt_groups[
  meps_rcv_mandate, on = c("rcv_id", "polgroup_id")
][i.result >= -1] # exclude absent

# Are Groups' majorities and MEP the same? ------------------------------------#
mjrt_groups_meps[, diff := (result - i.result)^2]

mjrt_groups_meps_avg <- mjrt_groups_meps[, list(
  group_dist = sqrt(mean(diff, na.rm = TRUE) ) ),
  by = list(pers_id)]


### MEP - National Party -------------------------------------------------------
# Merge Group line with MEPs vote
mjrt_parties_meps <- mjrt_parties[
  meps_rcv_mandate[
    result >= -1
  ], 
  on = c("rcv_id", "natparty_id")
] # exclude absent

# Are Groups' majorities and MEP the same? ------------------------------------#
mjrt_parties_meps[, diff := sqrt( (result - i.result)^2)]

mjrt_parties_meps_avg <- mjrt_parties_meps[, list(
  party_dist = mean(diff, na.rm = TRUE) ),
  by = list(pers_id)]


### Renew's Majorities - MEP ----------------------------------------------------------------
# Merge Group line with MEPs vote
mjrt_renew_meps <- mjrt_groups[
  polgroup_id %in% renew_polgroup_ids, # subset to just Renew
  list(rcv_id, result, who_won) # select cols
][
  meps_rcv_mandate[
    !polgroup_id %in% renew_polgroup_ids # exclude Renew
    & result >= -1 # exclude absent & no votes
  ],
  on = c("rcv_id")
]

# Are Groups' majorities and MEP the same? ------------------------------------#
mjrt_renew_meps[, diff := sqrt( (result - i.result)^2)]

mjrt_renew_meps_avg <- mjrt_renew_meps[, list(
  renew_dist = mean(diff, na.rm = TRUE) ),
  by = list(pers_id)]


meps_eucldist <- meps_current |>
  dplyr::left_join(mjrt_groups_meps_avg, by = "pers_id") |>
  # dplyr::left_join(mjrt_parties_meps_avg, by = "pers_id") |>
  dplyr::left_join(mjrt_renew_meps_avg, by = "pers_id") |>
  dplyr::left_join(
    y = meps_current |> 
      dplyr::summarise(
        n_meps = n(), 
        .by = c(polgroup_id, natparty_id) ),
    by = c("polgroup_id", "natparty_id") 
  ) |> 
  join_meps_names() |>
  join_polit_labs() |>
  join_meps_countries() |>
  dplyr::mutate(
    is_poachable = ifelse(
      test = (renew_dist < group_dist & !pers_id %in% c(257256L) ),
      yes = 1L, no = 0L),
    country_party = paste0(country_iso3c, " - ", national_party),
    dist_diff = group_dist - renew_dist,
  ) |>
  dplyr::filter(!is.na(is_poachable))

data.table::fwrite(x = meps_eucldist, file = here::here(
  "data_out", "meps", "meps_eucldist.csv"))

meps_eucldist |>
  ggplot(aes(x = group_dist, y = renew_dist)) +
  geom_point(aes( colour = factor(is_poachable) ),
             size = 6, show.legend = FALSE) +
  ggrepel::geom_text_repel(
    data = meps_eucldist[meps_eucldist$is_poachable == 1L, ],
    aes(label = mep_name)
  ) +
  labs(
    title = "Who should we talk to?",
    subtitle = "MEPs in blue are the ones displaying greater affinity with Renew than their own current Group",
    x = "MEPs' distance from own Group (lower values stand for greater proximity)",
    y = "MEPs' distance from Renew (lower values stand for greater proximity)") +
  xlim(0, 2) +
  ylim(0, 2) +
  scale_color_manual(values = c("0" = "grey", "1" = "blue")) +
  theme_minimal() +
  theme(
    plot.title.position = "plot",
    plot.title = element_text(face = "bold", size = 24),
    plot.subtitle = element_text(size = 20),
    axis.title = element_text(size = 20)
  )
ggsave(filename = here::here("figures", "renew_topoach.pdf"),
       device = "pdf", dpi = 300, height = 7, width = 7)
ggsave(filename = here::here("figures", "renew_topoach.jpeg"),
       device = "jpeg", dpi = 300, height = 14, width = 18)

# rm(list = ls(pattern = "who_won"))
