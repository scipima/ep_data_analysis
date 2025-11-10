###--------------------------------------------------------------------------###
# Create a Grid for Report Loop ------------------------------------------------
###--------------------------------------------------------------------------###


#------------------------------------------------------------------------------#
## Load join functions ---------------------------------------------------------

#' This brings in a set of convenience functions to merge data.

# source(file = here::here("scripts_r", "join_functions.R"))
#------------------------------------------------------------------------------#

meps_rcv_mandate |>
  dplyr::select(polgroup_id, natparty_id, country_id) |>
  dplyr::distinct() |>
  dplyr::mutate(
    # Collapse all French parties into 1, "L'Europe Ensemble"
    natparty_id = ifelse(
      test = polgroup_id == 7035L & country_id == 11L,
      yes = 6935L, no = natparty_id)) |>
  dplyr::distinct() |>
  join_meps_countries() |>
  join_polit_labs() |>
  dplyr::left_join(y = national_parties[, list(
    identifier, national_party_long = pref_label_en)],
    by = c("natparty_id" = "identifier")) |>
  dplyr::mutate(
    country_name_en = countrycode::countrycode(sourcevar = country_iso3c,
                                            origin = "iso3c",
                                            destination = "country.name.en"),
    # Collapse all French parties into 1, "L'Europe Ensemble"
    national_party = ifelse(test = natparty_id == 6935L,
                            yes = "Ens", no = national_party),
    national_party_long = ifelse(test = natparty_id == 6935L,
                                 yes = "L'Europe Ensemble", no = national_party_long)
    ) |>
  dplyr::select(country_id, country_iso3c, country_name_en,
                polgroup_id, political_group,
                natparty_id, national_party, national_party_long) %>%
  data.table::fwrite(file = here::here("data_reference", "qmd_grid.csv"))



#------------------------------------------------------------------------------#
## 2024 EP Election Grid -------------------------------------------------------
# Freeze EP Political Group and National parties as in the last EP Plenary before June 2024 EP Election

# meps_rcv_mandate <- data.table::fread(file = here::here("data_out", "meps_rcv_mandate_all.csv"), verbose = TRUE, key = c("rcv_id", "pers_id") )
# national_parties_all <- fread(file = here::here("data_out", "bodies", "national_parties_all.csv") )
# political_groups_all <- fread(file = here::here("data_out", "bodies", "political_groups_all.csv") )

# meps_rcv_mandate_all |>
#   dplyr::filter(rcv_id == 168833) |> # EP9 last RCV
#   # dplyr::filter(rcv_id == 169314) |> # EP10 first RCV
#   dplyr::select(polgroup_id, natparty_id, country_id) |>
#   dplyr::distinct() |>
#   join_meps_countries() |>
#   dplyr::left_join(
#     y = national_parties_all[, list(
#     identifier, national_party = label, national_party_long = pref_label_en)],
#     by = c("natparty_id" = "identifier") ) |>
#   dplyr::left_join(
#     y = political_groups_all[, list(identifier, political_group = label)],
#     by = c("polgroup_id" = "identifier") ) |>
#   dplyr::mutate(
#     country_name_en = countrycode::countrycode(sourcevar = country_iso3c,
#                                                origin = "iso3c",
#                                                destination = "country.name.en") ) |>
#   dplyr::select(country_id, country_iso3c, country_name_en,
#                 polgroup_id, political_group,
#                 natparty_id, national_party, national_party_long) |>
#   data.table::fwrite(file = here::here("data_reference", "ep2024_09_election_grid.csv"))
