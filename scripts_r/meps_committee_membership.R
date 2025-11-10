### --------------------------------------------------------------------------###
# MEPS-Committee Membership ----------------------------------------------------
### --------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c("data.table", "dplyr", "future.apply", "here", "httr",  
                        "httr2", "lubridate", "janitor", "jsonlite",
                        "readr", "stringi", "tidyr", "tidyselect") )

# Hard code the start of the mandate ------------------------------------------#
mandate_starts <- as.Date("2024-07-14")

#------------------------------------------------------------------------------#
## GET/meps/show-current -------------------------------------------------------
# Returns the list of all active MEPs for today's date
api_raw <- httr::GET(
  url = "https://data.europarl.europa.eu/api/v2/meps/show-current?format=application%2Fld%2Bjson&offset=0"
)
api_list <- jsonlite::fromJSON(
  rawToChar(api_raw$content),
  flatten = TRUE
)
# Get vector of current MEPs IDs
meps_ids_current <- api_list$data |>
  janitor::clean_names() |>
  dplyr::pull(identifier) |>
  as.integer()

# Remove API objects --------------------------------------------------------###
rm(api_list, api_params, api_raw)

# store tmp list if code breaks down the line
if (!exists("meps_ids_list")) {
  meps_ids_list <- readr::read_rds(
    file = here::here("data_out", "meps", "meps_ids_list.rds")
  )
}


###--------------------------------------------------------------------------###
### Get mandate, political group, national party, Committee --------------------
hasMembership <- lapply(
  X = meps_ids_list[
    names(meps_ids_list) %in% meps_ids_current # filter to just current MEPs
  ],
  FUN = function(i_data) {
    i_data |>
      dplyr::select(
        pers_id = identifier,
        hasMembership
      ) |>
      tidyr::unnest(hasMembership, keep_empty = TRUE) |>
      tidyr::unnest_wider(memberDuring, names_sep = "_")
  }
) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::select(-dplyr::any_of("contactPoint")) |>
  dplyr::mutate(pers_id = as.integer(pers_id)) |>
  janitor::clean_names()


###--------------------------------------------------------------------------###
### Get Committee --------------------------------------------------------------
meps_committee <- hasMembership[
  grepl(
    pattern = "COMMITTEE_PARLIAMENTARY_STANDING|COMMITTEE_PARLIAMENTARY_SUB",
    x = membership_classification, ignore.case = TRUE
  )
  # get both members and substitutes
  & role %in% c("def/ep-roles/MEMBER", "def/ep-roles/CHAIR_VICE", "def/ep-roles/CHAIR"),
  list(pers_id, role,
       committee_id = organization,
       start_date = member_during_start_date,
       end_date = member_during_end_date
  )
] |>
  dplyr::mutate(
    committee_id = as.integer(
      gsub(pattern = "org/", replacement = "", x = committee_id)
    ),
    start_date = lubridate::as_date(start_date),
    end_date = ifelse(is.na(end_date), as.character(Sys.Date()), end_date),
    end_date = lubridate::as_date(end_date)
  ) |>
  # membership has to start after the start of the 10 mandate
  dplyr::filter(
    end_date >= mandate_starts
    & start_date >= mandate_starts
    & end_date == Sys.Date() # they must still be members
  ) |>
  # for each MEP and Committee, get just the last membership
  dplyr::group_by(pers_id, committee_id) |>
  dplyr::slice_max(order_by = start_date, n = 1) |> 
  dplyr::ungroup() |>
  data.table::as.data.table()


#------------------------------------------------------------------------------#
# Grab vector of bodies in the current data
bodies_id <- na.omit( unique( meps_committee$committee_id) ) 

# grid to loop over
api_params <- paste0("https://data.europarl.europa.eu/api/v2/corporate-bodies/", 
                     sort(unique(bodies_id)),
                     "?format=application%2Fld%2Bjson&json-layout=framed")

# Function ------------------------------------------------------------------###
get_body_id <- function(links = api_params) {
  future.apply::future_lapply(
    X = links, FUN = function(api_url) {
      print(api_url)
      api_raw <- httr::GET(api_url)
      api_list <- jsonlite::fromJSON(
        rawToChar(api_raw$content),
        flatten = TRUE)
      # extract info
      return(api_list[["data"]]) } ) }

# perform parallelised check
future::plan(strategy = multisession) # Run in parallel on local computer
list_tmp <- get_body_id()
future::plan(strategy = sequential) # revert to normal

# append list and transform into DF
committee_id <- data.table::rbindlist(l = list_tmp,
                                      use.names = TRUE, fill = TRUE) |>
  dplyr::select(committee_id = identifier, committee_lab = label) |> 
  dplyr::mutate(committee_id = as.integer(committee_id)) |> 
  janitor::clean_names()

# merge with committee labels -------------------------------------------------#
meps_committee <- meps_committee |> 
  dplyr::full_join(
    y = committee_id, 
    by = "committee_id")


###--------------------------------------------------------------------------###
## Upload list of current MEPs -------------------------------------------------
meps_current <- data.table::fread(here::here(
  "data_out", "meps", "meps_current.csv") )


###--------------------------------------------------------------------------###
# Load join functions ----------------------------------------------------------

#' This brings in a set of convenience functions to merge data.

source(file = here::here("source_scripts_r", "join_functions.R"))
###--------------------------------------------------------------------------###

# merge Committee with Current MEPs
meps_committee_full <- meps_committee |> 
  dplyr::full_join(
    y = meps_current, by = "pers_id") |> 
  join_polit_labs() |> 
  join_meps_names() |> 
  join_meps_countries() |> 
  dplyr::mutate(role = gsub(pattern = "def/ep-roles/", replacement = "", x = role) )

# clean data to be saved as .xlsx ---------------------------------------------#
meps_committee_full_toxlsx <- meps_committee_full |> 
  dplyr::select(-ends_with("_id"), - ends_with("_date"))
# check
# meps_committee_full_toxlsx[, .N, by = list(mep_name, committee_lab)][order(N)]


# polgroup county by committee ------------------------------------------------#
polgroup_count_bycommittee <- meps_committee_full |> 
  dplyr::filter(!is.na(committee_lab)) |> 
  dplyr::select(committee_lab, political_group) |> 
  dplyr::group_by(committee_lab, political_group) |> 
  dplyr::summarise(polgroup_count = n()) |> 
  dplyr::ungroup() |> 
  dplyr::group_by(committee_lab) |> 
  dplyr::mutate(comm_count = sum(polgroup_count)) |> 
  dplyr::ungroup() |> 
  dplyr::arrange(committee_lab) 

# committee long shaped -------------------------------------------------------#
committee_count_long <- polgroup_count_bycommittee |> 
  dplyr::left_join(
    y = polgroup_count_bycommittee |> 
      dplyr::filter(political_group == "NI") |> 
      dplyr::select(committee_lab, NI = polgroup_count),
    by = "committee_lab") |> 
  dplyr::mutate(comm_effective_count = ifelse(
    test = is.na(NI), yes = comm_count, no = comm_count - NI) ) |> 
  dplyr::mutate(threshold = comm_effective_count*2/3) |> 
  dplyr::select(-NI) 

# committee wide shaped -------------------------------------------------------#
committee_count <- committee_count_long |> 
  tidyr::pivot_wider(
    names_from = political_group, values_from = polgroup_count, 
    values_fill = 0) 

# save csv to disk ------------------------------------------------------------#
committee_count |>
  data.table::fwrite(here::here("data_out", "bodies", "committee_count.csv"))
committee_count_long |>
  data.table::fwrite(here::here("data_out", "bodies", "committee_count_long.csv"))


#------------------------------------------------------------------------------#
## Excel and upload -------------------------------------------------------
writexl::write_xlsx(
  x = list(
    committee_count = committee_count,
    meps_committee_full_toxlsx = meps_committee_full_toxlsx
  ),
  path = here::here("data_out", "bodies", "committee_count.xlsx") )



# committee_count |> 
#   filter(committee_lab %in% c("ITRE", "IMCO")) |> 
#   summarise(across(.cols = comm_count : NI, .fns = sum) )  |> 
#   knitr::kable()

# read in current allocation
commissioner_committees_raw <- data.table::fread(
  here::here("data_in", "commissioner_committees_raw.csv") ) |> 
  janitor::clean_names()

# renew commissioners: past threshold?
commissioner_committees_raw |> 
  dplyr::filter(political_group == "Renew") |> 
  dplyr::select(commissioner, committee, political_group) |> 
  dplyr::distinct() |> 
  dplyr::left_join(
    y = committee_count, 
    by = c("committee" = "committee_lab")
  ) |> 
  group_by(commissioner) |> 
  summarise(across(.cols = comm_count : NI, .fns = sum) ) |>
  dplyr::mutate(
    coalition = PPE + Renew + `S&D` + `Verts/ALE`,
    is_past_threshold = ifelse(test = coalition >= ceiling(threshold),
                               yes = 1L, no = 0L) ) |> 
  knitr::kable()


commissioner_committees_raw |> 
  dplyr::select(commissioner, committee, political_group) |> 
  dplyr::distinct() |> 
  dplyr::left_join(
    y = committee_count, 
    by = c("committee" = "committee_lab")
  ) |> 
  dplyr::mutate(
    political_group = ifelse(test = political_group == "",
                             yes = "Others", no = political_group)
  ) |> 
  dplyr::group_by(political_group) |> 
  dplyr::summarise(summ = sum(comm_count)) |> 
  dplyr::mutate(tot = sum(summ),
                share = summ / tot)

# renew's share in the EP
# 77 / 718


#------------------------------------------------------------------------------#
## Stephane's Committee --------------------------------------------------------
members_only <- meps_committee_full |> 
  dplyr::select(-ends_with("_id"), - ends_with("_date")) |> 
  dplyr::filter(committee_lab %in% c("ITRE", "IMCO", "ENVI", "ECON",
                                     "INTA", "EMPL", "BUDG", "JURI") ) |> 
  dplyr::group_by(mep_name, role, political_group, national_party, country) |> 
  dplyr::summarise(committee = paste(committee_lab, collapse = "; ")) |> 
  dplyr::arrange(committee, mep_name)
data.table::fwrite(x = members_only, file = here::here(
  "data_out", "bodies", "members_only.csv") )

full_committee <- meps_committee_full |> 
  dplyr::select(mep_name, committee_lab, role, political_group, national_party, country) |> 
  dplyr::filter(committee_lab %in% c("ITRE", "IMCO", "ENVI", "ECON",
                                     "INTA", "EMPL", "BUDG", "JURI") ) |> 
  dplyr::arrange(committee_lab, mep_name)
data.table::fwrite(x = full_committee, file = here::here(
  "data_out", "bodies", "full_committee.csv") )


#------------------------------------------------------------------------------#
## Check Thierry's table -------------------------------------------------------
library(readxl)
Majorities_for_the_hearings_ms <- read_excel("C:/Users/marco/Downloads/Majorities for the hearings_ms.xlsx", sheet = "Committee count") 

check_diff <- Majorities_for_the_hearings_ms |> 
  dplyr::select(Cttee, PPE, `S&D`, PfE, ECR, Renew, ESN, `Verts/ALE`, `The Left`, NI) |> 
  pivot_longer(cols = -Cttee) |> 
  dplyr::mutate(Cttee = gsub(pattern = "\\*", replacement = "", x = Cttee)) |> 
  dplyr::full_join(committee_count_long |> 
              dplyr::select(committee_lab, political_group, polgroup_count), 
            by = c("Cttee" = "committee_lab",
                   "name" = "political_group")) |> 
  dplyr::mutate(
    polgroup_count = ifelse(is.na(polgroup_count), 0, polgroup_count),
    diff = value - polgroup_count) |> 
  filter(!is.na(diff))
data.table::fwrite(check_diff, here::here("data_out", "cmt_count_diff.csv"))





