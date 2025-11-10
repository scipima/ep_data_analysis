###--------------------------------------------------------------------------###
# EP Daily Vote Master File ----------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' The purpose of this script is to coordinate all scripts that should be executed on each day of the Plenary, as soon as the votes are released by the EP administration.
#' The code only runs without errors on the day the Plenary, as today's date is hard-coded in the scripts.
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
library(quarto)
library(here)
library(data.table)


#------------------------------------------------------------------------------#
# REPO SETUP: check if dir exists to dump incoming & processed files ----------#
source(file = here::here("scripts_r", "repo_setup.R") )

#------------------------------------------------------------------------------#
## Hard code Plenary days -----------------------------------------------------#
today_date <- gsub(pattern = "-", replacement = "",
                   x = as.character( Sys.Date() ) )
# today_date <- "20250618" # test
activity_id_today <- paste0("MTG-PL-", Sys.Date())
# activity_id_today <- paste0("MTG-PL-", "2025-06-18") # test


#------------------------------------------------------------------------------#
## GET/meetings/foreseen-activities --------------------------------------------
### Download all these files is very long, make sure we do it rarely ----------#
# check whether data already exists
meetings_foreseen <- get_api_data(
  path = here::here("data_out", "meetings", "meetings_foreseen_rds",
                      paste0("foreseen_activities_", today_date, ".rds")),
  script = here::here("scripts_r", "api_meetings_foreseen_activities.R"),
  max_days = 1,
  file_type = "rds",
  varname = "meetings_foreseen",
  envir = .GlobalEnv
)


#------------------------------------------------------------------------------#
## Process Daily Votes ---------------------------------------------------------
source( here::here("scripts_main", "ep_rcv_today.R") )


#------------------------------------------------------------------------------#
## Aggregates ------------------------------------------------------------------
source(file = here::here("scripts_r", "aggregate_rcv_today.R") )

# p =meps_rcv_today |> 
#   join_meps_names() |> join_polit_labs() |> join_meps_countries()


#------------------------------------------------------------------------------#
## EPP Coalitions --------------------------------------------------------------
source(file = here::here("scripts_test", "epp_coalitions.R") )


#------------------------------------------------------------------------------#
## Vote Analysis for Management ------------------------------------------------
quarto::quarto_render(input = here::here(
    "analyses", "metrics", "vote_metrics_day.qmd") )


#------------------------------------------------------------------------------#
## Vote Analysis for Management ------------------------------------------------
source(file = here::here("analyses", "pa_analysis", "pa_analysis_loop.R") )
