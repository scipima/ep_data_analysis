###--------------------------------------------------------------------------###
# EP Plenary Session Master File -----------------------------------------------
###--------------------------------------------------------------------------###


#------------------------------------------------------------------------------#
#' The purpose of this script is to coordinate all scripts that should be executed on each day of the Plenary, as soon as the votes are released by the EP administration.
#' The code only runs without errors on the day the Plenary, as today's date is hard-coded in the scripts.
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
library("quarto")
library("here")


#------------------------------------------------------------------------------#
## Process Daily Votes ---------------------------------------------------------
source( here::here("scripts_main", "ep_rcv_mandate_10.R") )


#------------------------------------------------------------------------------#
## Group Metrics ---------------------------------------------------------------
quarto::quarto_render(input = here::here(
  "analyses", "metrics", "group_metrics_10.qmd") )


#------------------------------------------------------------------------------#
## Divisions in other Groups ---------------------------------------------------
quarto::quarto_render(input = here::here(
    "analyses", "monitoring", "groups_divisions.qmd") )


#------------------------------------------------------------------------------#
## Internal Cohesion within Renew ----------------------------------------------
quarto::quarto_render(input = here::here(
  "analyses", "monitoring", "internal_cohesion.qmd") )


#------------------------------------------------------------------------------#
## MEPs to target ----------------------------------------------
quarto::quarto_render(input = here::here(
  "analyses", "monitoring", "target_meps.qmd") )


#------------------------------------------------------------------------------#
## National Delegations --------------------------------------------------------
# D66
quarto::quarto_render(input = here::here(
  "analyses", "nat_parties", "m_10", "nld_d66_test.qmd") )

# Other national delegation
source(file = here::here(
  "analyses", "vote_param_qmd", "partyreport_loop.R"), echo = TRUE )
