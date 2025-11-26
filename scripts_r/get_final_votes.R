#------------------------------------------------------------------------------#
# Extract Final Votes ----------------------------------------------------------
#------------------------------------------------------------------------------#

#' While the EP Rules of Procedure mention *final votes*, it is much harder to measures them as there is no explicit flag in the data for those cases
#' We revert then to string matching in the vote description to try to catch as many occurrences as we can.
#' However, this solution is suboptimal, as our indicator is subject to typos during data entry, or incompleteness of the strings we use to capture final votes.


#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
packages <- c("data.table", "dplyr")
if (any( !packages %in% pacman::p_loaded() ) ) {
  pacman::p_load(char = packages[ !packages %in% pacman::p_loaded() ] ) }


#------------------------------------------------------------------------------#
## Read data in ----------------------------------------------------------------
# read in file if not present already -----------------------------------------#
if ( !exists("pl_session_docs_rcv") ) {
  # Read in data conditional on mandate ---------------------------------------#
  if (mandate_starts == "2019-07-01") {
    pl_session_docs_rcv <- data.table::fread(file = here::here(
      "data_out", "rcv", "pl_session_docs_rcv_all.csv"),
      na.strings = c(NA_character_, "") )
  } else {
    pl_session_docs_rcv <- data.table::fread(file = here::here(
      "data_out", "rcv", "pl_session_docs_rcv_10.csv"),
      na.strings = c(NA_character_, "") ) } }
# sapply(X = votes_dt, function(x) sum(is.na(x))) # check NAs


#------------------------------------------------------------------------------#
## Final votes -----------------------------------------------------------------
# Identify whether vote is final based on guess of what VoteWatch did ---------#
# c("single vote", "vote:.*", "as a whole", "Procedural vote", "Election", "joint text")
pl_session_docs_rcv[
  grepl(pattern = "vote unique|vote final|vote\\:.*|ensembl*e du texte|Election|Proposition de la Commission|Proc.dure d.approbation|proc.dure d.urgence|Demande de d.cision d.urgence|Approbation|Demande de vote|Projet de d.cision du Conseil|Projet du Conseil|D.cision d.engager des n.gociations interinstitutionnelles|Proposition de r.solution|Accord\\s?provisoire|Proposition.? de d.cision|Projet de recommandation|Proposition de recommandation|Demande de procéder au vote sur les amendements|Demande de mettre aux voix les amendements|Projet de d.cision|Projet de directive du Conseil|Projet de r.glement du Conseil|.lection de la Commission|Projet commun|Recommandation de d.cision|D.cision du maintien du recours",
        x = title_dcterms_fr, ignore.case = T, perl = T),
  is_final := 1L]
pl_session_docs_rcv[is.na(is_final), is_final := 0L]
# table(pl_session_docs_rcv$is_final, exclude = NULL)

# EXCEPT: grab resolutions and decision unless they feature flags for amendments
pl_session_docs_rcv[
  # grab these ...
  grepl(pattern = "r.solution|d.cision|approbation|Accord\\s?provisoire|recommandation",
        x = title_dcterms_fr, ignore.case = T, perl = T)
  # ... unless they are together with these
  & grepl(pattern = "Am \\d+|§ \\d+|Consid.rant",
          x = title_dcterms_fr, ignore.case = T, perl = T),
  is_final := 0L]


# Deal with special cases
pl_session_docs_rcv[
  grepl(pattern = "147046", x = title_dcterms_fr) # "Assimilation de la violation des mesures restrictives de l’Union aux crimes visés à l’article 83, paragraphe 1, du TFUE - C9-0219/2022"
  | grepl(pattern = "153148", x = title_dcterms_fr) # Une stratégie industrielle de l'UE pour stimuler la compétitivité industrielle, les échanges commerciaux et la création d'emplois de qualité - B9-0104/2023
  , is_final := 1L
]

# Checks
# table(pl_session_docs_rcv$is_final, exclude = NULL) # check
# unique(pl_session_docs_rcv$title_dcterms_fr[
#   pl_session_docs_rcv$is_final == 0L])

pl_session_docs_rcv[, rcv_id := as.integer(gsub(pattern = "^.*-DEC-", replacement = "",
                                                x = event_dec_itm_id))]


# Bring in VOTES data ---------------------------------------------------------#
# Read in data conditional on mandate ---------------------------------------#
if ( !exists("today_date") && mandate_starts == "2019-07-01" ) {
  pl_votes <- data.table::fread(here::here(
    "data_out", "votes", "pl_votes_all.csv"),
    select = c("activity_label_fr", "referenceText_fr", "rcv_id") )
} else if ( !exists("today_date") && mandate_starts == "2024-07-14" ){
  pl_votes <- data.table::fread(here::here(
    "data_out", "votes", "pl_votes_10.csv"),
    select = c("activity_label_fr", "referenceText_fr", "rcv_id") )
}

pl_votes[
  grepl(pattern = "vote unique|vote final|vote\\:.*|ensembl*e du texte|Election|Proposition de la Commission|Proc.dure d.approbation|proc.dure d.urgence|Demande de d.cision d.urgence|Approbation|Demande de vote|Projet de d.cision du Conseil|Projet du Conseil|D.cision d.engager des n.gociations interinstitutionnelles|Proposition de r.solution|Accord\\s?provisoire|Proposition.? de d.cision|Projet de recommandation|Proposition de recommandation|Demande de procéder au vote sur les amendements|Demande de mettre aux voix les amendements|Projet de d.cision|Projet de directive du Conseil|Projet de r.glement du Conseil|.lection de la Commission|Projet commun|Recommandation de d.cision|D.cision du maintien du recours",
        x = activity_label_fr, ignore.case = TRUE, perl = TRUE),
  `:=`( is_final = 1L ) ]
pl_votes[, .N, by = is_final]

pl_votes[
  grepl(pattern = "vote unique|vote final|vote\\:.*|ensembl*e du texte|Election|Proposition de la Commission|Proc.dure d.approbation|proc.dure d.urgence|Demande de d.cision d.urgence|Approbation|Demande de vote|Projet de d.cision du Conseil|Projet du Conseil|D.cision d.engager des n.gociations interinstitutionnelles|Proposition de r.solution|Accord\\s?provisoire|Proposition.? de d.cision|Projet de recommandation|Proposition de recommandation|Demande de procéder au vote sur les amendements|Demande de mettre aux voix les amendements|Projet de d.cision|Projet de directive du Conseil|Projet de r.glement du Conseil|.lection de la Commission|Projet commun|Recommandation de d.cision|D.cision du maintien du recours|Nomination de ",
        x = referenceText_fr, ignore.case = TRUE, perl = TRUE),
  `:=`( is_final = 1L ) ]
pl_votes[, .N, by = is_final]


# Deal with special cases
pl_votes[
  # If this is present ...
  grepl(pattern = "r.solution|d.cision|approbation|Accord\\s?provisoire|recommandation",
        x = activity_label_fr, ignore.case = TRUE, perl = TRUE)
  # ... AND this is  present ...
  & grepl(pattern = "Am \\d+|§ \\d+|Consid.rant",
          x = activity_label_fr, ignore.case = FALSE, perl = TRUE),
  # ... THEN it's not final
  `:=`( is_final =  0L ) ]
pl_votes[, .N, by = is_final]

# If NA, then 0
pl_votes[is.na(is_final), is_final := 0L]

pl_session_docs_rcv = pl_session_docs_rcv[
  pl_votes,
  on = "rcv_id"
]
pl_session_docs_rcv[, is_final := data.table::fifelse(
  test = (is_final %in% 1L | i.is_final %in% 1L),
  yes = 1L, no = 0L)
]

#------------------------------------------------------------------------------#
# Save to disk conditional on mandate -----------------------------------------#
if (!exists("today_date") && mandate_starts == "2019-07-01" ) {
  votes_final = pl_session_docs_rcv |>
    dplyr::select(rcv_id, is_final)
  data.table::fwrite(x = votes_final, file = here::here(
    "data_out", "votes", "votes_final_all.csv") )
} else if ( !exists("today_date") && mandate_starts == "2024-07-14" ) {
  votes_final = pl_session_docs_rcv |>
    dplyr::select(rcv_id, is_final)
  data.table::fwrite(x = votes_final, file = here::here(
    "data_out", "votes", "votes_final_10.csv") ) }

# Clean up before exiting
rm(list = ls(pattern = "^pl_|^vote|^rcv"))
