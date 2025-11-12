###--------------------------------------------------------------------------###
# Process EP Plenary Documents -------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' This script processes all Plenary Documents retrieved through from the EP API.
#' REF: https://data.europarl.europa.eu/en/home
#' !!WATCH OUT!!
#' This script assumes that several other tables have already been created locally.
#' For instance, in order to process the `creator` column, you need to have already executed `scripts_r/api_bodies.R`, as we need the list of Committees.
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
# Clean ENV -------------------------------------------------------------------#
# rm(list = ls())
script_starts <- Sys.time()
cat("\n=====\nStarting to clean Plenary Documents.\n=====\n")

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
packages <- c("data.table", "dplyr", "here", "janitor", "tidyr", "xml2")
if (any( !packages %in% pacman::p_loaded() ) ) {
  pacman::p_load(char = packages[ !packages %in% pacman::p_loaded() ] ) }
rm(packages)


#------------------------------------------------------------------------------#
## Functions -------------------------------------------------------------------
source(file = here::here("scripts_r", "unnest_functions.R"))


#------------------------------------------------------------------------------#
## Data ------------------------------------------------------------------------
# Test (If there are no new files, just read in what you have in the bank)
if ( !exists(x = "pl_docs_list") ) {
  pl_docs_list <- readr::read_rds(file = here::here(
    "data_out", "docs_pl", "pl_docs_list.rds") )
}
if ( !exists(x = "pl_docs") ) {
  pl_docs <- data.table::fread(
    file = here::here("data_out", "docs_pl", "pl_docs.csv"))
}


#------------------------------------------------------------------------------#
## Process Data ----------------------------------------------------------------

# append data ---------------------------------------------------------------###
cols_languages_short <- c(".el", ".bg", ".de", ".hu", ".cs", ".ro", ".et", ".nl",
                          ".fi", ".mt", ".sl", ".sv", ".pt", ".it", ".lt", ".es",
                          ".da", ".pl", ".sk", ".lv", ".hr", ".ga")
cols_languages <- paste0("title_dcterms", cols_languages_short)


### Title ----------------------------------------------------------------------
docs_title <-  lapply(
  X = pl_docs_list, FUN = function(i_doc) {
    if ( "title_dcterms" %in% names(i_doc) ) {
      i_doc[, c("identifier", "title_dcterms") ] |>
        tidyr::unnest(title_dcterms, names_sep = "_") }
  } ) |>
  data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
  dplyr::select(identifier, title_dcterms_en, title_dcterms_fr)
sort(pl_docs$identifier[
  !pl_docs$identifier %in% docs_title$identifier
])

# write to disk ---------------------------------------------------------------#
pl_docs <- pl_docs |>
  dplyr::left_join(
    y = docs_title,
    by = "identifier") |>
  dplyr::distinct()
# sapply(pl_docs, function(x) sum(is.na(x))) # check

# write to disk
data.table::fwrite(x = pl_docs, file = here::here(
  "data_out", "docs_pl", "pl_docs.csv"))


### Adopts --------------------------------------------------------------------
#' Connection between Plenary Docs and Committee Doc being adopted.

docs_adopts <- unnest_nested_list(data_list = pl_docs_list,
                                  group_cols = "id",
                                  unnest_col = "adopts")
docs_adopts[, adopts := gsub(pattern = "eli/dl/doc/", replacement = "",
                             x = adopts)]
# write to disk ---------------------------------------------------------------#
data.table::fwrite(x = docs_adopts, file = here::here(
  "data_out", "docs_pl", "pl_docs_adopts.csv"))


### Based_on ------------------------------------------------------------------

#' Not clear what this field is referring to.

docs_based_on <- unnest_nested_list(data_list = pl_docs_list,
                                    group_cols = "id",
                                    unnest_col = "based_on")
docs_based_on[, based_on := gsub(pattern = "eli/dl/doc/", replacement = "",
                                 x = based_on)]

# write to disk ---------------------------------------------------------------#
data.table::fwrite(x = docs_based_on, file = here::here(
  "data_out", "docs_pl", "pl_docs_based_on.csv"))


### Creator -------------------------------------------------------------------

#' This gives us information regarding who authored reports, resolutions.
#' These can be: individual MEPs, EP Groups, Committees.
#' The issue is that the `creator` col is messy, with all entities thrown in in a mixed bag.
#' To tease it out, we pull the data on Committees, and match the values in the `creator` col against the Committee labs.
#' All values that are left out are assigned to political groups.

docs_creator <- unnest_nested_list(data_list = pl_docs_list,
                                   group_cols = "id",
                                   unnest_col = "creator")

# Read in Committee data ------------------------------------------------------#
if ( file.exists(here::here("data_out", "bodies", "committee_long.csv")) ) {
  committee_long <- data.table::fread(
    here::here("data_out", "bodies", "committee_long.csv") )
} else {
  stop("\nYou need the list of Committees to process the 'creator' column. Please run the script at 'scripts_r/api_bodies.R'.\n")
}

committee_labs <- unique(committee_long[label != "", list(label)])[["label"]]
committee_labs <- sort(c(
  committee_labs,
  "BUDE", # "European Parliament delegation to the Conciliation Committee"
  "COP" # Conference of Presidents"
))

# define the entities ---------------------------------------------------------#
# sort(unique(docs_creator$creator)) # check
docs_creator[grepl(pattern = "person", creator), entity:= "mep"]
# Grep all COMMITTEES
docs_creator[grepl(pattern = paste0(committee_labs, collapse = "|"), x = creator),
             entity := "committee_lab"]
# At this point, all remaining NAs are Political Groups
docs_creator[is.na(entity), entity:= "political_group"]
docs_creator[, creator := gsub(
  pattern = "person/|org/|EP_GROUP_|EP_|http://publications.europa.eu/resource/authority/corporate-body/",
  replacement = "", x = creator
)]

# WATCH OUT: here you have "EFD"|"EFDD" = "Europe of Freedom and Direct Democracy"

# Checks
# sort(unique(docs_creator$creator[docs_creator$entity == "committee_lab"]))
# sort(unique(docs_creator$creator[docs_creator$entity == "political_group"]))

# write to disk ---------------------------------------------------------------#
data.table::fwrite(x = docs_creator, file = here::here(
  "data_out", "docs_pl", "pl_docs_creator.csv"))
# docs_creator[, .N, by = identifier] # check
# googledrive::drive_upload(
#   media = here::here("data_out", "docs_pl", "pl_docs_creator.csv"),
#   path = "R_stuff/R_projects_data/ep_rollcall/data_out/pl_docs_creator.csv",
#   overwrite = TRUE)


### inverse_based_on_a_realization_of ------------------------------------------

#' This seems to relate the PL Docs to either previous voting items, or others things that are harder to decipher

docs_inverse_based_on_a_realization_of <- unnest_nested_list(
  data_list = pl_docs_list,
  group_cols = "id",
  unnest_col = "inverse_based_on_a_realization_of")
# write to disk ---------------------------------------------------------------#
data.table::fwrite(x = docs_inverse_based_on_a_realization_of, file = here::here(
  "data_out", "docs_pl", "pl_docs_inverse_based_on_a_realization_of.csv"))


### inverse_corrects ----------------------------------------------------------
#' We don't have a use case for this now, comment it out

# docs_inverse_corrects <- lapply(X = pl_docs_list, FUN = function(doc) {
#     if ("inverse_corrects" %in% names(doc)) {
#         doc |>
#             dplyr::select(doc_id = identifier, inverse_corrects) |>
#             tidyr::unnest(cols = inverse_corrects, names_sep = "_") |>
#             dplyr::select(-inverse_corrects_is_realized_by) } } ) |>
#     data.table::rbindlist(use.names = TRUE, fill = TRUE)


### inverse_created_a_realization_of ------------------------------------------

#' Connects the procedural number of Plenary Docs with procedure identifiers.
#' Unfortunately there are some Plenary Docs that are connected with multiple procedures.

docs_inverse_created_a_realization_of <- unnest_nested_list(
  data_list = pl_docs_list,
  group_cols = "id",
  unnest_col = "inverse_created_a_realization_of")

# check
# p=docs_inverse_created_a_realization_of[, .N, by = id][order(N)]

# write to disk ---------------------------------------------------------------#
data.table::fwrite(x = docs_inverse_created_a_realization_of, file = here::here(
  "data_out", "docs_pl", "docs_inverse_created_a_realization_of.csv") )


#### inverse_foresees_change_of ------------------------------------------------

#' This gives a list of amendments to EP files.
#' No use case for now, we comment it out.

# docs_inverse_foresees_change_of <- lapply(
#     X = pl_docs_list, FUN = function(doc) {
#         if ("inverse_foresees_change_of" %in% names(doc)) {
#             doc[, c("identifier", "inverse_foresees_change_of")] |>
#                 tidyr::unnest(inverse_foresees_change_of, names_sep = "_")
#         } } ) |>
#     data.table::rbindlist(use.names = TRUE, fill = TRUE) |>
#     dplyr::rename(
#         doc_id = identifier,
#         identifier = inverse_foresees_change_of_identifier,
#         creator = inverse_foresees_change_of_creator
#     ) |>
#     dplyr::select(-ends_with(cols_languages_short),
#                   -any_of( c("is_realized_by") ) ) |>
#     as.data.frame()

# dim(docs_inverse_foresees_change_of) # check
# docs_inverse_foresees_change_of_creator <- docs_inverse_foresees_change_of |>
#     dplyr::select(doc_id, identifier, creator) |>
#     tidyr::unnest(cols = creator) |>
#     data.table::as.data.table()
# dim(docs_inverse_foresees_change_of_creator) # check

# clean data
# docs_inverse_foresees_change_of_creator[
#     grepl(pattern = "person", creator), entity:= "mep"]
# docs_inverse_foresees_change_of_creator[
#     grepl(pattern = paste0(committee_labs, collapse = "|"), x = creator),
#     entity := "committee_lab"]
# docs_inverse_foresees_change_of_creator[
#     is.na(entity), entity:= "political_group"]

# clean col
# docs_inverse_foresees_change_of_creator[, creator := gsub(
#     pattern = "http://publications.europa.eu/resource/authority/corporate-body/EP_|http://publications.europa.eu/resource/authority/corporate-body/EP_GROUP_|person/|org/",
#     replacement = "", x = creator)]

# Checks
# sort(unique(docs_inverse_foresees_change_of_creator$creator))
# sort(unique(docs_inverse_foresees_change_of_creator$creator[
#   docs_inverse_foresees_change_of_creator$entity == "committee_lab"]))
# sort(unique(docs_inverse_foresees_change_of_creator$creator[
#   docs_inverse_foresees_change_of_creator$entity == "political_group"]))

# write to disk -------------------------------------------------------------###
# data.table::fwrite(x = docs_inverse_foresees_change_of[
#   , !names(docs_inverse_foresees_change_of) %in% "creator"],
#   file = here::here("data_out", "docs_pl", "pl_docs_inverse_foresees_change_of.csv"))
# data.table::fwrite(x = docs_inverse_foresees_change_of_creator, file = here::here(
#   "data_out", "docs_pl", "pl_docs_inverse_foresees_change_of_creator.csv"))


### is_about ------------------------------------------------------------------

#' This gathers all the EUROVOC identifiers related to Plenary Docs

docs_is_about <- unnest_nested_list(data_list = pl_docs_list,
                                    group_cols = "id",
                                    unnest_col = "is_about")
docs_is_about[, is_about := gsub(
  pattern = "http://eurovoc.europa.eu/", replacement = "", x = is_about)]

# save eurovoc ids in vector
eurovoc_ids <- unique(docs_is_about$is_about)


### Is_derivative_of ----------------------------------------------------------

#' This helps us towards understanding what Resolutions are merged into a single Joint Resolution
#' !!WATCH OUT!!
#' THere is currently a bug here, but cannot understand why.
#' eli/dl/doc/RC-10-2025-0366 is a simple list in the original json.
#' However, it is read in as a dataframe, and this creates all sorts of aberrant data.
#' Comment out until we find a solution.

# docs_is_derivative_of <- unnest_nested_list(unnest_col = "is_derivative_of")
# docs_is_derivative_of[, is_derivative_of := gsub(pattern = "eli/dl/doc/",
#                                                  replacement = "", x = is_derivative_of) ]

# # write to disk -------------------------------------------------------------###
# data.table::fwrite(x = docs_is_derivative_of, file = here::here(
#   "data_out", "docs_pl", "pl_docs_is_derivative_of.csv"))


### isAboutDirectoryCode ------------------------------------------------------
docs_isAboutDirectoryCode <- unnest_nested_list(data_list = pl_docs_list,
                                                group_cols = "id",
                                                unnest_col = "isAboutDirectoryCode")


### isAboutSubjectMatter -------------------------------------------------------
#' This could be a substitute of EUROVOC, but it is not clear where to retrieve a dictionary of these labels.

docs_isAboutSubjectMatter <- unnest_nested_list(data_list = pl_docs_list,
                                                group_cols = "id",
                                                unnest_col = "isAboutSubjectMatter")


### Participation -------------------------------------------------------------
#' This is a fairly complex unnesting and merging operation.
#' We have participation by people, political groups, and committees.
#' We also have participation in the name of organisation.
#' Unfortunately, it not clear why sometimes organisation are not listed under the organisation, but only in terms of in the name of organisation.
#' That means that we have to do several unnesting and merges in order to get the full list of organisations.

# Apply the general functions
docs_workHadParticipation <- unnest_nested_df(
  data_list = pl_docs_list,
  group_col = "id",
  unnest_col = "workHadParticipation"
)

# docs_workHadParticipation <- lapply(
#   X = pl_docs_list, FUN = function(doc) {
#     if ("workHadParticipation" %in% names(doc)) {
#       doc |>
#         dplyr::select(identifier, workHadParticipation) |>
#         tidyr::unnest_wider(col = workHadParticipation ) |>
#         dplyr::mutate(
#           across(.cols = any_of(c(
#             "id", "type", "had_participant_organization",
#             "participation_role", "had_participant_person",
#             "participation_in_name_of") ),
#             .fns = as.list) ) } } ) |>
#   data.table::rbindlist(use.names = TRUE, fill = TRUE)

# Isolate person
docs_workHadParticipation_person <- unique(
  docs_workHadParticipation[, list(
    had_participant_person = as.character( unlist(had_participant_person) ) ),
    keyby = list(id)
  ][, had_participant_person := as.integer( gsub(
    pattern = "person/", replacement = "",
    x = had_participant_person) )
  ][ !is.na(had_participant_person) ]  # Drop NAs
)
# Check
# sort(unique(docs_workHadParticipation_person$had_participant_person))
data.table::setnames(
  docs_workHadParticipation_person,
  old = "had_participant_person", new = "pers_id"
)


# Participation the name of
docs_workHadParticipation_innameof = unique(
  docs_workHadParticipation[, list(
    participation_in_name_of = as.character( unlist(participation_in_name_of) ) ),
    keyby = list(id)
  ][, participation_in_name_of := gsub(
    pattern = "org/|http://publications.europa.eu/resource/authority/corporate-body/|http://publications.europa.eu/resource/authority/corporate-body/EP_",
    replacement = "", x = participation_in_name_of)
  ][
    !is.na(participation_in_name_of) # Drop NAs
  ]
)

# Participation the name of: POLITICAL GROUPS
docs_workHadParticipation_innameof_polgroup = docs_workHadParticipation_innameof[
  !participation_in_name_of %in% committee_labs] # Exclude Committees
# Check
# sort(unique(docs_workHadParticipation_innameof_polgroup$participation_in_name_of))

# Participation the name of: COMMITTEES
docs_workHadParticipation_innameof_cmt <- docs_workHadParticipation_innameof[
  participation_in_name_of %in% committee_labs]
# Check
# sort(unique(docs_workHadParticipation_innameof_cmt$participation_in_name_of))

docs_workHadParticipation_id_cmt = unique(
  docs_workHadParticipation[, list(
    workHadParticipation_id = as.character( unlist(workHadParticipation_id) ) ),
    keyby = list(id)
  ][, workHadParticipation_id := gsub(
    pattern = "^.*_",
    replacement = "", x = workHadParticipation_id)
  ][workHadParticipation_id %in% committee_labs] # Grab just Committee
)
# Check
# sort(unique(docs_workHadParticipation_id_cmt$workHadParticipation_id))


# Isolate organisation
docs_workHadParticipation_organization = unique(
  docs_workHadParticipation[, list(
    had_participant_organization = as.character( unlist(had_participant_organization) ) ),
    keyby = list(id)
  ][, had_participant_organization := gsub(
    pattern = "org/",
    replacement = "", x = had_participant_organization)
  ][had_participant_organization %in% committee_labs]
)
# Check
# sort(unique(docs_workHadParticipation_organization$had_participant_organization))


# Merge -----------------------------------------------------------------------#
# For some reasons, these 2 datasets only have info regarding A- files
docs_workHadParticipation_organization <- data.table::merge.data.table(
  x = docs_workHadParticipation_organization, y = docs_workHadParticipation_id_cmt,
  by.x = c("id", "had_participant_organization"),
  by.y = c("id", "workHadParticipation_id"),
  all = TRUE
) |> unique()

# `participation_in_name_of` also has B- files
docs_workHadParticipation_organization  <- data.table::merge.data.table(
  x = docs_workHadParticipation_organization, y = docs_workHadParticipation_innameof_cmt,
  by.x = c("id", "had_participant_organization"),
  by.y = c("id", "participation_in_name_of"),
  all = TRUE
) |> unique()
# Rename col
data.table::setnames(
  docs_workHadParticipation_organization,
  old = "had_participant_organization", new = "cmt_lab"
)
# sort(unique(docs_workHadParticipation_organization$had_participant_organization))

# write to disk -------------------------------------------------------------###
data.table::fwrite(x = docs_workHadParticipation_person, file = here::here(
  "data_out", "docs_pl", "pl_docs_workHadParticipation_person.csv"))
data.table::fwrite(x = docs_workHadParticipation_organization, file = here::here(
  "data_out", "docs_pl", "pl_docs_workHadParticipation_organization.csv"))


###--------------------------------------------------------------------------###
### Committee ------------------------------------------------------------------

# What files are in `docs_creator` but not in `docs_workHadParticipation_organization`?
sort(
  unique(docs_workHadParticipation_organization$id)[
    !unique(docs_workHadParticipation_organization$id) %in%
      unique(docs_creator$id[docs_creator$entity == "committee_lab"])]
)

# And viceversa, what files are in `docs_workHadParticipation_organization` but not in `docs_creator`?
sort(
  unique(docs_creator$id[docs_creator$entity == "committee_lab"])[
    !unique(docs_creator$id[docs_creator$entity == "committee_lab"]) %in%
      unique(docs_workHadParticipation_organization$id)]
)

# Merge with docs_creator
docs_committee <- data.table::merge.data.table(
  x = docs_workHadParticipation_organization,
  y = docs_creator[entity == "committee_lab", list(id, creator)],
  by.x = c("id", "cmt_lab"),
  by.y = c("id", "creator"),
  all = TRUE
)

# Merge with pl_docs to get more metadata
docs_committee = pl_docs[, c("id", "identifier", "doc_id")
][
  docs_committee,
  on = "id"
]
# Check what you're missing out
sort(unique(
  pl_docs$id[
    !pl_docs$id %in% docs_committee$id
  ]))


# checks
# sapply(docs_committee, function(x) sum(is.na(x)))
# read in data form csv
# docs_committee_csv <- data.table::fread(file = here::here(
# "data_out", "docs_pl", "pl_docs_committee_fromcsv.csv"))
# check joint committees
# docs_committee[, .N, by = list(doc_id)][order(N)]
# pl_docs[, .N, by = list(identifier)][order(N)]
# docs_committee_csv[, .N, by = list(doc_id)][order(N)]


# check if we can get some Committee identifiers regarding Joint Resolutions --#
# p = docs_is_derivative_of |>
#   dplyr::filter(grepl(pattern = "RC-", x = identifier)) |>
#   dplyr::left_join(
#     y = docs_committee,
#     by = c("is_derivative_of" = "doc_id") )

# write to disk -------------------------------------------------------------###
data.table::fwrite(x = docs_committee,
                   file = here::here("data_out", "docs_pl", "pl_docs_committee.csv"))


###--------------------------------------------------------------------------###
## EUROVOC ---------------------------------------------------------------------

#' You need to manually download the documentation from `eurovoc_xml.zip`.
#' REF: https://op.europa.eu/en/web/eu-vocabularies/dataset/-/resource?uri=http://publications.europa.eu/resource/dataset/eurovoc#

# Read in the combinations of THESAURUS_ID (the parent term) and the DESCRIPTEUR_ID
desc_thes <- xml2::read_xml(here::here("data_in", "eurovoc", "desc_thes.xml"))
eurovoc_desc_thes <- data.frame(
  THESAURUS_ID = desc_thes |>
    xml2::xml_find_all("./RECORD/THESAURUS_ID") |>
    xml2::xml_text(),
  DESCRIPTEUR_ID = desc_thes |>
    xml2::xml_find_all("./RECORD/DESCRIPTEUR_ID") |>
    xml2::xml_text(),
  COUNTRY = desc_thes |>
    xml2::xml_find_all("./RECORD/DESCRIPTEUR_ID") |>
    xml2::xml_attr(attr = "COUNTRY"),
  TOPTERM = desc_thes |>
    xml2::xml_find_all("./RECORD/TOPTERM") |>
    xml2::xml_text() )

thes_en <- xml2::read_xml(here::here("data_in", "eurovoc", "thes_en.xml"))
eurovoc_thes <- data.frame(
  THESAURUS_ID = thes_en |>
    xml2::xml_find_all("./RECORD/THESAURUS_ID") |>
    xml2::xml_text(),
  THESAURUS_LIBELLE = thes_en |>
    xml2::xml_find_all("./RECORD/LIBELLE") |>
    xml2::xml_text() )

desc_en <- xml2::read_xml(here::here("data_in", "eurovoc", "desc_en.xml"))
eurovoc_desc <- data.frame(
  DESCRIPTEUR_ID = desc_en |>
    xml2::xml_find_all("./RECORD/DESCRIPTEUR_ID") |>
    xml2::xml_text(),
  DESCRIPTEUR_LIBELLE = desc_en |>
    xml2::xml_find_all("./RECORD/LIBELLE") |>
    xml2::xml_text() )

# merge -----------------------------------------------------------------------#
eurovoc <- merge(eurovoc_desc_thes, eurovoc_thes,
                 by = "THESAURUS_ID", all = TRUE)
eurovoc <- merge(eurovoc, eurovoc_desc,
                 by = "DESCRIPTEUR_ID", all = TRUE) |>
  dplyr::group_by(DESCRIPTEUR_ID) |>
  dplyr::slice_min(order_by = THESAURUS_ID, n = 1L) |>
  dplyr::ungroup() |>
  janitor::clean_names() |>
  dplyr::select(descripteur_id, descripteur_libelle, thesaurus_id,
                thesaurus_libelle, country, topterm)


# Merge dictionaries with EUROVOC classification in EP dossier
docs_is_about <- merge(docs_is_about, eurovoc,
                       by.x = "is_about", by.y = "descripteur_id",
                       all.x = TRUE, all.y = FALSE) |>
  dplyr::mutate(
    identifier = gsub(pattern = "eli/dl/doc/", replacement = "", x = id,
                      fixed = TRUE),
    # invert the orders of the groups
    doc_label = gsub(pattern = "(^[A-Z]{1,2}-\\d-)(\\d{4})-(\\d{4})",
                     replacement = "\\1\\3-\\2", x = identifier, perl = T),
    # delete -
    doc_label = gsub(pattern = "(?<=[A-Z])-",
                     replacement = "", x = doc_label, perl = T),
    # treat RC separately
    doc_label = gsub(pattern = "^RC",
                     replacement = "RC-B", x = doc_label, perl = T),
    # slash at the end
    doc_label = gsub(pattern = "(?<=-\\d{4})-",
                     replacement = "/", x = doc_label, perl = T) )
data.table::fwrite(x = docs_is_about, here::here(
  "data_out", "docs_pl", "pl_docs_eurovoc.csv"))


#------------------------------------------------------------------------------#
# Clean up before exiting -----------------------------------------------------#
rm(list = ls(pattern = "doc|desc_|eurovoc|pl"))
rm(thes_en)

# Run time
script_ends <- Sys.time()
script_lapsed = script_ends - script_starts
cat("\n=====\nFinished cleaning Plenary Documents.\n=====\n")
print(script_lapsed) # Execution time

# Remove objects
rm(list = ls(pattern = "script_"))
