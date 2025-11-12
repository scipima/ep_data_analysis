###--------------------------------------------------------------------------###
# EP Meetings: Foreseen Activities ---------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' The script grabs all Meetings Foreseen Activities from the EP API.
#' REF: https://data.europarl.europa.eu/en/home; https://data.europarl.europa.eu/en/developer-corner/opendata-api
#------------------------------------------------------------------------------#

# rm(list = ls())
script_starts <- Sys.time()
cat("\n=====\nStarting to collect Plenary Foreseen Activities.\n=====\n")

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c("curl", "data.table", "dplyr", "here", "httr2",
                        "lubridate", "janitor", "jsonlite", "readr", "stringi",
                        "tidyr", "tidyselect") )


#------------------------------------------------------------------------------#
# REPO SETUP: check if dir exists to dump incoming & processed files ----------#
source(file = here::here("scripts_r", "repo_setup.R") )


#------------------------------------------------------------------------------#
## Parameters ------------------------------------------------------------------
# Plenary dates
if ( !exists("pl_date") ) {
  pl_date <- gsub(pattern = "-", replacement = "", x = Sys.Date() )
  # pl_date <- "20250403" # test
}
print(pl_date)

if ( !exists("pl_date_ymd") ) {
  pl_date_ymd <- as.character( Sys.Date() )
  # pl_date_ymd <- "2025-04-03" # test
}
print(pl_date_ymd)


# Hard code the start of the mandate ------------------------------------------#
if ( !exists("mandate_starts") ) {
  mandate_starts <- as.Date("2024-07-14") }


#------------------------------------------------------------------------------#
# REPO SETUP: check if dir exists to dump incoming & processed files ----------#
source(file = here::here("scripts_r", "repo_setup.R") )


#------------------------------------------------------------------------------#
## GET/meps/show-current -------------------------------------------------------
# Returns the list of all active MEPs for today's date

# Build REQUEST
req = httr2::request(
  paste0("https://data.europarl.europa.eu/api/v2/meps/show-current?mandate-date=",
         pl_date_ymd,
         "&format=application%2Fld%2Bjson&offset=0") ) |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0")

# Get RESPONSE
resp = req |>
  httr2::req_error(is_error = ~FALSE) |> # ignore error, we deal with it below
  httr2::req_throttle(capacity = 490, fill_time_s = 300) |> # call politely
  httr2::req_retry(max_tries = 5, # retry a bunch of times in case of failures
                   backoff = ~ 2 ^ .x + runif(n = 1, min = -0.5, max = 0.5) ) |>
  httr2::req_perform()

# If not an error, download and make available in ENV
if ( httr2::resp_status(resp) == 200L ) {
  resp_body = resp |>
    httr2::resp_body_json(simplifyDataFrame = TRUE)
} else {
  stop("\nWARNING: API request failed for current MEPs.\n")
}


# Extract data ----------------------------------------------------------------#
meps_current <- data.table::as.data.table(x = resp_body$data)
meps_current[, `:=`(
  pers_id = as.integer(identifier),
  mep_name = label
)]
# Remove objects
rm(req, resp, resp_body)


## GET/meetings/ ---------------------------------------------------------------
### Download all these files if very long, make sure we do it rarely ----------#
pl_meetings_foreseen <- get_api_data(
  path = here::here("data_out", "meetings", "pl_meetings_foreseen.csv"),
  script = here::here("scripts_r", "api_meetings.R"),
  max_days = 1,
  file_type = "csv",
  varname = "pl_meetings_foreseen",
  envir = .GlobalEnv
)

# Process data
pl_meetings_foreseen[, `:=`(n_week = data.table::isoweek(activity_date) ) ]
pl_meetings_foreseen <- pl_meetings_foreseen[ n_week == min(n_week) ]


###--------------------------------------------------------------------------###
## GET/meetings/{event-id}/foreseen-activities ---------------------------------
#' Returns the list of all Foreseen Activities linked to EP Meetings.

# Get vector of IDs to plug in in the API request
meetings_ids <- paste0(pl_meetings_foreseen$activity_id, collapse = ",")
# meetings_ids = "MTG-PL-2025-04-03" # test

# Build REQUEST
req <- httr2::request(
  paste0("https://data.europarl.europa.eu/api/v2/meetings/",
         meetings_ids,
         "/foreseen-activities?format=application%2Fld%2Bjson&offset=0") ) |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0")

# Add time-out and ignore error before performing request
resp = req |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
  httr2::req_error(is_error = ~FALSE) |> # ignore error, we deal with it below
  httr2::req_throttle(capacity = 490, fill_time_s = 300) |> # call politely
  httr2::req_retry(max_tries = 5, # retry a bunch of times in case of failures
                   backoff = ~ 2 ^ .x + runif(n = 1, min = -0.5, max = 0.5) ) |>
  httr2::req_perform()

# If not an error, download and make available in ENV
if ( httr2::resp_status(resp) == 200L ) {
  resp_body = resp |>
    httr2::resp_body_json(simplifyDataFrame = TRUE)
} else {
  stop("\nWARNING: API request failed for Foreseen Plenary Activities.\n")
}

# Extract data ----------------------------------------------------------------#
foreseen_activities <- data.table::as.data.table(x = resp_body$data)
# Clean up API stuff
rm(meetings_ids, req, resp, resp_body)

# Flag for URGENT DEBATES or RESOLUTIONS --------------------------------------#
if ("was_purpose_of" %in% names(foreseen_activities) ){
  was_purpose_of = foreseen_activities |>
    dplyr::select(id, was_purpose_of) |>
    tidyr::unnest(was_purpose_of, names_sep = "_") |>
    tidyr::unnest(was_purpose_of_activity_label) |>
    dplyr::select( any_of( c("id", "fr", "en") ) )
  
  if ("fr" %in% names(was_purpose_of) ) {
    is_urgent = was_purpose_of |>
      dplyr::mutate(is_urgent = ifelse(
        test = grepl(pattern = "article 150", x = fr),
        yes = 1L, no = 0L
      )) |>
      dplyr::select(id, is_urgent) |>
      dplyr::filter(is_urgent == 1L) |>
      dplyr::distinct()|>
      data.table::as.data.table()
  } else if ("en" %in% names(was_purpose_of) ) {
    is_urgent = was_purpose_of |>
      dplyr::mutate(is_urgent = ifelse(
        test = grepl(pattern = "Rule 150", x = en),
        yes = 1L, no = 0L
      )) |>
      dplyr::select(id, is_urgent) |>
      dplyr::filter(is_urgent == 1L) |>
      dplyr::distinct() |>
      data.table::as.data.table()
  } else {
    warning("There are no Urgent debates or resolutions.")
  }
}

if (exists(x = "is_urgent")) {
  # Merge
  foreseen_activities = is_urgent[
    foreseen_activities,
    on = "id"
  ]
  foreseen_activities[is.na(is_urgent), is_urgent := 0L]
} else {
  foreseen_activities[, is_urgent := 0L]
}


# Select cols to retain
cols_tokeep <- c("id", "activity_id", "is_urgent",
                 "activity_date", "activity_start_date", "activity_end_date",
                 "activity_label.en", "activity_label.fr",
                 "agendaLabel.en", "agendaLabel.fr",
                 "speakingTimeContent.en", "speakingTimeContent.fr",
                 "consists_of", "had_activity_type", "notation_periclesId",
                 "hasLocality", "hasRoom.id", "hasRoom.type", "hasRoom.identifier",
                 "hasRoom.officeAddress", "hasRoom.hasSite",
                 "inverse_was_scheduled_in", "activity_order", "notation_agendaPoint",
                 "documented_by_a_realization_of", "based_on_a_realization_of",
                 "occured_at_stage", "was_purpose_of",
                 "structuredContent.en", "structuredContent.fr",
                 "headingLabel.en", "headingLabel.fr")
# Not all these cols are always available - subset based on availability
cols_tokeep <- cols_tokeep[cols_tokeep %in% names(foreseen_activities)]

foreseen_activities <- foreseen_activities[, ..cols_tokeep ] |>
  janitor::clean_names()


# `has_locality` is not always present, conditionally clean -------------------#
if ("has_locality" %in% names(foreseen_activities)) {
  foreseen_activities = foreseen_activities |>
    dplyr::mutate(has_locality = gsub(
      pattern = "http://publications.europa.eu/resource/authority/place/",
      replacement = "", x = has_locality) ) |>
    tidyr::fill(has_locality, .direction = "down") |>
    data.table::as.data.table()
} else {
  foreseen_activities[, has_locality := NA]
}


# Filter just to votes --------------------------------------------------------#
votes_foreseen = foreseen_activities[
  grepl(pattern = "ITM-V", x = activity_id)]


# Legislative Procedure -------------------------------------------------------#
if ("structured_content_en" %in% names(votes_foreseen)) {
  votes_foreseen[
    grepl(pattern = '"symbol\">***<', x = structured_content_en, fixed = TRUE),
    legis_procedure := "Consent procedure"]
  votes_foreseen[
    grepl(pattern = '"symbol\">*<', x = structured_content_en, fixed = TRUE),
    legis_procedure := "Consultation procedure"]
  votes_foreseen[
    grepl(pattern = '"symbol\">***I<', x = structured_content_en, fixed = TRUE),
    legis_procedure := "Ordinary legislative procedure: first reading"]
  votes_foreseen[
    grepl(pattern = '"symbol\">***II<', x = structured_content_en, fixed = TRUE),
    legis_procedure := "Ordinary legislative procedure: second reading"]
  votes_foreseen[
    grepl(pattern = '"symbol\">***III<', x = structured_content_en, fixed = TRUE),
    legis_procedure := "Ordinary legislative procedure: third reading"]
}


#------------------------------------------------------------------------------#
## PROCEDURES: Get metadata ----------------------------------------------------
#' We take the unique because it may happens that a procedure is repeated twice in a voting week.
#' For instance, `2025/0526(COD)` was voted as a urgent procedure on 2025-10-21, and again on 2025-10-23.
process_ids = unique(
  sort(gsub(pattern = "eli/dl/proc/", replacement = "", 
            x = unlist(votes_foreseen$inverse_was_scheduled_in) ) )
)

# Split the vector in chunks of size 50 each
chunk_size <- 20L
process_ids_chunks <- split(
  x = process_ids, ceiling(seq_along(process_ids) / chunk_size)
)
# Empty storage
process_ids_list <- vector(mode = "list", length = length(process_ids_chunks))

# loop to get all decisions
for (i_param in seq_along(process_ids_list) ) {
  print(i_param)
  
  # Build REQUEST
  req = httr2::request(
    paste0("https://data.europarl.europa.eu/api/v2/procedures/",
           paste0(process_ids_chunks[[i_param]], collapse = ","),
           "?format=application%2Fld%2Bjson") ) |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0")
  
  # Add time-out and ignore error before performing request
  resp = req |>
    httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
    httr2::req_error(is_error = ~FALSE) |> # ignore error, we deal with it below
    httr2::req_throttle(capacity = 490, fill_time_s = 300) |> # call politely
    httr2::req_retry(max_tries = 5, # retry a bunch of times in case of failures
                     backoff = ~ 2 ^ .x + runif(n = 1, min = -0.5, max = 0.5) ) |>
    httr2::req_perform()
  
  # If not an error, download and make available in ENV
  if ( httr2::resp_status(resp) == 200L ) {
    resp_body = resp |>
      httr2::resp_body_json(simplifyDataFrame = TRUE) 
    process_ids_list[[i_param]] = resp_body$data
  } else {
    cat("\nWARNING: API call to PROCEDURE ID endpoint failed for chunk", i_param,
        "IDs:", paste0(process_ids_list[[i_param]], collapse = ","), "\n")
  }
}

# Extract data ----------------------------------------------------------------#
votes_procedures <- lapply(X = process_ids_list,
                           FUN = as.data.table) |> 
  data.table::rbindlist(use.names = TRUE, fill = TRUE)

# Remove objects
rm(process_ids, req, resp, resp_body)


#------------------------------------------------------------------------------#
## Clean Data ------------------------------------------------------------------
### Extract Committees ---------------------------------------------------------
# Check that all cells are indeed DFs
row_idx = sapply(X = votes_procedures$had_participation, FUN = is.data.frame)
# Unnest
procedures_cmt_tmp = data.table::rbindlist(
  l = setNames(object = votes_procedures$had_participation[row_idx],
               nm = votes_procedures$id[row_idx]),
  use.names = TRUE, fill = TRUE, idcol = "process_id") |>
  dplyr::filter(
    participation_role %in% c("def/ep-roles/COMMITTEE_LEAD")
  ) |>
  dplyr::select(process_id,
                committee_lab = had_participant_organization) |>
  tidyr::unnest(committee_lab) |>
  dplyr::mutate(
    committee_lab = gsub(pattern = "org/", replacement = "",
                         x = committee_lab, fixed = TRUE)
  ) |>
  tidyr::nest(
    committee = committee_lab
  ) |>
  dplyr::arrange(process_id) |> 
  dplyr::distinct()


# Check that we get the same results from string ------------------------------#
# Extract Committee
cmts_fromstring <- votes_foreseen |>
  dplyr::mutate(
    committee_lab = stringr::str_extract_all(
      string = votes_foreseen$structured_content_en,
      pattern = "resource\\=\\\"org\\/[A-Z]{4,5}")
  ) |>
  dplyr::select(inverse_was_scheduled_in, committee_lab) |>
  tidyr::unnest_longer(col = c(inverse_was_scheduled_in, committee_lab),
                       keep_empty = FALSE) |>
  dplyr::mutate(
    committee_lab = stringr::str_remove_all(string = committee_lab,
                                            pattern = 'resource="org/')
  ) |> 
  dplyr::distinct()

procedures_cmt <- procedures_cmt_tmp |>
  tidyr::unnest(col = committee) |>
  distinct() |> 
  dplyr::full_join(
    y = cmts_fromstring,
    by = c("process_id" = "inverse_was_scheduled_in",
           "committee_lab" = "committee_lab")
  ) |>
  tidyr::nest(
    committee = committee_lab
  ) |>
  dplyr::arrange(process_id)
# Remove objects
rm(cmts_fromstring)


#------------------------------------------------------------------------------#
### Extract Renew Shadow -------------------------------------------------------
procedures_renew_shadow = data.table::rbindlist(
  l = setNames(object = votes_procedures$had_participation[row_idx],
               nm = votes_procedures$id[row_idx]),
  use.names = TRUE, fill = TRUE, idcol = "process_id") 
# Extract EP Mandate
procedures_renew_shadow[, `:=`(
  parliamentary_term = as.integer(
    gsub(pattern = "org/ep-", replacement = "", x = parliamentary_term)
  ) ) ]
# Only get RENEW Shadows
procedures_renew_shadow = procedures_renew_shadow[
  participation_role %in% c("def/ep-roles/RAPPORTEUR_SHADOW")
  & politicalGroup %in% c("org/RENEW")
][order(process_id, parliamentary_term, activity_date)]

# TEST: How many Shadow Rapporteurs across MANDATEs per process_id?
# procedures_renew_shadow[, list( length( unique(parliamentary_term) ) ),
#                         by = list(process_id)]
# For procedures spanning multiple EP TERMS, you may have several Shadows.
# We only retain those from the last mandate.
shadows_latest_term <- procedures_renew_shadow[, .SD[
  parliamentary_term == max(parliamentary_term)
],
by = process_id]

# Unnest the list of persons to get individual MEP IDs
shadows_unnested <- shadows_latest_term[, list(
  pers_id = unlist(had_participant_person)),
  by = list(process_id)] |> 
  unique()

# Clean the person ID and convert to integer
shadows_unnested[, pers_id := as.integer(
  gsub(pattern = "person/", replacement = "", x = pers_id, fixed = TRUE)
)]

# Join to get MEP names
shadows_with_names <- meps_current[
  shadows_unnested,
  on = "pers_id"
]

# Nest the results back into a list-column, grouped by procedure
procedures_renew_shadow <- shadows_with_names[, list(
  renew_shadow = list(.SD[, .(pers_id, mep_name)])),
  by = list(process_id)
][order(process_id)]

# Check that we only captured SHADOW
if ( !all(
  grepl(pattern = "def/ep-roles/RAPPORTEUR_SHADOW",
        x = procedures_renew_shadow$participation_role) ) ) {
  warning("You may have Renew MEPs not as Shadow - Possible?")
}
rm(list = ls(pattern = "^shadows_"))


#------------------------------------------------------------------------------#
### Extract Rapporteurs --------------------------------------------------------
procedures_rapporteur_tmp = data.table::rbindlist(
  l = setNames(object = votes_procedures$had_participation[row_idx],
               nm = votes_procedures$id[row_idx]),
  use.names = TRUE, fill = TRUE, idcol = "process_id")
# Extract EP Mandate
procedures_rapporteur_tmp[, parliamentary_term := as.integer(
  gsub(pattern = "org/ep-", replacement = "", x = parliamentary_term) )
]
# Only get Rapporteurs
procedures_rapporteur_tmp = procedures_rapporteur_tmp[
  participation_role %in% c("def/ep-roles/RAPPORTEUR", "def/ep-roles/CHAIR",
                            "def/ep-roles/RAPPORTEUR_CO")
][order(process_id, parliamentary_term, activity_date)]

# If a file has both a RAPPORTEUR and RAPPORTEUR_CO, only retain the latter
procedures_rapporteur_tmp <- procedures_rapporteur_tmp[, if (
  "def/ep-roles/RAPPORTEUR_CO" %in% participation_role &&
  "def/ep-roles/RAPPORTEUR" %in% participation_role) {
  .SD[participation_role != "def/ep-roles/RAPPORTEUR"]
} else { .SD },
by = process_id]

# Sometimes PROCESS_ID have multiple entries because they are recurring procedures.
# E.g. Many budgetary issues are repeated in several EP TERMS.
procedures_rapporteur_tmp = procedures_rapporteur_tmp[, .SD[
  parliamentary_term == max(parliamentary_term)
],
by = process_id
]

# Flatten `had_participant_person` & convert PERS_ID
procedures_rapporteur_tmp <- procedures_rapporteur_tmp[, list(
  pers_id = unlist(had_participant_person)),
  by = process_id
][, `:=`(
  pers_id = as.integer(gsub(pattern = "person/", replacement = "",
                            x = pers_id, fixed = TRUE) )
)] |> 
  unique()

# From blurb
rapporteur_fromstring <- votes_foreseen |>
  dplyr::select(inverse_was_scheduled_in, structured_content_en) |>
  dplyr::mutate(
    rapporteur_id = stringr::str_extract_all(
      string = structured_content_en,
      pattern = '(?<=person\\/)\\d{4,6}(?=\\")')
  ) |>
  dplyr::select(inverse_was_scheduled_in, rapporteur_id) |>
  tidyr::unnest_longer(col = c(inverse_was_scheduled_in, rapporteur_id),
                       keep_empty = FALSE) |>
  dplyr::mutate(rapporteur_id = as.integer(rapporteur_id)) |>
  # Sometimes MEPs table multiple Resolutions for the same Procedure ID, thus appearing multiple times
  dplyr::distinct() 

# Merge
procedures_rapporteur <- data.table::merge.data.table(
  x = procedures_rapporteur_tmp, y = rapporteur_fromstring,
  by.x = c("process_id", "pers_id"),
  by.y = c("inverse_was_scheduled_in", "rapporteur_id"),
  all = TRUE
)
# Merge with MEPs' names
procedures_rapporteur = meps_current[, list(mep_name, pers_id)
][
  procedures_rapporteur, on = "pers_id"]

# Nest
procedures_rapporteur = procedures_rapporteur |>
  tidyr::nest(
    rapporteur = c(pers_id, mep_name)
  ) |>
  dplyr::arrange(process_id)
# Remove objects
rm(rapporteur_fromstring)


#------------------------------------------------------------------------------#
### Extract Procedure_IDs ------------------------------------------------------
procedures_process_id <- votes_procedures[, list(id, procedure_id = label)]


#------------------------------------------------------------------------------#
### Extract Doc_IDs ------------------------------------------------------------
docid_tmp = data.table::rbindlist(
  l = setNames(object = votes_procedures$consists_of,
               nm = votes_procedures$id),
  use.names = TRUE, fill = TRUE, idcol = "process_id")
docid_tmp[, activity_date := as.Date(activity_date)]

# Unnest
docid_tmp = docid_tmp[
  had_activity_type == "def/ep-activities/TABLING_PLENARY",
  list(based_on_a_realization_of = as.character(unlist(based_on_a_realization_of))),
  by = list(process_id, activity_date)] |> 
  unique()

#' WATCH OUT! You cannot filter out Amendments at this stage.
#' Indeed, many Resolutions only show up as Amendments.

# Create a DOC identifier just for non-AM
docid_tmp[, `:=`(
  identifier = gsub(pattern = "eli/dl/doc/|-AM-.*", replacement = "",
                    x = based_on_a_realization_of, perl = TRUE)
)]
# Subset cols & get rid of amendments
docid_tmp = unique(docid_tmp[, list(process_id, activity_date, identifier)])

#' Now the Amendments will be discarded as a consequence of taking the unique combinations of these columns.


#------------------------------------------------------------------------------#
#' DEFENSIVE: Last minute data, or URGENT PROCEDURES, may not be fully populated.
#' Thus, we import part of the data from other sources.
#------------------------------------------------------------------------------#

# Unnest the 2 cols separately as they are riddled with NULL values -----------#
# Col 1
inverse_was_scheduled_in = votes_foreseen[, list(
  id, inverse_was_scheduled_in
)][, list(
  inverse_was_scheduled_in = as.character(unlist(inverse_was_scheduled_in))
),
by = id
] |> 
  unique()
# Col 2
based_on_a_realization_of = votes_foreseen[, list(
  id, based_on_a_realization_of
)][, list(
  based_on_a_realization_of = as.character(unlist(based_on_a_realization_of))
),
by = id
] |> 
  unique()
# Merge them back together ----------------------------------------------------#
based_on_a_realization_of = based_on_a_realization_of[
  inverse_was_scheduled_in,
  on = "id"
][, id := NULL][order(inverse_was_scheduled_in)]

# !!WATCH OUT!! ---------------------------------------------------------------#
# Some Procedures have both an A- and C- file.
# We want to display only A- files in those cases.
based_on_a_realization_of[, `:=`(
  is_c_doc = ifelse(
    test = grepl(pattern = "/C-", x = based_on_a_realization_of, fixed=TRUE),
    yes = 1L, no = 0L),
  is_multiple = .N
),
by = inverse_was_scheduled_in
]
# Exclude the cases identified above
based_on_a_realization_of = based_on_a_realization_of[
  ! (is_c_doc == 1L & is_multiple > 1L)
]
# Exclude O- files (not sure what they are ... Oral Amendments???)
based_on_a_realization_of = based_on_a_realization_of[
  !grepl(pattern = "O-\\d{1,2}-", x = based_on_a_realization_of, perl=TRUE)
]
# Subset cols
based_on_a_realization_of = based_on_a_realization_of[
  !is.na(based_on_a_realization_of), # drop NAs, no need to carry them on
  list(inverse_was_scheduled_in,
       identifier = gsub(pattern = "eli/dl/doc/", replacement = "",
                         x = based_on_a_realization_of, fixed = TRUE)
  )]


# Merge the 2 sources together ------------------------------------------------#
# full merge - if necessary
procedures_doc_id <- data.table::merge.data.table(
  x = based_on_a_realization_of, y = docid_tmp,
  by.x = c("inverse_was_scheduled_in", "identifier"),
  by.y = c("process_id", "identifier"),
  all = TRUE
)
# Rename
data.table::setnames(x = procedures_doc_id, old = "inverse_was_scheduled_in",
                     new = "process_id")

# For procedures spanning multiple EP TERMS, you may have several 'A-' Reports.
# Retain only the last ones.
procedures_doc_id[, `:=`(
  is_A_many = ifelse(
    # condition 1: A- files; condition 2: not the most recent
    test = (grepl(pattern = "A-", x = identifier, fixed=TRUE)
            & activity_date < max(activity_date, na.rm = TRUE)),
    yes="drop", no="keep"
  )),
  by = process_id
]
# Drop multiple 'A' Reports
procedures_doc_id = procedures_doc_id[
  !is_A_many %in% "drop",
  list(process_id, identifier)
]


# Fix docs' labels ------------------------------------------------------------#
# create temporary duplicate col for string processing
procedures_doc_id[, identifier2 := identifier]
# invert the orders of the groups for A-, B-, C- files
procedures_doc_id[
  grepl(pattern = "^[ABC]{1}.\\d{1,2}.", x = identifier2),
  doc_id := gsub(
    pattern = "(^[ABC]{1}.\\d{1,2}.)(\\d{4}).(\\d{4})",
    replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
# treat RC separately
procedures_doc_id[
  grepl(pattern = "^RC.\\d{1,2}", x = identifier2),
  doc_id := gsub(
    pattern = "(^RC.\\d{1,2}.)(\\d{4}).(\\d{4})",
    replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
procedures_doc_id[, doc_id := gsub(pattern = "^RC", replacement = "RC-B",
                                   x = doc_id, perl = T)]
# delete - between doc_id LETTER and MANDATE NUMBER
procedures_doc_id[, doc_id := gsub(pattern = "([A-Z]{1}).(\\d{1,2})",
                                   replacement = "\\1\\2", x = doc_id, perl = T)]
# slash at the end
procedures_doc_id[, doc_id := gsub(pattern = "(?<=.\\d{4}).",
                                   replacement = "/", x = doc_id, perl = T)]
# sapply(procedures_doc_id, function(x) sum(is.na(x))) # check

# Save temporary file
data.table::fwrite(x = procedures_doc_id[, list(process_id, identifier, doc_id)],
                   file = here::here(
                     "data_out", "files_tmp", "procedures_doc_id.csv"
                   ))
# Delete cols
procedures_doc_id[, c("identifier", "identifier2") := NULL]


# Clean and filter data -------------------------------------------------------#
# Drop NAs in DOC ID
procedures_doc_id <- procedures_doc_id[!is.na(doc_id)]
# For Resolutions, we may have all the B- files as well as the RC- files
procedures_doc_id[, nchar_doc := nchar(doc_id)]
# We sort them here, so Joint Resolutions always appear on top
procedures_doc_id[order(process_id, -nchar_doc)]
procedures_doc_id[, nchar_doc := NULL]
# Nest the results back into a list-column, grouped by procedure.
procedures_doc_id <- procedures_doc_id|>
  tidyr::nest(doc_id = doc_id)



#------------------------------------------------------------------------------#
rm(procedures_cmt_tmp, docid_tmp, procedures_rapporteur_tmp)


## Merge all tables ------------------------------------------------------------
final_dt <- votes_foreseen |>
  tidyr::unnest(cols = inverse_was_scheduled_in, keep_empty = TRUE) |>
  dplyr::distinct() |> 
  dplyr::left_join(
    y = procedures_process_id,
    by = c("inverse_was_scheduled_in" = "id") ) |>
  dplyr::left_join(
    y = procedures_doc_id,
    by = c("inverse_was_scheduled_in" = "process_id") ) |>
  dplyr::left_join(
    y = procedures_rapporteur,
    by = c("inverse_was_scheduled_in" = "process_id") ) |>
  dplyr::left_join(
    y = procedures_cmt,
    by = c("inverse_was_scheduled_in" = "process_id") ) |>
  dplyr::left_join(
    y = procedures_renew_shadow,
    by = c("inverse_was_scheduled_in" = "process_id") ) |>
  dplyr::select(
    id, activity_date, activity_order, notation_agenda_point,
    activity_label_en, activity_label_fr,
    agenda_label_en, agenda_label_fr,
    has_locality, procedure_id, legis_procedure, is_urgent,
    doc_id, committee, rapporteur, renew_shadow) |>
  dplyr::mutate(
    activity_order_day =
      as.integer(activity_order) - min(as.integer(activity_order)) + 1L,
    .by = activity_date) |>
  dplyr::arrange(activity_date, activity_order_day)


# Store as .rds ---------------------------------------------------------------#
dir.create(path = here::here("data_out", "meetings", "meetings_foreseen_rds"),
           recursive = TRUE, showWarnings = FALSE)
# Store as .rds
readr::write_rds(x = final_dt, file = here::here(
  "data_out", "meetings", "meetings_foreseen_rds",
  paste0("foreseen_activities_", pl_date, ".rds") )
)

# Store as json --------------------------------------------------------------#
dir.create(path = here::here("data_out", "meetings", "meetings_foreseen_json"),
           recursive = TRUE, showWarnings = FALSE)
final_dt |>
  tidyr::nest(data = dplyr::everything(),
              .by = activity_date) |>
  jsonlite::write_json(path = here::here(
    "data_out", "meetings", "meetings_foreseen_json", "foreseen_activities_latest.json"),
    na = "null")

#------------------------------------------------------------------------------#
# # Document type
# type <- gsub(pattern = "(^.*type\\>)(.*)(\\:?\\<\\/type\\>.*$)", replacement = "\\2",
#              x = votes_foreseen$structured_content_en, perl = TRUE)
#
# # Procedure ID
# procedure_id <- gsub(pattern = "(^.*)(\\d{4}\\/\\d{4}[A-Z]?\\([A-Z]{3,4}\\))(.*$)",
#                      replacement = "\\2", x = votes_foreseen$structured_content_en,
#                      perl = TRUE)
#

# # MEP pers_id
# pers_id = gsub(pattern = '(^.*)(resource\\=\\\"person\\/.*)("\\>.*$)',
#                replacement = "\\2", x = votes_foreseen$structured_content_en,
#                perl = TRUE)
# pers_id = ifelse(
#   test = grepl(pattern = 'resource=\"person/', x = pers_id, fixed = TRUE),
#   yes =  gsub(pattern = 'resource=\"person/', replacement = "",
#               x = pers_id, fixed = TRUE),
#   no = NA)
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
# Clean up before exiting -----------------------------------------------------#

# Run time
script_ends <- Sys.time()
script_lapsed = script_ends - script_starts
cat("\n=====\nFinished collecting Plenary Foreseen Activities.\n=====\n")
print(script_lapsed) # Execution time

# Remove objects
rm(list = ls(pattern = "^script_"))
