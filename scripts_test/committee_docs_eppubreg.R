###--------------------------------------------------------------------------###
# Committee and OEIL Documents -------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' The script downloads all Committee documents from the EP Public Register.
#' The webpage can be retrieved here: https://www.europarl.europa.eu/RegistreWeb/.
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
library(dplyr)
library(readr)
library(tidyr)
library(xml2)
library(httr2)
library(janitor)
library(future.apply)
library(data.table)


#------------------------------------------------------------------------------#
# Hard code the start of the mandate ------------------------------------------#
mandate_starts <- as.Date("2024-07-14")


#------------------------------------------------------------------------------#
source(file = here::here("scripts_r", "join_functions.R"))


#------------------------------------------------------------------------------#
## Get last affiliation for all MEPS -------------------------------------------
meps_dates_ids <- read_csv(
  here::here("data_out", "meps", "meps_dates_ids_all.csv") ) |>
  dplyr::group_by(pers_id) |>
  dplyr::slice_max(order_by = activity_date, n = 1, with_ties = FALSE) |>
  dplyr::ungroup() |>
  dplyr::select(- activity_date)



#------------------------------------------------------------------------------#
## PPVD ----------------------------------------------------------------------
body_json <- '{"terms":[9,10],"types":["OPLE","TAVF","TPRA","TPRR"],"sortAndOrder":"DATE_DOCU_DESC","nbRows":10000}'

# Build query
request_query <- httr2::request(
  "https://www.europarl.europa.eu/RegistreWeb/services/search"
) |>
  httr2::req_headers("Content-Type"="application/json") |>
  httr2::req_body_raw(body_json)

# Perform request
request_perform <- httr2::req_perform(request_query)

# get json
request_json <- httr2::resp_body_json(resp = request_perform, simplifyVector=TRUE)

# clean data
committee_docs_pubreg <- request_json$references |>
  # get rid of useless cols
  dplyr::select(-c(
    score, leg, dateEvnt, dateRecep, dateInscri, dateDiff, commentRef, codeDiff,
    authoritiesReceivers, auteursAdonis, authoritesAdonis, destinatairesAdonis,
    eurovocsLevel1, eurovocsLevel2, eurovocsLevel3, directoryCodesLevel1,
    directoryCodesLevel2, directoryCodesLevel3, directoryCodesLevel4,
    policyAreas, geographicalAreas, subjectHeadings, highlightText) ) |>
  # expand `relations` cols/df & merge it back --------------------------------#
  dplyr::left_join(
    y = request_json$references |>
      dplyr::select(docId, relations) |>
      tidyr::unnest(cols = relations) |>
      tidyr::pivot_wider(names_from = type, values_from = code,
                         names_prefix = "relations_",
                         values_fn = ~paste(.x, collapse = "; ") ),
    by = "docId") |>
  dplyr::select(-relations) |>
  # expand `authoritiesAuthors` cols/df & merge it back -----------------------#
  dplyr::left_join(
    y = request_json$references |>
      dplyr::select(docId, authoritiesAuthors) |>
      tidyr::unnest(cols = authoritiesAuthors) |>
      tidyr::pivot_wider(names_from = type, values_from = code,
                         names_prefix = "authorities_authors_",
                         values_fn = ~paste(.x, collapse = "; ") ),
    by = "docId") |>
  dplyr::select(-authoritiesAuthors) |>
  # expand `allAuthorities` cols/df & merge it back ---------------------------#
  dplyr::left_join(
    y = request_json$references |>
      dplyr::select(docId, allAuthorities) |>
      tidyr::unnest(cols = allAuthorities) |>
      tidyr::pivot_wider(names_from = type, values_from = code,
                         names_prefix = "all_authorities_",
                         values_fn = ~paste(.x, collapse = "; ") ),
    by = "docId") |>
  dplyr::select(-allAuthorities) |>
  # get doc titles
  tidyr::unnest(cols = fragments) |>
  tidyr::unnest(cols = versions) |>
  dplyr::filter(language %in% c("EN", "FR")) |>
  dplyr::select(-c(version, commentDocu, summary, original, titleHighlights,
                   summaryHighlights, attachmentHighlights, value, fileInfos) ) |>
  tidyr::pivot_wider(names_from = language, names_prefix = "title_",
                     values_from = title) |>
  janitor::clean_names()


# Write data to disk ----------------------------------------------------------#
data.table::fwrite(x = committee_docs_pubreg,
                   here::here("data_out", "procedures", "procedures_eppubreg.csv"))
# Write list of Procedures IDs ------------------------------------------------#
write(x = committee_docs_pubreg$relations_procedure,
      here::here("data_out", "procedures", "procedures_eppubreg_ids.txt"))

# remove objects --------------------------------------------------------------#
rm(body_json, request_json, request_perform, request_query)


#------------------------------------------------------------------------------#
## Loop to get html -------------------------------------------------------------

# Vector of procedure ids
procedures_vct <- committee_docs_pubreg |>
  dplyr::select(relations_procedure) |>
  tidyr::separate_longer_delim(cols = relations_procedure, delim = ";") |>
  dplyr::pull(relations_procedure) |>
  na.omit() |>
  trimws() |>
  unique()

# Empty container
list_tmp <- vector(mode = "list", length = length(procedures_vct))

# Loop ------------------------------------------------------------------------#
for (link in seq_along(procedures_vct) ) {

  i_link = procedures_vct[link]
  print( i_link )

  # get webpage ---------------------------------------------------------###
  # Create request
  req <- httr2::request(
    paste0(
      "https://oeil.secure.europarl.europa.eu/oeil/popups/ficheprocedure.do?lang=en&reference=",
      i_link) )
  # Ignore error
  resp <- req |>
    httr2::req_error(is_error = ~FALSE) |>
    httr2::req_perform()
  # If not an error, download it
  if ( httr2::resp_status(resp) == 200L) {
    webpage <- resp |>
      httr2::resp_body_html()

    # subject -------------------------------------------------------------###
    subject <- webpage |>
      rvest::html_nodes("#basic-information-data .ep_gridrow .ep_gridcolumn:nth-child(1) p~ p+ p") |>
      rvest::html_text2()
    # entire grid ---------------------------------------------------------###
    entire_grid <- webpage |>
      rvest::html_nodes("#basic-information-data .ep_gridrow") |>
      rvest::html_text2()

    # subject -------------------------------------------------------------###
    key_events <- webpage |>
      rvest::html_nodes("#key_events-data") |>
      rvest::html_text()

    # put it all into df --------------------------------------------------###
    list_tmp[[link]] <- data.frame(
      subject = subject,
      entire_grid = entire_grid,
      key_events = key_events)
  }
}


#------------------------------------------------------------------------------#
## Append all DFs into a single DF, and save data ------------------------------
oeil_procedures <- data.table::rbindlist(
  l = setNames(object = list_tmp,
               nm = procedures_vct),
  use.names = T, fill = T, idcol = "procedure_id")

data.table::fwrite(x = oeil_procedures,
                 here::here("data_out", "procedures", "oeil_procedures.csv"))
# oeil_procedures <- data.table::fread(here::here("data_out", "procedures", "oeil_procedures.csv"))
readr::write_rds(x = list_tmp,
                 here::here("data_out", "procedures", "oeil_procedures.rds"))


#------------------------------------------------------------------------------#
## Processing ------------------------------------------------------------------
## get status
oeil_procedures[, process_status := sub(".*Status", "", entire_grid)]
oeil_procedures[, process_status := trimws(
  gsub(pattern = "\\\n|\\\r", replacement = "",
       x = process_status, ignore.case = TRUE))
]
oeil_procedures[, c("subject", "entire_grid") := NULL]


# Clean dates -----------------------------------------------------------------#
keyevents_raw_list <- stringr::str_split(string = oeil_procedures$key_events,
                        pattern = "\\\t(?=\\d{2}\\/\\d{2}/\\d{4})")

key_events_list <- lapply(
  X = setNames(object = keyevents_raw_list,
               nm = oeil_procedures$procedure_id),
  FUN = function(x) {
    p2 = stringr::str_replace_all(string = x,
                                  pattern = "\\\r\\\n", replacement = "")
    p3 = stringr::str_replace_all(string = p2,
                                  pattern = "\\\t{1,}\\s{0,}", replacement = " - ")
    p4 = stringr::str_replace(string = p3,
                              pattern = "\\s+-\\s+$", replacement = "")
    p5 = stringr::str_replace(string = p4,
                              pattern = "\\s+-\\s+Summary", replacement = "")
    p5 = p5[ !p5 %in% "" ]
  }
)


#------------------------------------------------------------------------------#
## Subset to Open Procedures ---------------------------------------------------
open_procedures <- oeil_procedures[
  !grepl(pattern = "rejected|lapsed|completed", x = process_status),
]
open_procedures[, c("key_events") := NULL]

# merge open procedures with all the info coming from Public Register
open_procedures_wide <- open_procedures |>
  dplyr::left_join(
    y = committee_docs_pubreg,
    by = c("procedure_id" = "relations_procedure")
  )


# Get vector of procedures where Renew MEPs are involved
renew_procedures_vct <- open_procedures_wide |>
  dplyr::select(doc_id, code_auteurs) |>
  tidyr::unnest(code_auteurs) |>
  # get MEPs' political groups
  dplyr::left_join(
    y = meps_dates_ids,
    by = c("code_auteurs" = "pers_id") ) |>
  # subset to renew MEPs
  dplyr::filter(polgroup_id %in% c(5704, 7035) ) |>
  # extract vector of relevant docs
  dplyr::pull(doc_id)

# subset to just procedures where Renew MEPs are involved
renew_open_procedures <- open_procedures_wide |>
  dplyr::filter(doc_id %in% renew_procedures_vct) |>
  dplyr::select(-c(date_docu, relations_fdr, relations_csl,
            authorities_authors_ct, authorities_authors_ce,
            all_authorities_co, all_authorities_ct, all_authorities_ce))

# Separate in 2 cols Rapporteurs and Opinions
renew_open_procedures_tab <- renew_open_procedures |>
  dplyr::mutate(rapporteur_opinion = ifelse(
    test = type_docu == "TPRR",  yes = "Rapporteur", no = "Opinion")) |>
  dplyr::select(procedure_id, process_status, title_en, authorities_authors_co, auteurs,
         rapporteur_opinion) |>
  dplyr::distinct() |>
  tidyr::unnest(auteurs) |>
  tidyr::pivot_wider(names_from = rapporteur_opinion, values_from = auteurs,
              values_fn = ~paste(.x, collapse = "; ") )

# append list of procedures ids and dates
procedures_keydates <- stack(key_events_list[
  names(key_events_list) %in% renew_open_procedures_tab$procedure_id]) |>
  filter(
    grepl(pattern = "Decision by Parliament|Committee report tabled for plenary",
          x = values, ignore.case = TRUE)) |>
  mutate(
    key_dates = ifelse(
      test = grepl(pattern = "Committee report tabled for plenary", x = values),
      yes = "Committee report tabled for plenary", no = "Decision by Parliament"),
    date = stringr::str_extract(string = values,
                                pattern = "\\d{2}\\/\\d{2}\\/\\d{4}") ) |>
  pivot_wider(id_cols = ind, names_from = key_dates, values_from = date,
              values_fn = ~paste(.x, collapse = "; "))

# reshape table to be in the same format
final_table <- renew_open_procedures_tab |>
  left_join(
    y = procedures_keydates,
    by = c("procedure_id" = "ind")) |>
  # For some procedures, we have both the DRAFT and the final REPORT or OPINION.
  # Here we delete the DRAFTS.
  dplyr::group_by(procedure_id, authorities_authors_co) |>
  dplyr::mutate(
    is_report = ifelse(grepl("report", x = title_en, ignore.case = TRUE), 1, 0 ),
    is_opinion = ifelse(grepl("opinion", x = title_en, ignore.case = TRUE), 1, 0 )
  ) |>
  dplyr::mutate(
    sum_report = sum(is_report, na.rm = TRUE),
    sum_opinion = sum(is_opinion, na.rm = TRUE)
  ) |>
  dplyr::filter(
    !(
      grepl(pattern = "DRAFT", x = title_en, ignore.case = TRUE)
      & (sum_report >= 2 | sum_opinion >= 2) )
  ) |>
  dplyr::select(
    Committee = authorities_authors_co, `Doc Title` = title_en, procedure_id,
    Rapporteur, Opinion, `Committee report tabled for plenary`,
    `Decision by Parliament`, `Procedure status` = process_status)


#------------------------------------------------------------------------------#
## Excel and upload ------------------------------------------------------------
writexl::write_xlsx(
  x = list(
    `Renew Open Procedures` = final_table
  ),
  path = here::here("data_out", "renew_open_procedures.xlsx") )

