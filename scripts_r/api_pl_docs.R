###--------------------------------------------------------------------------###
# EP Plenary Documents from Open Data API --------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
#' This script collects the list as well as all info relative to Plenary Documents from the EP API.
#' REF: https://data.europarl.europa.eu/en/home
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
# Clean ENV -------------------------------------------------------------------#
# rm(list = ls())


#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
packages <- c("data.table", "dplyr", "here", "httr2", "janitor",
              "jsonlite", "tidyr", "xml2")
if (any( !packages %in% pacman::p_loaded() ) ) {
  pacman::p_load(char = packages[ !packages %in% pacman::p_loaded() ] ) }
rm(packages)

source(file = here::here("scripts_r", "parallel_api_calls.R"))


#------------------------------------------------------------------------------#
## GET plenary-documents -------------------------------------------------------
# EXAMPLE: https://data.europarl.europa.eu/api/v2/plenary-documents?year=2022&format=application%2Fld%2Bjson&offset=0&limit=50
# create parameters to loop over
years <- paste(2016L : data.table::year(Sys.Date()), collapse="," )


### Get all meetings -----------------------------------------------------------
req <- httr2::request(
  paste0("https://data.europarl.europa.eu/api/v2/plenary-documents?year=",
         years,
         "&format=application%2Fld%2Bjson&offset=0") ) |>
  httr2::req_headers("User-Agent" = "renew_parlwork-prd-2.0.0") |>
  httr2::req_perform() |>
  httr2::resp_body_json(simplifyDataFrame = TRUE)

# Extract data ----------------------------------------------------------------#
pl_docs <- data.table::as.data.table(req$data)

# Clean
pl_docs[, `:=`(
  work_type = gsub(
    pattern = "http://publications.europa.eu/resource/authority/resource-type/|def/ep-document-types/",
    replacement = "", x = work_type) ) ]
# unique(pl_docs$work_type)


# Fix docs' labels ------------------------------------------------------------#
# create temporary duplicate col for string processing
pl_docs[, identifier2 := identifier]
# invert the orders of the groups for A-, B-, C- files
pl_docs[
  grepl(pattern = "^[ABC]{1}.\\d{1,2}.", x = identifier2),
  doc_id := gsub(
    pattern = "(^[ABC]{1}.\\d{1,2}.)(\\d{4}).(\\d{4})",
    replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
# treat RC separately
pl_docs[
  grepl(pattern = "^RC.\\d{1,2}", x = identifier2),
  doc_id := gsub(
    pattern = "(^RC.\\d{1,2}.)(\\d{4}).(\\d{4})",
    replacement = "\\1\\3-\\2", x = identifier2, perl = T) ]
pl_docs[, doc_id := gsub(pattern = "^RC", replacement = "RC-B",
                         x = doc_id, perl = T)]
# delete - between doc_id LETTER and MANDATE NUMBER
pl_docs[, doc_id := gsub(pattern = "([A-Z]{1}).(\\d{1,2})",
                         replacement = "\\1\\2", x = doc_id, perl = T)]
# slash at the end
pl_docs[, doc_id := gsub(pattern = "(?<=.\\d{4}).",
                         replacement = "/", x = doc_id, perl = T)]
# Delete cols
pl_docs[, c("identifier2", "label", "type") := NULL]
# sapply(pl_docs, function(x) sum(is.na(x))) # check


# write data ------------------------------------------------------------------#
write(x = paste0("https://www.europarl.europa.eu/doceo/document/",
                 pl_docs$identifier[
                   !grepl(pattern = "^QOB-", x = pl_docs$identifier)
                 ],
                 "_EN.html"),
      here::here("data_out", "docs_pl", "all_pl_docs_doceo_links.txt") )


# Remove API objects ----------------------------------------------------------#
rm(req, years)


#------------------------------------------------------------------------------#
## GET/plenary-documents/doc-id ------------------------------------------------

#------------------------------------------------------------------------------#
#' Returns a single EP Plenary Document for the specified doc ID.
#' This process can be extremely long, as there are thousands such files.
#' There are several ways to tackle this.
#' One is to use Bash and either `wget` or `curl`.
#' However, in a Windows OS, this could become tricky very quickly.
#' The other option is to remain within `R`, and split the list of Doc IDs into chunks, and download these files in batches.
#' As these are living documents, we need to check for changes.
#' Within `R`, this is tricky, so opt to overwrite the whole folder once per month.
#------------------------------------------------------------------------------#

# get doc ids, getting rid of oral question -----------------------------------#
# unique(pl_docs$work_type)
doc_ids <- sort(unique(pl_docs$identifier[
  pl_docs$work_type !=
    "QUESTION_RESOLUTION_MOTION" ] ) )

# create api params - this is the list of html files if you use bash ----------#
api_params <- paste0("https://data.europarl.europa.eu/api/v2/plenary-documents/",
                     doc_ids,
                     "?format=application%2Fld%2Bjson")

# write URLs to text file - we index this .txt file to download from API ------#
write(x = api_params, here::here(
  "data_out", "docs_pl", "all_pl_docs_api.txt"))


### Download all these files if very long, make sure we do it rarely -----------
# Get the day difference between today and the last file ----------------------#
last_doc <- as.integer(
  Sys.Date() - as.Date(
    file.info(
      here::here("data_out", "docs_pl", "pl_docs_list.rds")
    )[["mtime"]] ) )

# Get new Docs IDs ------------------------------------------------------------#
if ( file.exists(here::here("data_out", "docs_pl", "pl_docs.csv") ) ) {
  pl_docs_in <- data.table::fread(
    file = here::here("data_out", "docs_pl", "pl_docs.csv"))
  # Get the IDs of the ones already on disk (as a vector)
  doc_ids_in <- pl_docs_in[
    work_type != "QUESTION_RESOLUTION_MOTION"]$identifier
  # retain only the new ones
  doc_ids_missing <- doc_ids[
    !doc_ids %in% doc_ids_in
  ]
} else {
  # write to disk for the first execution ever
  # !!DO NOT WRITE TO DISK THIS FILE BEFORE!!
  # OTHERWISE YOU WILL NEVER DOWNLOAD ANYTHING NEW
  data.table::fwrite(x = pl_docs, file = here::here(
    "data_out", "docs_pl", "pl_docs.csv"))
}


#### If there are no files, or they are older than 30 days ---------------------
if ( last_doc > 30L | is.na(last_doc) | !exists("doc_ids_missing") ) {

  # Build URLs for document chunks --------------------------------------------#
  chunk_size <- 50L  # Reduced from 50 to avoid timeouts
  doc_ids_chunks <- split(x = doc_ids, ceiling(seq_along(doc_ids) / chunk_size))

  # Build API URLs for each chunk
  api_urls <- sapply(doc_ids_chunks, function(chunk) {
    paste0(
      "https://data.europarl.europa.eu/api/v2/plenary-documents/",
      paste0(chunk, collapse = ","),
      "?format=application%2Fld%2Bjson"
    )
  })

  # Use parallel API calls for all documents
  cat("Processing",
      length(api_urls),
      "document chunks - using parallel processing\n"
    )

  results <- parallel_api_calls(
    urls = api_urls,
    capacity = 495,
    fill_time_s = 300,
    timeout_s = 600,
    show_progress = TRUE,
    extract_data = TRUE
  )

  # DEBUG: Analyze results immediately
  cat("\n=== DEBUGGING RESULTS ===\n")
  cat("Total API URLs:", length(api_urls), "\n")
  cat("Total responses:", length(results$responses), "\n")
  cat("Failed calls:", sum(results$failed_calls), "\n")
  cat("NULL responses:", sum(sapply(results$responses, is.null)), "\n")

  # Find which responses are NULL
  null_indices <- which(sapply(results$responses, is.null))
  if (length(null_indices) > 0) {
    cat("NULL response indices:", paste(null_indices, collapse = ", "), "\n")
    cat("Corresponding failed_calls status:\n")
    for (idx in head(null_indices, 5)) {  # Show first 5 NULL responses
      cat("  Response", idx, "- failed:", results$failed_calls[idx], "\n")
      cat("  Corresponding URL:", substr(api_urls[idx], 1, 150), "...\n")
    }
  }
  cat("=========================\n")

  if ( any(results$failed_calls) ) {
    pl_docs_list <- results$responses[!results$failed_calls]
  } else {
    pl_docs_list <- results$responses
    rm(results)
  }

  # Write tmp file --------------------------------------------------------#
  readr::write_rds(x = pl_docs_list, file = here::here(
    "data_out", "docs_pl", "pl_docs_list.rds") )

  ##### Process all docs -------------------------------------------------------
  source( file = here::here("scripts_r", "process_pl_docs.R") )


  # Remove objects
  rm(pl_docs_list)

} else if ( length(doc_ids_missing) > 0L ) {
  #### If not, download the missing files --------------------------------------

  # Build URLs for missing document chunks ------------------------------------#
  chunk_size <- 20L  # Reduced from 50 to avoid timeouts
  doc_ids_chunks <- split(x = doc_ids_missing, ceiling(seq_along(doc_ids_missing) / chunk_size))

  # Build API URLs for each chunk
  api_urls <- sapply(doc_ids_chunks, function(chunk) {
    paste0(
      "https://data.europarl.europa.eu/api/v2/plenary-documents/",
      paste0(chunk, collapse = ","),
      "?format=application%2Fld%2Bjson"
    )
  })

  # Use parallel API calls for missing documents
  use_parallel <- length(api_urls) > 1
  if (use_parallel) {
    cat("Processing", length(api_urls), "missing document chunks - using parallel processing\n")
    results <- parallel_api_calls(
      urls = api_urls,
      capacity = 120, # More conservative rate for incremental updates (1 every 2.5 seconds)
      fill_time_s = 300,
      show_progress = TRUE,
      extract_data = TRUE
    )
  } else {
    cat("Single missing document chunk - using sequential processing\n")
    results <- parallel_api_calls(
      urls = api_urls,
      show_progress = FALSE,
      extract_data = TRUE
    )
  }

  list_tmp <- results$responses[!results$failed_calls]

  # If there are no new files, just read in what you have in the bank ---------#
  pl_docs_list <- readr::read_rds(file = here::here(
    "data_out", "docs_pl", "pl_docs_list.rds") )
  # Append the old and new list
  pl_docs_list <- c(pl_docs_list, list_tmp)
  # Overwrite tmp file --------------------------------------------------------#
  readr::write_rds(x = pl_docs_list, file = here::here(
    "data_out", "docs_pl", "pl_docs_list.rds") )

  ##### Process all docs -------------------------------------------------------
  source( file = here::here("scripts_r", "process_pl_docs.R") )


  # Remove objects ------------------------------------------------------------#
  rm(list_tmp, pl_docs_list)

} else {

  #### Else, write to disk the current file ------------------------------------
  data.table::fwrite(x = pl_docs, file = here::here(
    "data_out", "docs_pl", "pl_docs.csv") )
}


#------------------------------------------------------------------------------#
### BASH: scrape only if difference is bigger than a month ---------------------
# if( last_doc > 30L | is.na(last_doc) ) {
#   # read data across OS
#   if (.Platform$OS.type == "windows") {
#     system2(command = "wsl",
#             args = paste(
#               "wget -N -P --wait=1",
#               here::here("data_in", "pl_docs_api_json"),
#               "-i",
#               here::here("data_out", "docs_pl", "all_pl_docs_api.txt") ) )
#   } else if (.Platform$OS.type == "unix") {
#     system2(command = "wget",
#             args = paste(
#               "-N -P --wait=1",
#               here::here("data_in", "pl_docs_api_json"),
#               "-i",
#               here::here("data_out", "docs_pl", "all_pl_docs_api.txt") ) ) }
# }
# rm(last_doc)

# If you have a Bash terminal available, these are also useful options --------#
# wget -N -P data_in/pl_docs_api_json -i data_out/docs_pl/all_pl_docs_api.txt
# wget --wait=2 -P data_in/pl_docs_api_json -i data_out/docs_pl/all_pl_docs_api.txt
# wget -nc -P data_in/pl_docs_api_json -i data_out/docs_pl/all_pl_docs_api.txt
#------------------------------------------------------------------------------#
