###--------------------------------------------------------------------------###
# Automate Vote Analysis by Committee ------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if (!require("pacman")) install.packages("pacman")
pacman::p_load(char = c("data.table", "dplyr", "here", "janitor", "knitr",
                        "lubridate", "readxl", "tidyr") )

# hard code today's date
if ( !exists("today_date") ) {
    today_date <- gsub(pattern = "-",
                       replacement = "", x = as.character( Sys.Date() ) )
}
today_ymd <- Sys.Date()
# today_date <- "20250618" # test
# today_ymd <- "2025-06-18" # test
today_date_print <- paste(lubridate::day(today_ymd),
                          lubridate::month(today_ymd, label = TRUE,
                                           abbr = FALSE),
                          lubridate::year(today_ymd) )

### Votes ----------------------------------------------------------------------
if ( !exists("votes_today") ) {
    votes_today <- data.table::fread(file = here::here(
        "data_out", "daily", paste0("votes_today_", today_date, ".csv") ),
        na.strings = c(NA_character_, "")  )
}

# Catch inconsistencies in data stream ----------------------------------------#
if ( "vote_label_en" %in% names(votes_today) ) {
    data.table::setnames(x = votes_today,
                         old = "vote_label_en", new = "vote_label")
} else if ( "vote_label_fr" %in% names(votes_today) ) {
    data.table::setnames(x = votes_today,
                         old = "vote_label_fr", new = "vote_label")
} else if ( "vote_label_mul" %in% names(votes_today) ) {

    # Extract multilingual vector
    vote_label_mul <- votes_today$vote_label_mul

    # List of split vectors
    vote_labels_list = stringr::str_split(string = vote_label_mul,
                                          pattern = "\\s-\\s")

    # Delimiter counts
    delim_count = stringi::stri_count(str = vote_label_mul,
                                      regex = "\\s-\\s")
    delim_first = delim_count/2
    # idx = 2L

    # Empty repository
    vote_labels_en <- vector(mode = "list", length = length(vote_label_mul))

    for (i_row in seq_along(vote_label_mul)) {

        # Extract string vector
        full_label = vote_labels_list[[ i_row ]]

        # Store ENG title
        vote_labels_en[[i_row]] <- paste0(
            full_label[ (delim_first[ i_row ] + 1) : delim_count[ i_row ] ],
            collapse = " - ")
    }

    # Append vector to original data
    votes_today <- cbind(votes_today, vote_label = unlist(vote_labels_en) )

} else {
    stop("You have no vote title! Check again.")
}

# Filter just to final votes
cols_tokeep <- c("doc_id", "rcv_id", "vote_label", "referenceText_en", "is_final",
                 "referenceText_fr", "activity_label_mul", "activity_label_fr",
                 "activity_label_en", "number_of_attendees", "number_of_votes_favor",
                 "number_of_votes_abstention", "number_of_votes_against", "vote_id")

final_votes <- votes_today |>
    # Only FINAL RCVs
    dplyr::filter(
        is_final == 1
        & decision_method == "VOTE_ELECTRONIC_ROLLCALL"
    ) |>
    # Slim down to just selected cols
    dplyr::select( any_of(cols_tokeep) ) |>
    # Flag for multiple final for a single file - UNLIKELY
    dplyr::mutate(is_multiple_final = ifelse(
        test = sum(is_final, na.rm = TRUE) > 1L,
        yes = 1L, no = 0L),
        .by = vote_id) |>
    data.table::as.data.table()

if ( !"number_of_attendees" %in% names(votes_today) ) {
    final_votes$number_of_attendees <- NA
}


# Convert NA to 0
cols_outcome <- c("number_of_votes_favor", "number_of_votes_abstention",
                  "number_of_votes_against")
final_votes[, c(cols_outcome) := lapply(
    X = .SD,
    FUN = function(x) {ifelse(test = is.na(x), 0L, x) } ),
    .SDcols =  cols_outcome]

# CHECK: Do we have a majority for each vote?
final_votes[, n_max := apply(
    X = .SD, MARGIN = 1,
    FUN = function(x) {
        length( x[x == max(x)] )
    } ),
    .SDcols =  cols_outcome]

if ( any(final_votes$n_max > 1L) ) {
    stop("One or more votes does not have a majority!")
}

# Get the max col
final_votes[, vote_outcome_int := max.col(.SD),
            .SDcols = cols_outcome]
# Rename
final_votes[vote_outcome_int == 1L, vote_outcome := "for"]
final_votes[vote_outcome_int == 3L, vote_outcome := "against"]
final_votes[vote_outcome_int == 2L, vote_outcome := "abstain"]
# Calculate N. attendees
final_votes[,
            votes_sum := rowSums(.SD),
            .SDcols = c("number_of_votes_favor", "number_of_votes_abstention",
                        "number_of_votes_against")
]
# If attendees missing, then insert calculated fields
final_votes[, `:=`(
    number_of_attendees = ifelse(
        test = is.na(number_of_attendees),
        yes = votes_sum, no = number_of_attendees)
)
]
# Drop cols
final_votes[, c("votes_sum", "n_max", "vote_outcome_int") := NULL] # Remove tmp col

# Fix the string of vote title - there cannot be any stars otherwise the Markdown interpret them as italics or bold
final_votes[, vote_label := gsub(pattern = "\\s\\*{1,3}I",
                                 replacement = "", x = vote_label)]


# If there are missing DOC IDs, we rectify them here
if ( any( is.na(final_votes$doc_id) ) ) {
    warning("There are missing DOC IDs in today's votes. This could create issues in later joins. Please check!")
}
# Last minute changes/Emergency files might have missing fields - Change accordingly
# final_votes[rcv_id == 176091, doc_id := "C10-0064/2025"]


### Include Committees ---------------------------------------------------------

#------------------------------------------------------------------------------#
#' Here we import data also from API `meetings/foreseen-activities`.
#' This is necessary as sometimes the data in the Vote Results is incomplete.
#' So, we grab info from the foreseen activities (which in turn also calls the `procedures` endpoint) to augment our available data.
#------------------------------------------------------------------------------#

# Load foreseen activities
meetings_foreseen <- readr::read_rds(file = here::here(
    "data_out", "meetings", "meetings_foreseen_rds",
    paste0("foreseen_activities_", today_date, ".rds") ) ) |>
    dplyr::filter(activity_date == today_ymd)

process_docid_cmt <- full_join(
    x = meetings_foreseen |>
        dplyr::select(procedure_id, doc_id) |>
        tidyr::unnest(doc_id),
    y = meetings_foreseen |>
        dplyr::select(procedure_id, committee) |>
        tidyr::unnest(committee),
    by = "procedure_id") |>
    data.table::as.data.table()

# Here again we must fix some missing DOC IDs
if ( any( is.na(process_docid_cmt$doc_id) ) ) {
    warning("There are missing DOC IDs in today's votes. This could create issues in later joins. Please check!")
}
# Last minute changes/Emergency files might have missing fields - Change accordingly
# process_docid_cmt[procedure_id == "2025/0085(COD)", doc_id := "C10-0064/2025"]

# Merge vote results with foreseen activities data
final_votes <- final_votes |>
    dplyr::left_join(
        y = process_docid_cmt,
        by = "doc_id"
    ) |>
    dplyr::group_by(rcv_id, doc_id) |>
    dplyr::mutate(committee_lab_nested = paste0( committee_lab, collapse = "-") ) |>
    dplyr::ungroup()

# If there is no `committee_lab` column present, create an empty one
if ( !"committee_lab" %in% names(final_votes) ) {
    final_votes[, committee_lab := NA]
}


if ( file.exists(here::here(
    "data_out", "daily", paste0("epp_coalitions_", today_date, ".csv")
    ) ) ) {
    cat("\nThere were breached of the Coalition Agreement today.\n")

    # Read in data
    epp_coalitions = data.table::fread(file = here::here(
        "data_out", "daily", paste0("epp_coalitions_", today_date, ".csv") ),
        select = c("rcv_id", "is_pfe_ecr", "is_pfe_ecr_esn") )

    # Merge
    final_votes = epp_coalitions[
        final_votes,
        on = "rcv_id"
    ]
} else {
    cat("\nThere was no breach of the Coalition Agreement today.\n")
}

#------------------------------------------------------------------------------#
## Write csv -------------------------------------------------------------------
dir.create(path = here::here("data_out", "tmp", "pa_analysis"),
           showWarnings = FALSE, recursive = TRUE)
data.table::fwrite(x = final_votes, file = here::here(
    "data_out", "tmp", "pa_analysis", "final_votes.csv"
))


#------------------------------------------------------------------------------#
# Read in reference grid
automated_vote_analysis_send_list <- data.table::fread(input = here::here(
    "data_reference", "automated_vote_analysis_send_list.csv") ) |>
    janitor::clean_names()

# Vector of WG present in today's votes
today_wg <- unique(automated_vote_analysis_send_list$working_group[
    automated_vote_analysis_send_list$committee %in% unique(final_votes$committee_lab)
])
# If no Committee has been assigned to a file, we give it to WG-B
if ( any( is.na(final_votes$committee_lab) ) ) {
    today_wg <- c(today_wg, "WG-B")
}


#------------------------------------------------------------------------------#
## Loop to knit .docx ----------------------------------------------------------
dir.create( path = here::here("analyses", "pa_analysis", "out_docx") )

# Loop
for ( i_wg in seq_along( today_wg) ) {
    quarto::quarto_render(
        input = here::here("analyses", "pa_analysis", "pa_analysis.qmd"),
        output_format = "docx",
        output_file = paste0("pa_analysis_i_wg.docx"),
        execute_params = list(
            working_group = today_wg[i_wg]
        )
    )

    # Move file from main branch to sub-folder --------------------------------#
    file.rename(
        from = here::here("analyses", "pa_analysis", "pa_analysis_i_wg.docx"),
        to = here::here("analyses", "pa_analysis", "out_docx",
                        paste0("pa_analysis_", today_wg[i_wg], ".docx") )
        )
}



