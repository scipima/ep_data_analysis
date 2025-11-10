###--------------------------------------------------------------------------###
# Vote Metrics of the Day ------------------------------------------------------
###--------------------------------------------------------------------------###

#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c("data.table", "dplyr", "forcats", "ggiraph", "ggplot2",
                        "here", "lubridate", "janitor", "quarto", "stringr",
                        "tidyr", "tidyselect", "tidytext") )


# Hard code the start of the mandate ------------------------------------------#
mandate_starts <- as.Date("2024-07-14")


#------------------------------------------------------------------------------#
## Functions -------------------------------------------------------------------
# Join functions --------------------------------------------------------------#
source(file = here::here("scripts_r", "join_functions.R") )

# Calculate Majorities --------------------------------------------------------#
source(file = here::here("scripts_r", "get_majority.R") )

# Mode ------------------------------------------------------------------------#
# https://stackoverflow.com/questions/2547402/how-to-find-the-statistical-mode
stat_mode <- function(x) {
    if ( length(x) <= 2 ) return(x[1])
    if ( anyNA(x) ) x = x[!is.na(x)]
    ux <- unique(x)
    ux[ which.max( tabulate( match(x, ux) ) ) ] }


#------------------------------------------------------------------------------#
## Graphics --------------------------------------------------------------------
# set theme globally for ggplots --------------------------------------------###
# https://stackoverflow.com/questions/34522732/changing-fonts-in-ggplot2
theme_set(theme_minimal(base_size = 10) )


# vote colours ----------------------------------------------------------------#
vote_colours <- c(`for` = '#00AEEF',
                  against = '#BE3455',
                  abstain = "#969696",
                  no_vote = '#5D5CA4',
                  absent = '#F47920')

# political groups colours ----------------------------------------------------#
polgroup_cols = c(`S&D` = "#EE3652",
                  ECR = "#0D88C3",
                  PfE = "#1A3153",
                  PPE = "#3C5979",
                  Renew = "#FFCC70",
                  `Verts/ALE` = "#19A24A",
                  `The Left` = "#733542",
                  ESN = "#000000",
                  NI = "#979797")


# Hard-code parameters --------------------------------------------------------#
renew_polgroup_id <- 7035L


###--------------------------------------------------------------------------###
## Read data -------------------------------------------------------------------
meps_rcv_mandate <- data.table::fread(
    file = here::here("data_out", "meps_rcv_mandate_10.csv"),
    verbose = TRUE, key = c("rcv_id", "pers_id"),
    na.strings = c(NA_character_, "") )
pl_votes <- data.table::fread(
    file = here::here("data_out", "votes", "pl_votes_10.csv"),
    select = c("activity_date", "rcv_id", "mandate", "number_of_attendees",
               "number_of_votes_abstention", "number_of_votes_against",
               "number_of_votes_favor"),
    na.strings = c(NA_character_, "") )

#------------------------------------------------------------------------------#
# get length objects
n_votes_tot <- nrow(pl_votes)
n_rcv_tot <- length( unique( meps_rcv_mandate$rcv_id ) )


#------------------------------------------------------------------------------#
# hard code today's date
today_date <- gsub(pattern = "-", replacement = "",
                   x = as.character( Sys.Date() ) )
# today_date <- "20250618" # test
rcv_today <- data.table::fread(here::here(
    "data_out", "daily", paste0("rcv_today_", today_date,".csv") ),
    na.strings = c(NA_character_, "") )
votes_today <- data.table::fread(here::here(
    "data_out", "daily", paste0("votes_today_", today_date,".csv") ),
    na.strings = c(NA_character_, "") )

#------------------------------------------------------------------------------#
# get length objects
n_votes_today <- nrow(votes_today)
n_rcv_today <- length( unique( rcv_today$rcv_id ) )


#------------------------------------------------------------------------------#
# Read in function
source(file = here::here("scripts_r", "cohesionrate_function.R") )

# convert to factor - essential for tabulate
meps_rcv_mandate[, result_fct := factor(result,
                                        levels = -3:1,
                                        labels = c("absent", "no_vote", "against",
                                                   "abstain", "for") ) ]
rcv_today[, result_fct := factor(result,
                                 levels = -3:1,
                                 labels = c("absent", "no_vote", "against",
                                            "abstain", "for") ) ]


#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
## Cohesion Rates --------------------------------------------------------------
### Overall cohesion rates -----------------------------------------------------

## Calculate Cohesion ---------------------------------------------------------#
# Full mandate's cohesion rates by RCV
cohesion_mandate <- meps_rcv_mandate[
    result >= -1L, # only official votes
    list(
        cohesion = cohesion_hn(result_fct),
        period = "10th legislature"),
    keyby = list(rcv_id, polgroup_id) ] |>
    join_polit_labs()

# Today's cohesion rates by RCV
cohesion_today <- rcv_today[
    result >= -1L, # only official votes
    list(
        cohesion = cohesion_hn(result_fct),
        period = as.character( lubridate::ymd(today_date) ) ),
    keyby = list(rcv_id, polgroup_id) ] |>
    join_polit_labs()

# Calculate average cohesion --------------------------------------------------#
cohesion_bypolgroup_avg <- dplyr::bind_rows(
    cohesion_mandate, cohesion_today) |>
    dplyr::group_by(political_group, period) |>
    dplyr::summarise(avg = round( mean(cohesion, na.rm = TRUE), digits = 1) ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
        period = factor(period,
                        levels = c(as.character( lubridate::ymd(today_date) ),
                                   "10th legislature") )
        ) |>
    # na.omit() |>
    join_polit_labs() |>
    dplyr::arrange(avg)

# get Political Group with max cohesion rate for mandate
today_date_chr = as.character(lubridate::ymd(today_date))

pg_max_cr_mandate_avg <- cohesion_bypolgroup_avg |>
    dplyr::filter(
        period == "10th legislature"
    ) |>
    dplyr::slice_max(order_by = avg, n = 1, with_ties = TRUE) |>
    dplyr::pull(political_group)

# get Political Group with max cohesion rate for TODAY
pg_max_cr_today_avg <- cohesion_bypolgroup_avg |>
    dplyr::filter(
        period == today_date_chr
    ) |>
    dplyr::slice_max(order_by = avg, n = 1, with_ties = TRUE) |>
    dplyr::pull(political_group)


#------------------------------------------------------------------------------#
### Final Cohesion -------------------------------------------------------------
votes_final <- data.table::fread( here::here(
    "data_out", "votes", "votes_final_10.csv") )

# Merge RCV with Finals -------------------------------------------------------#
cohesion_mandate <- votes_final[cohesion_mandate, on = "rcv_id"]
rcvid_today_finals <- votes_today$rcv_id[votes_today$is_final == 1L]
cohesion_today[, is_final := ifelse(rcv_id %in% rcvid_today_finals, 1L, 0L)]

# Colours ---------------------------------------------------------------------#
cols_final <- c(Final = "#0868ac", `Not Final` = "#d7301f")

# Plot ------------------------------------------------------------------------#
# REF: https://stackoverflow.com/questions/40211451/geom-text-how-to-position-the-text-on-bar-as-i-want
cohesion_bypolgroup_final <- dplyr::bind_rows(
    cohesion_mandate, cohesion_today) |>
    dplyr::group_by(political_group, period, is_final ) |>
    dplyr::summarise(avg = round( mean(cohesion, na.rm = TRUE), digits = 1) ) |>
    dplyr::ungroup() |>
    na.omit() |>
    join_polit_labs() |>
    dplyr::mutate(
        is_final = ifelse(test = is_final == 1L,
                          yes = "Final", no = "Not Final"),
        period = factor(period,
                        levels = c(as.character( lubridate::ymd(today_date) ),
                                   "10th legislature") ) ) |>
    dplyr::arrange(avg)


# get Political Group with max cohesion rate for mandate
pg_max_cr_mandate_final <- cohesion_bypolgroup_final |>
    dplyr::filter(
        period == "10th legislature"
        & is_final == "Final"
    ) |>
    dplyr::slice_max(order_by = avg, n = 1, with_ties = TRUE) |>
    dplyr::pull(political_group)

# get Political Group with max cohesion rate for TODAY
pg_max_cr_today_final <- cohesion_bypolgroup_final |>
    dplyr::filter(
        period == today_date_chr
        & is_final == "Final"
    ) |>
    dplyr::slice_max(order_by = avg, n = 1, with_ties = TRUE) |>
    dplyr::pull(political_group)


#------------------------------------------------------------------------------#
## Where were we less cohesive? ------------------------------------------------

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

worst_cohesion <- cohesion_today |>
    dplyr::filter(polgroup_id == renew_polgroup_id) |>
    dplyr::select(-c(is_final, period, political_group, polgroup_id)) |>
    dplyr::mutate(cohesion = round(cohesion, 1) ) |>
    dplyr::slice_min(order_by = cohesion, n = 10) |>
    dplyr::left_join(
        y = votes_today[, list(rcv_id, doc_id, vote_label, activity_label_mul)],
        by = "rcv_id")
