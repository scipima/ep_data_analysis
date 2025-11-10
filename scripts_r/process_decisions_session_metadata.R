###--------------------------------------------------------------------------###
# Process Vote Data ------------------------------------------------------------
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
#' This function is meant to operate on a nested data.frame, with several classes of cols.
#' We tackle the flat part first, which gives us the RCV metadata.
#' Then we grab all the dataframe-cols, unnest them, and keep only 3 languages (if available).
#' Finally, we grab the list-cols and unnest them.
#' The end result is data.frame in classic tabular format.
###--------------------------------------------------------------------------###

process_decisions_session_metadata <- function( votes_raw = resp_list[[1]] ) {
    print(unique(votes_raw$activity_date))

    # Check col names & rename ------------------------------------------------#
    if ( sum(grepl(pattern = "activity_order", x = names(votes_raw))) > 1L ) {
        stop("You may have 2 cols with similiar name and content. Simplify.")
    } else if (sum(grepl(pattern = "activity_order", x = names(votes_raw))) == 1L ){
        names(votes_raw)[
            grepl(pattern = "activity_order",
                  x = names(votes_raw))
        ] <- "activity_order"
    } else {
        warning("You have no 'activity_order' column.")
    }


    #### Flat cols -------------------------------------------------------------
    # sapply(votes_raw, class)

    # sometimes the class of some cols is corrupt - fix it here
    cols_character <- c("id", "activity_date", "activity_id", "activity_start_date",
                        "decision_method", "had_activity_type",
                        "decisionAboutId", "decisionAboutId_XMLLiteral",
                        "decided_on_a_part_of_a_realization_of")
    cols_integer <- c("activity_order", "notation_votingId", "number_of_attendees",
                      "number_of_votes_abstention", "number_of_votes_against",
                      "number_of_votes_favor")

    # Deal with changing classes between Plenaries -----------------------------
    # sometimes `type` is of class character
    if (class(votes_raw$type) == "character") {
        cols_character <- c(cols_character, "type") }
    # sometimes `decision_outcome` is of class character
    decision_outcome_cols <- names(votes_raw)[
        grepl(pattern = "decision_outcome", x = names(votes_raw) ) ]
    if ( length(decision_outcome_cols) == 1L
         && class(votes_raw[, decision_outcome_cols]) == "character") {
        cols_character <- c(cols_character, decision_outcome_cols) }

    # convert cols
    votes_raw <- votes_raw |>
        dplyr::mutate(
            across(.cols = tidyselect::any_of( cols_character ), as.character),
            across(.cols = tidyselect::any_of( cols_integer ), as.character),
            across(.cols = tidyselect::any_of( cols_integer ), as.integer) )

    # get flat cols
    votes_day <- votes_raw[, names(votes_raw) %in% c(cols_integer, cols_character)] |>
        dplyr::distinct() # DEFENSIVE: there may be duplicate rows

    if (class(votes_raw$type) == "list") {
        # `type` is the only col we have to unnest wider that is really a character
        votes_day <- votes_raw |>
            dplyr::select(activity_id, type) |>
            dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
            tidyr::unnest_wider(type, names_sep = "_") |>
            dplyr::mutate(type = ifelse(
                test = type_1 %in% "Decision",
                yes = paste(type_1, type_2, sep = "_"),
                no = paste(type_2, type_1, sep = "_") ) ) |>
            dplyr::select(-starts_with("type_")) |>
            dplyr::right_join(
                y = votes_day,
                by = "activity_id") }

    # Track cols to check or fix
    cols_tofix <- names(votes_raw)[ !names(votes_raw) %in% names(votes_day) ]


    #--------------------------------------------------------------------------#
    #### Tackle df-cols --------------------------------------------------------
    # Vector of languages to drop
    cols_languages_short <- c("_el", "_bg", "_de", "_hu", "_cs", "_ro", "_et", "_nl",
                              "_fi", "_mt", "_sl", "_sv", "_pt", "_it", "_lt", "_es",
                              "_da", "_pl", "_sk", "_lv", "_hr", "_ga")
    # Which cols are actually DFs?
    cols_dataframe <- names(votes_raw)[
        sapply(votes_raw, class) %in% c("data.frame")]
    # Process cols-DF one at a time
    list_tmp <- lapply(
        X = setNames(object = cols_dataframe, nm = cols_dataframe),
        FUN = function(j_col) {
            votes_raw |>
                dplyr::select(activity_id, tidyselect::any_of(j_col)) |>
                dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
                tidyr::unnest(tidyselect::any_of(j_col),
                              keep_empty = TRUE, names_sep = "_") |>
                dplyr::select( -ends_with( cols_languages_short) )
        } )

    # Merge all DF in list ----------------------------------------------------#
    # https://stackoverflow.com/questions/2209258/merge-several-data-frames-into-one-data-frame-with-a-loop
    df_tmp <- Reduce(f = function(x, y) {
        merge(x, y, all = TRUE, by = c("activity_id"))},
        x = list_tmp, accumulate=F)
    # merge back with original flat data
    votes_day <- merge(votes_day, df_tmp, by = c("activity_id"), all = TRUE) |>
        data.table::as.data.table()

    # Track cols to check or fix
    cols_tofix <- cols_tofix[ !cols_tofix %in% cols_dataframe ]


    #--------------------------------------------------------------------------#
    #### Tackle list-cols ------------------------------------------------------
    votes_raw = data.table::as.data.table(votes_raw)

    # inverse_consists_of -----------------------------------------------------#
    if ("inverse_consists_of" %in% names(votes_raw) ) {
        inverse_consists_of = votes_raw[, list(
            inverse_consists_of = as.character( unlist(inverse_consists_of) )
        ),
        by = list(activity_id)
        ]

        # Merge unnested DT into votes_day
        votes_day = inverse_consists_of[
            votes_day, on = "activity_id"
        ]
        # Remove list-col in TMP dt
        votes_raw[, inverse_consists_of := NULL]
        rm(inverse_consists_of) # remove object
        cols_tofix = cols_tofix[
            !cols_tofix %in% "inverse_consists_of"
        ]
    }


    # recorded_in_a_realization_of --------------------------------------------#
    if ("recorded_in_a_realization_of" %in% names(votes_raw) ) {
        recorded_in_a_realization_of = votes_raw[, list(
            recorded_in_a_realization_of = as.character(
                unlist(recorded_in_a_realization_of) )
        ),
        by = list(activity_id)
        ]

        # Merge unnested DT into votes_day
        votes_day = recorded_in_a_realization_of[
            votes_day, on = "activity_id"
        ]
        # Remove list-col in TMP dt
        votes_raw[, recorded_in_a_realization_of := NULL]
        rm(recorded_in_a_realization_of) # remove object

        cols_tofix = cols_tofix[
            !cols_tofix %in% "recorded_in_a_realization_of"
        ]
    }

    cols_list <- names(votes_raw)[
        sapply(votes_raw, class) %in% c("list")]
    cols_list <- cols_list[
        !cols_list %in% c(
            "had_voter_abstention", "had_voter_against", "had_voter_favor",
            "had_voter_intended_abstention", "had_voter_intended_against",
            "had_voter_intended_favor", "type", "decided_on_a_realization_of",
            "was_motivated_by") ]

    if ( length(cols_list > 0)) {
        print(cols_list)
        list_tmp <- lapply(
            X = setNames(object = cols_list, nm = cols_list),
            FUN = function(j_col) {
                votes_raw |>
                    dplyr::select(activity_id, tidyselect::any_of(j_col)) |>
                    dplyr::distinct() |> # DEFENSIVE: there may be duplicate rows
                    tidyr::unnest(tidyselect::any_of(j_col), keep_empty = TRUE) } )

        # Merge all DF in list ------------------------------------------------#
        # https://stackoverflow.com/questions/2209258/merge-several-data-frames-into-one-data-frame-with-a-loop
        df_tmp <- Reduce(f = function(x, y) {
            merge(x, y, all = TRUE, by = c("activity_id"))},
            x = list_tmp, accumulate=F)
        # merge back with original flat data
        votes_day <- merge(votes_day, df_tmp, by = c("activity_id"), all = TRUE) |>
            data.table::as.data.table() }


    #--------------------------------------------------------------------------#
    ## Final Cleaning ----------------------------------------------------------

    if ( length(decision_outcome_cols) == 1L ) {
        names(votes_day)[
            grepl(pattern = "decision_outcome", x = names(votes_day))
        ] <- "decision_outcome"
        votes_day[, decision_outcome := gsub(pattern = "def/ep-statuses/", replacement = "",
                                             x = decision_outcome, fixed = TRUE) ]
    } else {
        warning("Inconsistent cols naming: check 'decision_outcome'")
    }

    # Track cols to check or fix
    cols_tofix <- cols_tofix[
        !cols_tofix %in% c(cols_list, "had_voter_abstention", "had_voter_against",
                           "had_voter_favor", "had_voter_intended_abstention",
                           "had_voter_intended_against", "had_voter_intended_favor",
                           "type", "decided_on_a_realization_of",
                           "was_motivated_by") ]
    if ( length(cols_tofix) > 0L) {
        warning(
            paste0("You may have additional cols to process. Check again! ",
                   unique(votes_raw$activity_date) ) )
    }

    # get rid of useless cols
    cols_todelete <- names(votes_day)[
        names(votes_day) %in% c("id", "decisionAboutId_XMLLiteral")]
    votes_day[, c(cols_todelete) := NULL]
    # clean cols
    votes_day[, `:=`(
        activity_date = as.Date(activity_date),
        activity_start_date = lubridate::as_datetime(activity_start_date),
        decision_method = gsub(
            pattern = "http://publications.europa.eu/resource/authority/decision-method/|def/ep-decision-methods/",
            replacement = "", x = decision_method),
        had_activity_type = gsub(
            pattern = "http://publications.europa.eu/resource/authority/event/|def/ep-activities/",
            replacement = "", x = had_activity_type) ) ]
    if ( "had_decision_outcome" %in% names(votes_day) ) {
        votes_day[, had_decision_outcome := gsub(
            pattern = "http://publications.europa.eu/resource/authority/decision-outcome/",
            replacement = "", x = had_decision_outcome) ] }

    # return data.frame
    return(votes_day)
}


###--------------------------------------------------------------------------###
# test ------------------------------------------------------------------------#
# p1=process_vote_day()
# p = lapply(X = vote_list_tmp, FUN = function(x) process_vote_day(x))
