###--------------------------------------------------------------------------###
# Cohesion function ------------------------------------------------------------
###--------------------------------------------------------------------------###

###--------------------------------------------------------------------------###
#' The formula for cohesion is taken from [Is there a selection bias in roll call votes?](http://link.springer.com/10.1007/s11127-018-0529-1).
###--------------------------------------------------------------------------###


###--------------------------------------------------------------------------###
# Data  ------------------------------------------------------------------------
# meps_rcv_mandate <- data.table::fread(file = here::here(
#     "data_out", "meps_rcv_mandate_10.csv"),
#     verbose = TRUE, encoding = "UTF-8", na.strings = c(NA_character_, ""))
# str(meps_rcv_mandate)

# to test: convert result to factor (`tabulate` only works with factor or positive integers)
# if ( !"result_fct" %in% names(meps_rcv_mandate) ) {
#     meps_rcv_mandate[, result_fct := factor(result, levels = c(1L, -1L, 0L),
#                                           labels = c("For", "Against", "Abstain") ) ] }


###--------------------------------------------------------------------------###
# Function ---------------------------------------------------------------------
# formula in Hix and Noury
cohesion_hn <- function(votes_tally = meps_rcv_mandate$result) {
    # Checks ------------------------------------------------------------------#
    if ( is.integer(votes_tally) & any(votes_tally <= 0L) ) {
        warning("Vector is an integer with negative or 0 values. Forcing conversion to factor.")
        votes_tally <- as.factor(votes_tally) }
    if ( !is.factor(votes_tally) ) {
        warning("Vector is not a factor. Forcing conversion")
        votes_tally <- as.factor(votes_tally) }

    # Computation -------------------------------------------------------------#
    votes_tab <- tabulate(votes_tally)
    votes_sum <- sum(votes_tab, na.rm = TRUE)
    votes_max <- max(votes_tab, na.rm = TRUE)

    # Conditional return ------------------------------------------------------#
    #' For formula to make sense, sum of votes by group must be bigger than 1.
    #' If sum=1, mathematically cohesion rate is always 1.
    #' Thus, we return NA if sum=1
    if (votes_sum > 1L) {
        return(
            ( (votes_max - 0.5*(votes_sum - votes_max)) / (votes_sum) * 100 )
        )
    } else if (votes_sum == 1L) {
        # if not equal to 1, then issue warning and NA
        warning("One of the sums of the total votes by group is 1, It does not make sense to calculate cohesion rate.")
        return( NA_real_ ) }
}



###--------------------------------------------------------------------------###
# Test -------------------------------------------------------------------------
# test
# meps_rcv_mandate[, list(
#     cohesion = cohesion_hn(result),
#     max_mode = max(tabulate(result)),
#     tot_votes = sum(tabulate(result) ) ),
#     by = political_group][order(-cohesion)]

# meps2 = data.frame(
#     meps = letters[1:4],
#     result = factor(c("For", "For", "For", "Abstain"))
# )
# cohesion_hn(meps2$result)

# dataset to illustrate cases
# data.frame(
#     n_meps = c(2, 2, 3, 3, 4, 4),
#     For = c(2, 1, 2, 1, 2, 3),
#     Against = c("-", 1, 1, 1, 1, 1),
#     Abstain = c("-", "-", "-", 1, 1, "-"),
#     cohesion = c(100, 25, 50, 0, 25, 62.5) )


###--------------------------------------------------------------------------###
# where this could fail? does this function make sense when EP Group has very few members taking part in RCV?
# identify RCV with very low participation by group
# meps_rcv_mandate[, .N,
#                by = list(sitting_date, rcv_id, political_group)][order(N)]
#
# cohesion_hn_test <- function(votes_tally = meps_rcv_mandate$result[
#     meps_rcv_mandate$sitting_date == as.Date("2022-12-15") & meps_rcv_mandate$rcv_id == "151459"]) {
#     # sum of votes by group must be bigger than 1. if sum=1, cohesion rate is always 1
#     if (sum(tabulate(votes_tally)) > 1) {
#         ((max(tabulate(votes_tally)) - 0.5*(sum(tabulate(votes_tally)) - max(tabulate(votes_tally))))
#          / sum(tabulate(votes_tally))) * 100
#     } else if (sum(tabulate(votes_tally)) == 1) {
#         # if not equal to 1, then issue warning and NA
#         message("one of the sums of the total votes by group is 1, does not make sense to calculate cohesion rate")
#         return(cohesion_rate = NA_real_) } }

# test
# meps_rcv_mandate[
#     sitting_date == as.Date("2022-12-15") & rcv_id == "151459",
#     list(
#         cohesion = cohesion_hn(result),
#         max_mode = max(tabulate(result)),
#         tot_votes = sum(tabulate(result) ) ),
#     by = political_group][order(-cohesion)]

# cohesion_hn_test(votes_tally = meps_rcv_mandate$result[
#     meps_rcv_mandate$sitting_date == as.Date("2022-12-15")
#     & meps_rcv_mandate$rcv_id == "151459"
#     & meps_rcv_mandate$political_group == "EPP"])

# df <- data.frame(X1 = as.factor(sample(1:5, 1000000, replace = TRUE)))
# microbenchmark::microbenchmark(base_table = {base::table(df$X1)},
#                                collapse_fnobs = {collapse::fnobs(df, df$X1)},
#                                base_tabulate = {tabulate(df$X1) })

# test
# cohesion_byepgroup_byitem <- rcv_allsessions[, list(
#     cohesion = cohesion_hn(result),
#     max_mode = max(tabulate(result)),
#     tot_votes = sum(tabulate(result)) ),
#     by = list(political_group, rcv_id)][
#         order(-cohesion)]
# cohesion_byepgroup_byitem |>
#     ggplot(aes(x = political_group, y = cohesion, fill = political_group)) +
#     geom_boxplot(outlier.alpha = 0.1, outlier.size = 0.1, linewidth=0.3, show.legend = F) +
#     labs(x = "", y = "Cohesion rate",
#          title = "Distribution of cohesion rates") +
#     scale_fill_manual(values = polgroups_colours_vct) +
#     theme_minimal()

# rcv_allsessions[, year := data.table::year(rcv_date)]
# cohesion_byepgroup_byyear <- rcv_allsessions[, list(
#     cohesion = cohesion_hn(result),
#     max_mode = max(tabulate(result)),
#     tot_votes = sum(tabulate(result)) ),
#     by = list(political_group, year, rcv_id)][
#         order(-cohesion)]
# cohesion_byepgroup_byyear |>
#     ggplot(aes(x = factor(year), y = cohesion, fill = political_group)) +
#     geom_boxplot(outlier.alpha = 0.1, outlier.size = 0.1, linewidth=0.3, show.legend = F) +
#     facet_wrap(~political_group, nrow = 1) +
#     labs(x = "Years", y = "Cohesion rate",
#          title = "Distribution of cohesion rates") +
#     scale_fill_manual(values = polgroups_colours_vct) +
#     theme_minimal() +
#     theme(axis.text.x = element_text(angle = 90))
