library(ggplot2)
library(dplyr)
library(data.table)

#------------------------------------------------------------------------------#
source(file = here::here("scripts_r", "join_functions.R"))
source(file = here::here("scripts_r", "cohesionrate_function.R"))

# vote colours --------------------------------------------------------------###
vote_colours <- c(For = '#00AEEF',
                  Against = '#BE3455',
                  Abstain = "#969696",
                  `No vote` = '#5D5CA4',
                  Absent = '#F47920')


## Data ------------------------------------------------------------------------
meps_rcv_mandate <- data.table::fread(here::here(
  "data_out", "meps_rcv_mandate_10.csv") )
pl_votes <- fread(here::here("data_out", "votes", "pl_votes_10.csv"))
votes_labels <- data.table::fread(here::here(
  "data_out", "votes", "votes_labels_10.csv") )


target_rcvid = pl_votes |>
  filter(
    grepl("962687", inverse_consists_of)
    & decision_method == "VOTE_ELECTRONIC_ROLLCALL") |>
  pull(rcv_id)

meps_rcv_mandate[, result_fct := factor(result,
                                        levels = -3L : 1L,
                                        labels = c("absent", "no_vote", "against",
                                                   "abstain", "for") ) ]
# Check
# table(meps_rcv_mandate$result, meps_rcv_mandate$result_fct, exclude = NULL)

# Calculate Cohesion
cohesion_dt <- meps_rcv_mandate[
  rcv_id %in% target_rcvid
  & result >= -1L, # only official votes
  list(
    cohesion = round(cohesion_hn(result_fct), digits = 1)
    ),
  keyby = list(rcv_id, polgroup_id) ] |>
  join_polit_labs() |>
  select(rcv_id, cohesion, political_group, polgroup_id) |>
  left_join(
    y = fread(here::here("data_out", "aggregates", "tally_bygroup_byrcv_10.csv")) |>
      pivot_wider(id_cols = c(rcv_id, polgroup_id),
                  names_from = result_fct,
                  values_from = tally,
                  values_fill = 0L),
    by = c("rcv_id", "polgroup_id")
  ) |>
  select(-c(polgroup_id)) |>
  left_join(
    y = pl_votes |>
      select(inverse_consists_of, rcv_id, is_final, headingLabel_en,
             activity_label_en, referenceText_en,
             responsible_organization_label_en),
    by = "rcv_id"
  )
fwrite(x = cohesion_dt, file = here::here("data_out", "budget_cohesion.csv"))



# Extract RCV -----------------------------------------------------------------#
plot_dt = meps_rcv_mandate[
  rcv_id == 170611,
  list(Count = .N),
  keyby = list(polgroup_id, result)] |>
  join_polit_labs() |>
  dplyr::mutate(
    result_fct = factor(
      result, levels = -3:1,
      labels = c("Absent", "No vote", "Against", "Abstain", "For")
      ) )


#------------------------------------------------------------------------------#
# save to disk Excel and upload ------------------------------------------------
writexl::write_xlsx(
  x = list(RCV_long = plot_dt,
           RCV_wide = plot_dt |>
             tidyr::pivot_wider(id_cols = political_group,
                                names_from = result_fct,
                                values_from = Count, values_fill = 0)),
  path = here::here(
    "data_out", "tmp", "rcv_plot.xlsx" ),
  format_headers = TRUE)

plot_dt |>
  # dplyr::filter(political_group %in% c("ECR", "ESN", "PfE") ) |>
  ggplot(aes(x = forcats::fct_rev(political_group), y = Count)) +
  geom_col(aes(fill = result_fct), colour = "black", linewidth = 0.1) +
  labs(x="", y = "Count", fill="",
       title = stringr::str_wrap("Election of the Commission (2024-11-27)", width = 80) ) +
  coord_flip() +
  scale_fill_manual(values = vote_colours) +
  # scale_fill_brewer(type = "qual", palette = 1) +
  theme_minimal() +
  theme(plot.title.position = "plot",
        plot.title = element_text(face = "bold"),
        legend.text = element_text(face="bold"),
        axis.text.y = element_text(face = "bold"),
        legend.position = "top")
ggsave(filename = here::here("figures", "election_commission.jpeg"),
       device = "jpeg", dpi = 300, height = 9, width = 10.5)


plot_dt |>
  dplyr::mutate(Count_div = ifelse(result < 1, -1*Count, Count)) |>
  # dplyr::filter(political_group %in% c("ECR", "ESN", "PfE") ) |>
  ggplot(aes(x = forcats::fct_rev(political_group), y = Count_div)) +
  geom_col(aes(fill = result_fct), colour = "black", linewidth = 0.1) +
  labs(x="", y = "Count", fill="",
       title = stringr::str_wrap("Election of the Commission (2024-11-27)", width = 80) ) +
  coord_flip() +
  scale_fill_manual(values = vote_colours) +
  # scale_fill_brewer(type = "qual", palette = 1) +
  theme_minimal() +
  theme(plot.title.position = "plot",
        plot.title = element_text(face = "bold"),
        legend.text = element_text(face="bold"),
        axis.text.y = element_text(face = "bold"),
        legend.position = "top")
ggsave(filename = here::here("figures", "election_commission_div.jpeg"),
       device = "jpeg", dpi = 300, height = 9, width = 10.5)
