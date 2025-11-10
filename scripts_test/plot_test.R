library(ggplot2)
library(dplyr)
library(data.table)

#------------------------------------------------------------------------------#
source(file = here::here("scripts_r", "join_functions.R"))

# vote colours --------------------------------------------------------------###
vote_colours <- c(For = '#00AEEF',
                  Against = '#BE3455',
                  Abstain = "#969696",
                  `No vote` = '#5D5CA4',
                  Absent = '#F47920')


## Data ------------------------------------------------------------------------
meps_rcv_mandate <- data.table::fread(here::here(
  "data_out", "meps_rcv_mandate_10.csv") )
votes_labels <- data.table::fread(here::here(
  "data_out", "votes", "votes_labels_10.csv") )


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
