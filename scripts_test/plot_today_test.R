install.packages("pak")
library(tidyverse)
library(data.table)

#------------------------------------------------------------------------------#
# Hard code the start of the mandate ------------------------------------------#
mandate_starts <- "2024-07-14"

# Hard code today's date ------------------------------------------------------#
if ( !exists("today_date") ) {
  today_date <- gsub(pattern = "-", replacement = "",
                     x = as.character( Sys.Date() ) )
  # today_date <- "20250313" # test
}


#------------------------------------------------------------------------------#
source(file = here::here("scripts_r", "join_functions.R"))

# vote colours ----------------------------------------------------------------#
vote_colours <- c(`for` = '#00AEEF',
                  against = '#BE3455',
                  abstain = "#969696",
                  no_vote = '#5D5CA4',
                  absent = '#F47920')

# Data
### Write data to disk ---------------------------------------------------------
meps_rcv_today = data.table::fread(file = here::here(
  "data_out", "daily", paste0("rcv_today_", today_date, ".csv") ) )
# convert to factor - essential for tabulate
meps_rcv_today[, result_fct := factor(result,
                                        levels = -3:1,
                                        labels = c("absent", "no_vote", "against",
                                                   "abstain", "for") ) ]



rcvids_selected = c(174971)

dt_plot = meps_rcv_today[
  rcv_id %in% rcvids_selected,
  list(tally = .N),
  by = list(polgroup_id, rcv_id, result_fct)
] |>
  left_join(
    y = votes_dt[, list(rcv_id,inverse_consists_of)],
    by = "rcv_id"
  ) |>
  mutate(inverse_consists_of = gsub("eli/dl/event/", "", inverse_consists_of)) |>
  left_join(
    y = votes_labels[,list(activity_id, vote_label_mul)],
    by = c("inverse_consists_of" = "activity_id") ) |>
  join_polit_labs()
dt_plot |>
  ggplot(aes(x = result_fct, y = tally, fill = result_fct)) +
  geom_col(position = "dodge") +
  geom_text(aes(label = tally), position = position_dodge(0.9), vjust=-.2) +
  facet_grid(cols = vars(political_group), scales="free_x", space="free_x") +
    # facet_wrap(~vote_label_mul, ncol=1,
    #          labeller = labeller(vote_label_mul = label_wrap_gen(130))) +
  scale_fill_manual(values = vote_colours) +
  labs(fill="Vote", x="",
       title = str_wrap("Modification des directives (UE) 2022/2464 et (UE) 2024/1760 concernant les dates à partir desquelles les États membres doivent appliquer certaines obligations relatives à la publication d’informations en matière de durabilité par les entreprises et au devoir de vigilance des entreprises en matière de durabilité",
                        width = 95) ) +
  theme_bw() +
  theme(
    plot.title.position = "plot",
    axis.text.x = element_blank(),
    axis.ticks.x = element_blank(),
    legend.position = "top"
  )
ggsave(here::here("figures", "20250403_20250044COD_final.pdf"), device = "pdf",
       dpi=300, height = 6, width = 8)

meps_rcv_today[
  rcv_id %in% rcvids_selected] |>
  join_polit_labs() |>
  join_meps_names() |>
  join_meps_countries() |>
  write_csv(here::here("20250403_20250044COD_final.csv"))
