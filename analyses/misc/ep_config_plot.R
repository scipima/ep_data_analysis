
###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
library(tidyverse)
library(data.table)
# library(ggparliament)
devtools::install_github("zmeers/ggparliament")


source(file = here::here("scripts_r", "join_functions.R"))


###--------------------------------------------------------------------------###
## Colours ---------------------------------------------------------------------
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


# Data
meps_current <- data.table::fread(here::here(
    "data_out", "meps/meps_current.csv") ) |>
    join_polit_labs()



polgroups_size = meps_current |>
    summarise(sum_seats = n(), .by = political_group)

polgroups_size |>
    mutate(political_group = fct_reorder(.f = political_group, .x = sum_seats,
                                         .desc = TRUE)) |>
    ggplot(aes(x= political_group, y = sum_seats, fill = political_group)) +
    geom_col(show.legend = FALSE) +
    geom_text(aes(label = sum_seats), size = 6, nudge_y=6) +
    scale_fill_manual(values = polgroup_cols) +
    labs(title = "EP Configuration as of September 2025",
         y = "Seats", x = "") +
    theme_minimal() +
    theme(
        plot.title.position = "plot",
        plot.title = element_text(face = "bold", size = 20),
        axis.title = element_text(size = 16, face = "bold"),
        axis.text = element_text(size = 16)
    )
ggsave(filename = "figures/ep_config_2025_barplot.jpeg",
       device = "jpeg", dpi = 300, height = 9, width = 10.5)

###--------------------------------------------------------------------------###
polgroup_order = c("The Left", "S&D", "Verts/ALE", "Renew", "PPE", "ECR", "PfE",
                   "ESN", "NI")
house_semicircle <- polgroups_size |>
    dplyr::mutate(
        political_group = factor(political_group,
                                 levels = polgroup_order)) |>
    dplyr::arrange(political_group)
house_semicircle = house_semicircle |>
    ggparliament::parliament_data(
        type = "semicircle",
        parl_rows = 14,
        party_seats = house_semicircle$sum_seats)

house_semicircle  |>
    ggplot(aes(x = x, y = y, colour = political_group)) +
    ggparliament::geom_parliament_seats() +
    annotate(geom = "text",
             x = c(-2.2, -2, -1.4, -0.9, 1, 1.5, 2, 2.1, 2.15),
             y = c(2.805086e-01, 0.9, 1.7, 1.9, 1.9, 1.5, 0.9, 0.4, 0.05),
             label = paste(polgroup_order,
                           unique(house_semicircle$sum_seats),
                            sep = "\n"),
             size =6) +
    ggparliament::theme_ggparliament() +
    labs(title = "EP Configuration as of September 2025") +
    scale_colour_manual(values = polgroup_cols) +
    theme(
        legend.position = "none",
        plot.title.position = "plot",
        plot.title = element_text(face = "bold", size = 20),
    )
ggsave(filename = "figures/ep_config_2025_semicircle.jpeg",
       device = "jpeg", dpi = 300, height = 9, width = 10.5)
