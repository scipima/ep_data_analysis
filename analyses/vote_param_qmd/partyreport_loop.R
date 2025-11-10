###--------------------------------------------------------------------------###
# Report loop ------------------------------------------------------------------
###--------------------------------------------------------------------------###

## Libraries -------------------------------------------------------------------
library(dplyr)
library(data.table)
library(here)
library(quarto)

# Hard-code parameters --------------------------------------------------------#
renew_polgroup_id <- c(7035L)
dir.create(path = here::here("analyses", "vote_param_qmd", "out_html"),
           showWarnings = FALSE)

### ---------------------------------------------------------------------------#
## get data on parties and countries -------------------------------------------
country_renewparty <- data.table::fread(here::here(
    "data_reference", "qmd_grid.csv")) |>
    dplyr::filter(polgroup_id == renew_polgroup_id) |>
    dplyr::arrange(country_name_en, national_party_long)


# loop to knit pdf ------------------------------------------------------------#
for ( i_row in seq_len( nrow(country_renewparty) )[c(3, 14, 15, 26)] ) { #
    quarto::quarto_render(
        input = here::here("analyses", "vote_param_qmd", "country_test.qmd"),
        output_format = "html",
        execute_params = list(
            # Country ---------------------------------------------------------#
            country_id = country_renewparty$country_id[i_row],
            country_iso3c = country_renewparty$country_iso3c[i_row],
            country_name_en = country_renewparty$country_name_en[i_row],
            # Party -----------------------------------------------------------#
            natparty_id = country_renewparty$natparty_id[i_row],
            national_party = country_renewparty$national_party[i_row],
            national_party_long = country_renewparty$national_party_long[i_row] )
    )

    # Move file from main branch to sub-folder --------------------------------#
    file.rename(
        from = here::here("analyses", "vote_param_qmd", "country_test.html"),
        to = here::here("analyses", "vote_param_qmd", "out_html",
                        paste0(country_renewparty$country_iso3c[i_row],
                               "_",
                               country_renewparty$national_party[i_row],
                               ".html")
        ) )
}

