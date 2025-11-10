
###--------------------------------------------------------------------------###
## Libraries -------------------------------------------------------------------
library(tidyverse)
library(data.table)
library(XML)
library(xml2)
library(rvest)
library(lubridate)
library(here)
library(httr)
library(future.apply)
library(googledrive)

xml_files <- list.files(path = here::here("data_in", "procedures_open"))


# setting up for loop
xml_rawlist <- vector(mode = "list", length = length(xml_files))

# for loop ------------------------------------------------------------------###
for ( i_file in seq_along(xml_files) ) {

    print(xml_files[i_file])

    # get pointers ------------------------------------------------------------###
    # puts this in a loop until condition is satisfied, as sometimes reading webpage fails
    while( !base::exists("xml_tmp") ) {
        base::try( xml_tmp <- xml2::read_xml(
            here::here("data_in", "procedures_open",xml_files[i_file])
        ) ) }


    # get identifiers of votes
    Vote.Result_number <- xml2::xml_find_all(x = xml_tmp,
                                             xpath = ".//Vote.Result") |>
        xml2::xml_attr(attr = "Number")
    Vote.Result.Text.Title <- xml2::xml_find_all(x = xml_tmp,
                                                 xpath = ".//Vote.Result.Text.Title") |>
        xml2::xml_text()
    Vote.Result <- xml2::xml_find_all(x = xml_tmp,
                                      xpath = ".//Vote.Result")
    Vote.Result.Description.Text <- xml2::xml_find_first(x = Vote.Result,
                                                         xpath = ".//Vote.Result.Description.Text") |>
        xml2::xml_text()

    # Vote.Result.Description.Text_ref <- xml2::xml_find_all(x = xml_tmp, xpath = ".//Vote.Result.Description.Text") |>
    #   xml2::xml_children() |>
    #   xml2::xml_text()

    # create a df with all the info
    vote_result_info <- dplyr::bind_cols(
        Vote.Result_number = Vote.Result_number,
        Vote.Result.Text.Title = Vote.Result.Text.Title,
        # Vote.Result.Description.Text_ref = Vote.Result.Description.Text_ref,
        Vote.Result.Description.Text = Vote.Result.Description.Text) |>
        dplyr::mutate(row_id = dplyr::row_number())

    Vote.Result.Table.Results <- xml2::xml_find_all(x = xml_tmp,
                                                    xpath = ".//Vote.Result.Table.Results")
    TBODY_length <- xml_find_first(x = Vote.Result.Table.Results, xpath = ".//TBODY") |>
        xml2::xml_length()
    table_rows <- TBODY_length - 1 # to get rid of the 1st row which become header

    # We need to expand this dataset to then append it with the results by MEP.
    df_tmp <- data.frame(row_id = rep(1:nrow(vote_result_info), table_rows))
    # merge to duplicate rows
    vote_result_info <- merge(vote_result_info, df_tmp) |>
        dplyr::select(-row_id)

    ### grab table of vote -------------------------------------------------------
    # unfortunately, the resulting table is not well formatted, but we have to live with it
    # REF: https://rdrr.io/cran/XML/man/readHTMLTable.html
    doc <- htmlParse(xml_tmp)
    tableNodes <- getNodeSet(doc, "//table")
    raw_xmltables <- lapply(X = tableNodes, FUN = function(xml_table) {
        readHTMLTable(xml_table) } ) # , header = T # this fails for the last table, not clear why

    # The 'readHTMLTable' function picks up a lot of things which are not tables, so we subset it here
    # the number of cols is never less than 3 if it's a table, so we filter some stuff here ...
    is_votetable <- sapply(X = raw_xmltables, function(x) { dim(x)[2] > 2} )
    raw_xmltables <- raw_xmltables[is_votetable]
    # because the header=T fails at times, we convert all first rows into colnames here
    xml_tables = lapply(X = raw_xmltables,
                        FUN = function(x) {
                            x <- x |>
                                janitor::row_to_names(row_number = 1, remove_row = T) |>
                                janitor::clean_names() } )

    # v <- unique( sapply(X = xml_tables, FUN = names, simplify = T) )

    # bind all together
    xml_alltables <- data.table::rbindlist(l = xml_tables, use.names = T, fill = T)

    # remove tmp xml to perform `try` at the beginning of the loop
    rm(xml_tmp)

    # store results
    xml_rawlist[[counter]] <- dplyr::bind_cols(vote_result_info, xml_alltables)
}
