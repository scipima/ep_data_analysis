
#------------------------------------------------------------------------------#
## Libraries -------------------------------------------------------------------
library(tidyverse)
library(shiny)
library(rmarkdown)


#------------------------------------------------------------------------------#
## Functions -------------------------------------------------------------------
# Load join functions ---------------------------------------------------------#
source(file = here::here("scripts_r", "join_functions.R") )

# Calculate Majorities --------------------------------------------------------#
source(file = here::here("scripts_r", "get_majority.R") )



#------------------------------------------------------------------------------#
## Data ------------------------------------------------------------------------
meps_current <- read_csv(here::here("data_out", "meps", "meps_current.csv")) |> 
  join_meps_names()

plenary_docs <- read_csv(here::here("data_out", "docs_pl", "plenary_docs.csv") )



#------------------------------------------------------------------------------#
## UI --------------------------------------------------------------------------
ui <- fluidPage( 

  ### Select Title -------------------------------------------------------------
  selectizeInput(
    inputId = "selected_doctitle",
    label = "Select Document Title",
    choices = unique(plenary_docs$title_dcterms_en),
    multiple = TRUE, width = "100%"),
  
  ### Select Rapporteurs -------------------------------------------------------
  selectizeInput(
    inputId = "selected_rapporteur",
    label = "Select Rapporteur",
    choices = unique(meps_current$mep_name),
    multiple = TRUE, width = "100%"),
  
  ### Select DOC ID ------------------------------------------------------------
  selectizeInput(
    inputId = "selected_docid",
    label = "Select Doc ID",
    choices = unique(plenary_docs$label),
    multiple = TRUE, width = "100%"),
  
  
  ### Select DOC TYPE ------------------------------------------------------------
  selectizeInput(
    inputId = "selected_doctype",
    label = "Select Doc ID",
    choices = unique(plenary_docs$work_type),
    multiple = TRUE, width = "100%"),
  
  
  # Download option -----------------------------------------------------------#
  downloadButton("report", "Generate report")
)


#------------------------------------------------------------------------------#
## Server ----------------------------------------------------------------------
server <- function(input, output, session){
  output$report <- downloadHandler(
    # For PDF output, change this to "report.pdf"
    filename = "report.docx",
    content = function(file) {
      # Copy the report file to a temporary directory before processing it, in
      # case we don't have write permissions to the current working dir (which
      # can happen when deployed).
      tempReport <- file.path(tempdir(), "report.Rmd")
      file.copy("report.Rmd", tempReport, overwrite = TRUE)
      
      # Set up parameters to pass to Rmd document
      params <- list(
        selected_doctitle = input$selected_doctitle,
        selected_rapporteur = input$selected_rapporteur,
        selected_docid = input$selected_docid,
        selected_doctype = input$selected_doctype
        )
      
      # Knit the document, passing in the `params` list, and eval it in a
      # child of the global environment (this isolates the code in the document
      # from the code in this app).
      rmarkdown::render(tempReport,
                        output_file = file,
                        params = params,
                        envir = new.env(parent = globalenv())
      )
    }
  )
}


#------------------------------------------------------------------------------#
## Lunch the app ---------------------------------------------------------------
shinyApp(ui = ui, server = server)
