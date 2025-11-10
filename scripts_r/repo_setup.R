###--------------------------------------------------------------------------###
# GitHub Repository Setting Up -------------------------------------------------
###--------------------------------------------------------------------------###

##----------------------------------------------------------------------------##
## Libraries -------------------------------------------------------------------
if ( !require("pacman") ) install.packages("pacman")
pacman::p_load(char = c("here") )

### Data in --------------------------------------------------------------------
dir.create(path = here::here("data_in")) # Create new folder
cat("This folder stores all the unaltered incoming data.",
    file = here::here("data_in", "README.txt") )

dir.create(path = here::here("data_in", "plenary_docs_api_json") ) # Create new folder
cat("This folder stores all the unaltered incoming data regarding Plenary Documents.",
    file = here::here("data_in", "plenary_docs_api_json", "README.txt") )

dir.create(path = here::here("data_in", "meeting_decision_json") ) # Create new folder
cat("This folder stores all the unaltered incoming .json files for Meeting Decisions from Plenary.",
    file = here::here("data_in", "meeting_decision_json", "README.txt") )


### Data out -------------------------------------------------------------------
dir.create(path = here::here("data_out") ) # Create new folder
cat("This folder stores all the processed output data.",
    file=here::here("data_out", "README.txt") )
dir.create(path = here::here("data_out", "aggregates") ) # Create new folder
dir.create(path = here::here("data_out", "aggregates", "daily") ) # Create new folder
dir.create(path = here::here("data_out", "aggregates", "daily", "csv") ) # Create new folder
dir.create(path = here::here("data_out", "aggregates", "daily", "xlsx") ) # Create new folder
dir.create(path = here::here("data_out", "attendance") ) # Create new folder
dir.create(path = here::here("data_out", "bodies") ) # Create new folder
dir.create(path = here::here("data_out", "daily") ) # Create new folder
dir.create(path = here::here("data_out", "docs_cmt") ) # Create new folder
dir.create(path = here::here("data_out", "docs_pl") ) # Create new folder
dir.create(path = here::here("data_out", "meetings") ) # Create new folder
dir.create(path = here::here("data_out", "meps") ) # Create new folder
dir.create(path = here::here("data_out", "tmp", "natparty_tmp") ) # Create new folder
dir.create(path = here::here("data_out", "procedures") ) # Create new folder
dir.create(path = here::here("data_out", "rcv") ) # Create new folder
dir.create(path = here::here("data_out", "votes") ) # Create new folder
dir.create(path = here::here("data_out", "vl_vote") ) # Create new folder


## Scripts ---------------------------------------------------------------------
dir.create(path = here::here("scripts_r") ) # Create new folder


## Figures ---------------------------------------------------------------------
dir.create(path = here::here("figures") ) # Create new folder


## Reports .pdf ----------------------------------------------------------------
dir.create(path = here::here("analyses", "vote_param_qmd", "out_pdf") ) # Create new folder


##----------------------------------------------------------------------------##
## Functions -------------------------------------------------------------------

##' This function checks if the file exists and is recent enough.
##' If too old, it sources the script to regenerate it, then reads and returns the data.

## Top-level readers (explicit, easy to test)
read_csv_path <- function(path) {
  data.table::fread(path)
}

read_rds_path <- function(path) {
  readRDS(path)
}

read_feather_path <- function(path) {
  if (!requireNamespace("arrow", quietly = TRUE)) stop("arrow package required to read feather files")
  arrow::read_feather(path)
}

read_text_path <- function(path) {
  readLines(path, warn = FALSE)
}

##' This function checks if the file exists and is recent enough.
##' If too old, it sources the script to regenerate it, then reads and returns the data.
get_api_data <- function(
    path = here::here("data_out", "meetings", "pl_meetings_10.csv"),
    script = here::here("scripts_r", "api_meetings.R"),
    max_days = 1L,
    varname = NULL,
    envir = .GlobalEnv,
    verbose = TRUE,
    file_type = c("auto", "csv", "rds", "feather", "text"),
    reader = NULL
) {
  file_type <- match.arg(file_type)

  # choose reader
  chosen_reader <- NULL
  if (!is.null(reader) && is.function(reader)) {
    chosen_reader <- reader
  } else if (file_type == "csv") {
    chosen_reader <- read_csv_path
  } else if (file_type == "rds") {
    chosen_reader <- read_rds_path
  } else if (file_type == "feather") {
    chosen_reader <- read_feather_path
  } else if (file_type == "text") {
    chosen_reader <- read_text_path
  } else { # auto
    ext <- tolower(tools::file_ext(path))
    if (ext %in% c("csv", "txt")) chosen_reader <- read_csv_path
    else if (ext %in% c("rds", "rda")) chosen_reader <- read_rds_path
    else if (ext %in% c("feather")) chosen_reader <- read_feather_path
    else chosen_reader <- read_text_path
  }

  # Check and source if needed
  if (file.exists(path)) {
    mtime <- as.Date(file.info(path)[["mtime"]])
    if ((Sys.Date() - mtime) < max_days) {
      if (verbose) cat("Loading existing file:", path, "\n")
      data_api_tmp <- chosen_reader(path)
    } else {
      if (verbose) {
        cat("File is older than", 
            max_days,
            "days. Regenerating via:",
            script, 
            "\n")
      }
      source(file = script, echo = verbose, local = TRUE)
      data_api_tmp <- chosen_reader(path)
    }
  } else {
    if (verbose) {
      cat("File not found. Creating via:", script, "\n")
    }
    source(file = script, echo = verbose, local = TRUE)
    data_api_tmp <- chosen_reader(path)
  }

  # Assign to varname if provided (backward compatibility)
  if (!is.null(varname) && nzchar(varname)) {
    assign(varname, data_api_tmp, envir = envir)
  }

  invisible(data_api_tmp)
}

