###--------------------------------------------------------------------------###
# Parallel API Calls Function -------------------------------------------------
###--------------------------------------------------------------------------###

#' Perform parallel API calls with progress tracking and error handling
#' 
#' This function executes multiple API calls in parallel using doFuture,
#' with built-in rate limiting, progress tracking, and robust error handling.
#' 
#' @param urls Character vector of URLs to call
#' @param user_agent Character string for User-Agent header (default: "renew_parlwork-prd-2.0.0")
#' @param capacity Integer, rate limit capacity (default: 490 calls per 5 minutes)
#' @param fill_time_s Integer, rate limit fill time in seconds (default: 300)
#' @param timeout_s Integer, request timeout in seconds (default: 300)
#' @param max_retries Integer, maximum number of retries (default: 5)
#' @param show_progress Logical, whether to show progress bar (default: TRUE)
#' @param extract_data Logical, whether to extract "data" field from JSON response (default: TRUE)
#' @param daily_run Logical, whether this is a daily run (affects error handling, default: !exists("today_date"))
#' @param force_sequential Logical, whether to force sequential processing even if multiple cores are available (default: FALSE)
#' 
#' @return List with two elements:
#'   - responses: List of successful API responses
#'   - failed_calls: Logical vector indicating which calls failed
#'   
#' @examples
#' urls <- paste0("https://api.example.com/data/", 1:10)
#' result <- parallel_api_calls(urls)
#' successful_responses <- result$responses[!result$failed_calls]
#' 
#' # Force sequential processing
#' result_seq <- parallel_api_calls(urls, force_sequential = TRUE)
#' 
parallel_api_calls <- function(
    urls,
    user_agent = "renew_parlwork-prd-2.0.0",
    capacity = 490,
    fill_time_s = 300,
    timeout_s = 300,
    max_retries = 5,
    show_progress = TRUE,
    extract_data = TRUE,
    daily_run = !exists("today_date", envir = .GlobalEnv),
    force_sequential = FALSE
) {
  
  # Load required packages
  if (!requireNamespace("doFuture", quietly = TRUE)) {
    stop("Package 'doFuture' is required but not installed.")
  }
  if (show_progress && !requireNamespace("progressr", quietly = TRUE)) {
    stop("Package 'progressr' is required for progress tracking but not installed.")
  }
  if (!requireNamespace("httr2", quietly = TRUE)) {
    stop("Package 'httr2' is required but not installed.")
  }
  
  library(doFuture)
  if (show_progress) library(progressr)
  
  # Validate inputs
  if (length(urls) == 0) {
    stop("No URLs provided")
  }
  
on.exit({
  plan(sequential)  # Force sequential to release all workers
  gc(verbose = FALSE)  # Clean up memory
}, add = TRUE)

  # Set up parallel processing
  available_cores <- parallel::detectCores()
  if (is.na(available_cores) || available_cores <= 1 || force_sequential) {
    max_workers <- 1
    if (force_sequential) {
      cat("Sequential processing forced for", length(urls), "API calls\n")
    } else {
      cat("Parallel processing not available or only 1 core detected. Running sequentially.\n")
    }
    plan(sequential)
  } else {
    max_workers <- min(available_cores - 1, length(urls))
    cat("Using", max_workers, "parallel workers for", length(urls), "API calls\n")
    plan(multisession, workers = max_workers)
  }
  
  # Initialize result containers
  resp_list <- vector(mode = "list", length = length(urls))
  failed_calls <- rep(x = FALSE, length(resp_list))
  
  cat("Starting parallel API calls...\n")
  start_time <- Sys.time()
  
  # Define the parallel processing function
  process_calls <- function() {
    foreach(
      i_url = seq_along(urls),
      .options.future = list(
        packages = c("httr2", if (show_progress) "progressr"),
        globals = structure(TRUE, add = c("urls", "user_agent", "capacity", 
                                        "fill_time_s", "timeout_s", "max_retries", 
                                        "extract_data", "daily_run"))
      )
    ) %dofuture% {
      
      # Create an API request
      req <- httr2::request(urls[i_url])
      
      # Add time-out and ignore error before performing request
      resp <- tryCatch({
        req |>
          httr2::req_headers("User-Agent" = user_agent) |>
          httr2::req_timeout(timeout_s) |>
          httr2::req_error(is_error = ~FALSE) |>
          httr2::req_throttle(capacity = capacity, fill_time_s = fill_time_s) |>
          httr2::req_retry(max_tries = max_retries,
                           backoff = ~ 2 ^ .x + runif(n = 1, min = -0.5, max = 0.5)) |>
          httr2::req_perform()
      }, error = function(e) {
        # Return a mock response object for timeouts/errors
        list(status_code = 408) # 408 = Request Timeout
      })
      
      # Initialize return values
      resp_body <- NULL
      failed <- FALSE
      
      # Handle response (including timeouts and errors)
      if (is.list(resp) && !is.null(resp$status_code) && resp$status_code == 408) {
        # This was a timeout or connection error
        if (!daily_run) {
          cat("\nWARNING: API request timed out for", urls[i_url], "\n")
        }
        # Always mark timeouts as failed, regardless of daily_run
        failed <- TRUE
      } else if (httr2::resp_status(resp) == 200L) {
        # Successful response
        if (extract_data) {
          resp_json <- httr2::resp_body_json(resp, simplifyDataFrame = TRUE)
          if (!is.null(resp_json[["data"]]) && length(resp_json[["data"]]) > 0) {
            resp_body <- resp_json[["data"]]
          } else {
            # API returned 200 but with empty/null data
            cat("\nNOTE: API returned empty data for", urls[i_url], "\n")
            resp_body <- NULL
          }
        } else {
          resp_body <- httr2::resp_body_json(resp, simplifyDataFrame = TRUE)
        }
      } else if (httr2::resp_status(resp) != 200L) {
        if (!daily_run) {
          cat("\nWARNING: API request failed for", urls[i_url], "- Status:", httr2::resp_status(resp), "\n")
        }
        # Always mark non-200 responses as failed, regardless of daily_run
        failed <- TRUE
      }
      
      # Update progress if enabled
      if (show_progress && exists("p")) {
        p(sprintf("Completed API call %d/%d", i_url, length(urls)))
      }
      
      # Return both the response and failure status with index
      list(response = resp_body, failed = failed, index = i_url)
    }
  }
  
  # Execute with or without progress tracking
  if (show_progress) {
    results <- with_progress({
      p <<- progressor(steps = length(urls))
      process_calls()
    })
  } else {
    results <- process_calls()
  }
  
  # Reconstruct the original data structures from parallel results
  for (i in seq_along(results)) {
    idx <- results[[i]]$index
    resp_list[[idx]] <- results[[i]]$response
    failed_calls[idx] <- results[[i]]$failed
  }
  
  # Log completion time & exit message
  end_time <- Sys.time()
  cat("Parallel processing completed in", round(as.numeric(end_time - start_time, units = "mins"), 2), "minutes\n")
  
  # Return results
  return(list(
    responses = resp_list,
    failed_calls = failed_calls
  ))
}


#' Helper function to build EP API URLs for meetings/{id}/decisions
#' 
#' @param meeting_ids Character vector of meeting IDs
#' @return Character vector of complete API URLs
#' 
build_decisions_urls <- function(meeting_ids) {
  paste0(
    "https://data.europarl.europa.eu/api/v2/meetings/",
    meeting_ids,
    "/decisions?format=application%2Fld%2Bjson&json-layout=framed"
  )
}


#' Helper function to build other EP API URLs
#' 
#' @param endpoint Character string, API endpoint (e.g., "meetings", "meps")
#' @param ids Character vector of IDs (optional)
#' @param params Character string, additional URL parameters (optional)
#' @return Character vector of complete API URLs
#' 
build_ep_api_urls <- function(endpoint, ids = NULL, params = "?format=application%2Fld%2Bjson&json-layout=framed") {
  base_url <- "https://data.europarl.europa.eu/api/v2/"
  
  if (is.null(ids)) {
    paste0(base_url, endpoint, params)
  } else {
    paste0(base_url, endpoint, "/", ids, params)
  }
}