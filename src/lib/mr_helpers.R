# =============================================================================
# MapReduce Helper Functions
# =============================================================================
# Shared utilities for all MapReduce jobs in the airline analysis pipeline.
# Provides: CSV parsing, input format setup, HDFS I/O, result extraction.
# =============================================================================

# --- Load required libraries ---
suppressPackageStartupMessages({
  library(rmr2)
  library(rhdfs)
  library(dplyr)
})

# =============================================================================
# CONFIGURATION
# =============================================================================

HDFS_BASE <- "/user/ruser/airline_analysis"
HDFS_INPUT <- file.path(HDFS_BASE, "input", "airline_stats.csv")

# Column names in the CSV (must match airline_stats.csv header)
COL_NAMES <- c("pct_carrier_delay", "pct_atc_delay", "pct_weather_delay", "airline")
DELAY_COLS <- c("pct_carrier_delay", "pct_atc_delay", "pct_weather_delay")

# =============================================================================
# BACKEND CONFIGURATION
# =============================================================================

#' Configure rmr2 backend
#' @param backend Either "hadoop" for cluster mode or "local" for testing
configure_backend <- function(backend = "hadoop") {
  if (backend == "local") {
    rmr.options(backend = "local")
    cat("[INFO] Using LOCAL backend (no Hadoop required).\n")
  } else {
    rmr.options(backend = "hadoop")
    cat("[INFO] Using HADOOP backend.\n")
  }
}

# =============================================================================
# CSV PARSING HELPERS
# =============================================================================

#' Parse a CSV line into a named list
#' Handles the header row by returning NULL
#' @param line A single CSV line (character string)
#' @param header The header line (column names)
#' @return A named list of values, or NULL for header/invalid rows
parse_csv_line <- function(line, header = COL_NAMES) {
  # Skip empty lines
  if (nchar(trimws(line)) == 0) return(NULL)

  # Split by comma (simple CSV — no quoted commas in this dataset)
  parts <- strsplit(line, ",")[[1]]

  # Skip header row
  if (parts[1] == header[1]) return(NULL)

  # Must have exactly 4 columns
  if (length(parts) < 4) return(NULL)

  list(
    pct_carrier_delay = suppressWarnings(as.numeric(parts[1])),
    pct_atc_delay     = suppressWarnings(as.numeric(parts[2])),
    pct_weather_delay = suppressWarnings(as.numeric(parts[3])),
    airline           = parts[4]
  )
}

#' Create a CSV input format for rmr2 that handles our dataset
#' @return An rmr2 input format object
make_csv_input_format <- function() {
  make.input.format(
    format = "csv",
    sep = ",",
    col.names = COL_NAMES
  )
}

# =============================================================================
# HDFS I/O HELPERS
# =============================================================================

#' Read a CSV file from HDFS into a local R data frame
#' @param hdfs_path Path to the CSV file in HDFS
#' @return A data frame
read_hdfs_csv <- function(hdfs_path) {
  tryCatch({
    raw <- hdfs.read.text.file(hdfs_path)
    con <- textConnection(raw)
    df <- read.csv(con, header = TRUE, stringsAsFactors = FALSE,
                   colClasses = c("numeric", "numeric", "numeric", "character"))
    close(con)
    return(df)
  }, error = function(e) {
    warning("Failed to read from HDFS: ", conditionMessage(e))
    return(NULL)
  })
}

#' Pull MapReduce results from HDFS to a local data frame
#' @param hdfs_output_path The HDFS output directory from a MapReduce job
#' @return A data frame with columns 'key' and 'val'
pull_mr_results <- function(hdfs_output_path) {
  results <- tryCatch({
    from.dfs(hdfs_output_path)
  }, error = function(e) {
    warning("Failed to read MapReduce results from: ", hdfs_output_path,
            "\nError: ", conditionMessage(e))
    return(NULL)
  })
  return(results)
}

#' Clean up an HDFS directory (delete and recreate)
#' @param hdfs_path Path to the HDFS directory
hdfs_reset_dir <- function(hdfs_path) {
  tryCatch(
    hdfs.delete(hdfs_path),
    error = function(e) NULL  # Directory might not exist
  )
  hdfs.mkdir(hdfs_path)
}

# =============================================================================
# MAPREDUCE JOB RUNNER
# =============================================================================

#' Run a MapReduce job with standard configuration
#' @param input Input path (HDFS) or data frame (local)
#' @param output Output HDFS path
#' @param map_fn Map function: function(k, v) -> keyval(k, v)
#' @param reduce_fn Reduce function: function(k, vv) -> keyval(k, v)
#' @param combine Whether to use a combiner (default: FALSE)
#' @return The MapReduce result object
run_mr_job <- function(input, output, map_fn, reduce_fn, combine = FALSE) {
  cat(sprintf("  Running MapReduce job...\n"))
  cat(sprintf("    Input:  %s\n", deparse(substitute(input))))
  cat(sprintf("    Output: %s\n", output))

  # Clean output directory
  tryCatch(hdfs.delete(output), error = function(e) NULL)

  job <- mapreduce(
    input  = input,
    output = output,
    map    = map_fn,
    reduce = reduce_fn,
    combine = combine,
    input.format = make_csv_input_format()
  )

  cat("  MapReduce job completed.\n")
  return(job)
}

# =============================================================================
# STATISTICAL HELPER FUNCTIONS (for local R post-processing)
# =============================================================================

#' Compute Cohen's d effect size
#' @param m1, m2 Group means
#' @param s1, s2 Group standard deviations
#' @param n1, n2 Group sample sizes
#' @return Cohen's d value
cohens_d <- function(m1, m2, s1, s2, n1, n2) {
  pooled_sd <- sqrt(((n1 - 1) * s1^2 + (n2 - 1) * s2^2) / (n1 + n2 - 2))
  d <- (m1 - m2) / pooled_sd
  return(d)
}

#' Interpret Cohen's d magnitude
interpret_d <- function(d) {
  d_abs <- abs(d)
  if (d_abs < 0.2) return("negligible")
  if (d_abs < 0.5) return("small")
  if (d_abs < 0.8) return("medium")
  return("large")
}

#' Welch's t-test from sufficient statistics
#' @param n1, n2, m1, m2, v1, v2 Group statistics
#' @return List with t_stat, df, p_value
welch_ttest <- function(n1, n2, m1, m2, v1, v2) {
  se <- sqrt(v1 / n1 + v2 / n2)
  t_stat <- (m1 - m2) / se
  # Welch-Satterthwaite degrees of freedom
  df <- (v1 / n1 + v2 / n2)^2 /
        ((v1 / n1)^2 / (n1 - 1) + (v2 / n2)^2 / (n2 - 1))
  p_value <- 2 * pt(-abs(t_stat), df = df)
  list(t_stat = t_stat, df = df, p_value = p_value)
}

#' Compute F-statistic for ANOVA from SSB and SSW
#' @param ssb Sum of squares between
#' @param ssw Sum of squares within
#' @param df_b Degrees of freedom between
#' @param df_w Degrees of freedom within
#' @return List with f_stat and p_value
anova_f_test <- function(ssb, ssw, df_b, df_w) {
  msb <- ssb / df_b
  msw <- ssw / df_w
  f_stat <- msb / msw
  p_value <- pf(f_stat, df_b, df_w, lower.tail = FALSE)
  list(f_stat = f_stat, p_value = p_value, msb = msb, msw = msw)
}

# =============================================================================
# UTILITY
# =============================================================================

#' Print a section header
section_header <- function(title) {
  cat("\n")
  cat("============================================================\n")
  cat(title, "\n")
  cat("============================================================\n")
}

#' Ensure output directory exists locally
ensure_local_dir <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
  }
}
