#!/usr/bin/env Rscript
# =============================================================================
# Job 1: Descriptive Statistics via MapReduce
# =============================================================================
# Computes per-airline descriptive statistics (n, mean, sd, min, max)
# for each delay type (carrier, ATC, weather) using a single MapReduce pass.
#
# Map:   For each row, emit (airline::delay_type, {n=1, sum=x, sum²=x², min, max})
# Reduce: Aggregate to compute mean, sd, min, max per airline per delay type
# =============================================================================

source("src/lib/mr_helpers.R")
library(rhdfs)
hdfs.init()

section_header("Job 1: Descriptive Statistics (MapReduce)")

# --- Map Function ---
map_descriptive <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  airline <- row$airline
  if (is.na(airline) || airline == "") return(NULL)

  # Emit stats for each delay column
  for (col in DELAY_COLS) {
    x <- row[[col]]
    if (!is.na(x)) {
      key <- paste(airline, col, sep = "::")
      val <- list(n = 1, sum = x, sum_sq = x * x, min = x, max = x)
      keyval(key, val)
    }
  }
}

# --- Reduce Function ---
reduce_descriptive <- function(k, vv) {
  total_n     <- sum(sapply(vv, `[[`, "n"))
  total_sum   <- sum(sapply(vv, `[[`, "sum"))
  total_ss    <- sum(sapply(vv, `[[`, "sum_sq"))
  global_min  <- min(sapply(vv, `[[`, "min"))
  global_max  <- max(sapply(vv, `[[`, "max"))

  mean_val <- total_sum / total_n
  # Sample standard deviation (Bessel's correction)
  sd_val <- sqrt((total_ss - total_sum^2 / total_n) / (total_n - 1))

  keyval(k, list(
    n    = total_n,
    mean = round(mean_val, 6),
    sd   = round(sd_val, 6),
    min  = round(global_min, 6),
    max  = round(global_max, 6)
  ))
}

# --- Run MapReduce Job ---
output_path <- file.path(HDFS_BASE, "output", "descriptive")

cat("  Submitting MapReduce job for descriptive statistics...\n")
mr_result <- run_mr_job(
  input     = HDFS_INPUT,
  output    = output_path,
  map_fn    = map_descriptive,
  reduce_fn = reduce_descriptive
)

# --- Pull Results to Local R ---
cat("\n  Pulling results from HDFS...\n")
results <- pull_mr_results(output_path)

if (!is.null(results)) {
  # Convert to readable data frame
  stats_df <- data.frame(
    key   = results$key,
    stringsAsFactors = FALSE
  )

  # Parse the composite key back into airline and delay_type
  key_parts <- do.call(rbind, strsplit(stats_df$key, "::"))
  stats_df$airline   <- key_parts[, 1]
  stats_df$delay_type <- key_parts[, 2]

  # Extract values
  stats_df$n    <- sapply(results$val, `[[`, "n")
  stats_df$mean <- sapply(results$val, `[[`, "mean")
  stats_df$sd   <- sapply(results$val, `[[`, "sd")
  stats_df$min  <- sapply(results$val, `[[`, "min")
  stats_df$max  <- sapply(results$val, `[[`, "max")

  stats_df <- stats_df[, c("airline", "delay_type", "n", "mean", "sd", "min", "max")]
  stats_df <- stats_df[order(stats_df$airline, stats_df$delay_type), ]

  # Rename delay_type for readability
  stats_df$delay_type <- gsub("pct_", "", stats_df$delay_type)
  stats_df$delay_type <- gsub("_", " ", stats_df$delay_type)

  cat("\n  --- Descriptive Statistics ---\n\n")
  print(as.data.frame(stats_df), row.names = FALSE)

  # Save to local file
  ensure_local_dir("output/hdfs_export")
  write.csv(stats_df, "output/hdfs_export/descriptive_stats.csv", row.names = FALSE)
  cat("\n  Saved to: output/hdfs_export/descriptive_stats.csv\n")

  # Print unique airlines and total count (from the data)
  cat(sprintf("\n  Unique airlines: %d\n", length(unique(stats_df$airline))))
  cat(sprintf("  Airlines: %s\n", paste(sort(unique(stats_df$airline)), collapse = ", ")))

  # Save for use by later jobs
  saveRDS(stats_df, "output/hdfs_export/descriptive_stats.rds")
} else {
  cat("  ERROR: No results returned from MapReduce job.\n")
}

cat("\n  Job 1 complete.\n")
