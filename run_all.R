#!/usr/bin/env Rscript
# =============================================================================
# Airline Delay Analysis — Hadoop MapReduce Pipeline
# =============================================================================
# Master orchestration script that runs all analysis jobs in sequence.
#
# Usage:
#   Rscript run_all.R              # Run with Hadoop backend
#   Rscript run_all.R --local      # Run in local mode (no Hadoop needed)
#   Rscript run_all.R --hadoop     # Explicitly use Hadoop backend
#
# Prerequisites:
#   1. Source config/hadoop_env.sh
#   2. Run setup/01_install_r_packages.R
#   3. Start Hadoop: $HADOOP_HOME/sbin/start-dfs.sh
#   4. Run setup/02_hdfs_setup.R
# =============================================================================

cat("\n")
cat("############################################################\n")
cat("#                                                          #\n")
cat("#   Airline Delay Analysis — Hadoop MapReduce Pipeline     #\n")
cat("#                                                          #\n")
cat("############################################################\n")
cat("\n")

# --- Parse command-line arguments ---
args <- commandArgs(trailingOnly = TRUE)

if ("--local" %in% args) {
  backend <- "local"
} else if ("--hadoop" %in% args) {
  backend <- "hadoop"
} else {
  backend <- "hadoop"  # Default
}

cat(sprintf("Backend: %s\n", backend))
cat(sprintf("Start time: %s\n", Sys.time()))
cat("\n")

# --- Record start time ---
start_time <- proc.time()

# --- Load required libraries ---
cat("Loading libraries...\n")
library(rmr2)
library(rhdfs)
library(dplyr)
library(ggplot2)

# --- Configure backend ---
configure_backend(backend)

# --- Initialize HDFS ---
cat("Initializing HDFS connection...\n")
tryCatch({
  hdfs.init()
  cat("HDFS connection established.\n\n")
}, error = function(e) {
  if (backend == "hadoop") {
    stop("Failed to connect to Hadoop HDFS.\n",
         "Make sure Hadoop is running: $HADOOP_HOME/sbin/start-dfs.sh\n",
         "Error: ", conditionMessage(e))
  } else {
    cat("Running in local mode — HDFS not required.\n\n")
  }
})

# =============================================================================
# Run All Jobs
# =============================================================================

total_jobs <- 7

# --- Job 1: Descriptive Statistics ---
cat(sprintf("\n[Job 1/%d] Descriptive Statistics\n", total_jobs))
tryCatch({
  source("src/01_descriptive_stats.R", local = TRUE)
  cat("  ✓ Job 1 completed successfully.\n")
}, error = function(e) {
  cat("  ✗ Job 1 FAILED:", conditionMessage(e), "\n")
})

# --- Job 2: ANOVA ---
cat(sprintf("\n[Job 2/%d] ANOVA\n", total_jobs))
tryCatch({
  source("src/02_anova.R", local = TRUE)
  cat("  ✓ Job 2 completed successfully.\n")
}, error = function(e) {
  cat("  ✗ Job 2 FAILED:", conditionMessage(e), "\n")
})

# --- Job 3: T-Test and Wilcoxon ---
cat(sprintf("\n[Job 3/%d] T-Test and Wilcoxon Tests\n", total_jobs))
tryCatch({
  source("src/03_ttest_wilcoxon.R", local = TRUE)
  cat("  ✓ Job 3 completed successfully.\n")
}, error = function(e) {
  cat("  ✗ Job 3 FAILED:", conditionMessage(e), "\n")
})

# --- Job 4: Clustering ---
cat(sprintf("\n[Job 4/%d] Clustering Analysis\n", total_jobs))
tryCatch({
  source("src/04_clustering.R", local = TRUE)
  cat("  ✓ Job 4 completed successfully.\n")
}, error = function(e) {
  cat("  ✗ Job 4 FAILED:", conditionMessage(e), "\n")
})

# --- Job 5: Linear Regression ---
cat(sprintf("\n[Job 5/%d] Linear Regression\n", total_jobs))
tryCatch({
  source("src/05_regression.R", local = TRUE)
  cat("  ✓ Job 5 completed successfully.\n")
}, error = function(e) {
  cat("  ✗ Job 5 FAILED:", conditionMessage(e), "\n")
})

# --- Job 6: Logistic Regression ---
cat(sprintf("\n[Job 6/%d] Logistic Regression\n", total_jobs))
tryCatch({
  source("src/06_logistic_regression.R", local = TRUE)
  cat("  ✓ Job 6 completed successfully.\n")
}, error = function(e) {
  cat("  ✗ Job 6 FAILED:", conditionMessage(e), "\n")
})

# --- Job 7: Visualization ---
cat(sprintf("\n[Job 7/%d] Visualization\n", total_jobs))
tryCatch({
  source("src/07_visualization.R", local = TRUE)
  cat("  ✓ Job 7 completed successfully.\n")
}, error = function(e) {
  cat("  ✗ Job 7 FAILED:", conditionMessage(e), "\n")
})

# =============================================================================
# Summary
# =============================================================================

elapsed <- proc.time() - start_time

cat("\n")
cat("############################################################\n")
cat("#                    PIPELINE COMPLETE                     #\n")
cat("############################################################\n")
cat(sprintf("\nEnd time: %s\n", Sys.time()))
cat(sprintf("Total elapsed time: %.1f seconds\n", elapsed["elapsed"]))
cat("\n")

cat("Output files:\n")
cat("  HDFS results:  /user/ruser/airline_analysis/output/\n")
cat("  Local results: output/hdfs_export/\n")
cat("  Plots:         output/plots/\n")
cat("\n")

# List generated plots
if (dir.exists("output/plots")) {
  plots <- list.files("output/plots", pattern = "\\.png$", full.names = FALSE)
  if (length(plots) > 0) {
    cat(sprintf("Generated %d plots:\n", length(plots)))
    for (p in plots) {
      cat(sprintf("  - %s\n", p))
    }
  }
}

cat("\nNext steps:\n")
cat("  1. View plots in output/plots/\n")
cat("  2. Run validation: Rscript tests/validate_results.R\n")
cat("  3. Check HDFS output: hdfs dfs -ls -R /user/ruser/airline_analysis/output/\n")
cat("\n")
