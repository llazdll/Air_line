#!/usr/bin/env Rscript
# =============================================================================
# Setup Script: HDFS Directory Structure and Data Upload
# =============================================================================
# Creates the HDFS directory structure for the airline analysis pipeline
# and uploads the dataset from local disk to HDFS.
#
# Prerequisites:
#   - Hadoop HDFS must be running (start-dfs.sh)
#   - rhdfs package must be installed
#   - airline_stats.csv must exist in the project root
# =============================================================================

library(rhdfs)

# Initialize HDFS connection
tryCatch({
  hdfs.init()
  cat("Connected to HDFS successfully.\n\n")
}, error = function(e) {
  stop("Failed to connect to HDFS. Is Hadoop running?\n",
        "Start it with: $HADOOP_HOME/sbin/start-dfs.sh\n",
        "Error: ", conditionMessage(e))
})

# Base directory in HDFS
base_dir <- "/user/ruser/airline_analysis"

cat("============================================\n")
cat("HDFS Setup: Creating Directory Structure\n")
cat("============================================\n\n")

# --- Create directory structure ---
dirs <- c(
  base_dir,
  file.path(base_dir, "input"),
  file.path(base_dir, "output"),
  file.path(base_dir, "output", "descriptive"),
  file.path(base_dir, "output", "anova_pass1"),
  file.path(base_dir, "output", "anova_pass2"),
  file.path(base_dir, "output", "ttest"),
  file.path(base_dir, "output", "wilcoxon"),
  file.path(base_dir, "output", "clustering_airlines"),
  file.path(base_dir, "output", "clustering_sample"),
  file.path(base_dir, "output", "regression"),
  file.path(base_dir, "output", "regression_residuals"),
  file.path(base_dir, "output", "logistic_prep")
)

for (d in dirs) {
  # Remove if exists (for re-runs), then create
  tryCatch(hdfs.delete(d), error = function(e) NULL)
  hdfs.mkdir(d)
  cat(sprintf("  Created: %s\n", d))
}

# --- Upload dataset ---
cat("\n--- Uploading dataset ---\n")

local_csv <- file.path(getwd(), "airline_stats.csv")
hdfs_path <- file.path(base_dir, "input", "airline_stats.csv")

if (!file.exists(local_csv)) {
  stop("Dataset not found at: ", local_csv,
       "\nMake sure airline_stats.csv is in the project root directory.")
}

# Remove existing file in HDFS if present
tryCatch(hdfs.delete(hdfs_path), error = function(e) NULL)

# Upload
hdfs.put(local_csv, hdfs_path)
cat(sprintf("  Uploaded: %s -> %s\n", local_csv, hdfs_path))

# --- Verify ---
cat("\n--- Verification ---\n")

cat("\nHDFS directory listing:\n")
print(hdfs.ls(base_dir, recurse = TRUE))

cat("\nUploaded file info:\n")
info <- hdfs.file.info(hdfs_path)
print(info)

# Quick sanity check: read first few lines
cat("\nFirst 3 lines from HDFS:\n")
lines <- hdfs.read.text.file(hdfs_path, nlines = 3)
cat(lines, "\n")

cat("\n============================================\n")
cat("HDFS setup complete!\n")
cat("============================================\n")
cat(sprintf("\nDataset location: %s\n", hdfs_path))
cat(sprintf("Output directory: %s/output/\n", base_dir))
