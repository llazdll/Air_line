#!/usr/bin/env Rscript
# =============================================================================
# Setup Script: Install R Packages for Hadoop Analysis
# =============================================================================
# This script installs all required R packages including:
#   - CRAN packages (dplyr, ggplot2, cluster, etc.)
#   - RHadoop packages (rhdfs, rmr2) from GitHub
# =============================================================================

cat("============================================\n")
cat("Installing R Packages for Hadoop Analysis\n")
cat("============================================\n\n")

# --- Helper: Install from CRAN if missing ---
install_if_missing <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat(sprintf("  Installing %s from CRAN...\n", pkg))
    install.packages(pkg, repos = "https://cloud.r-project.org/",
                     dependencies = TRUE, quiet = TRUE)
    cat(sprintf("  %s installed.\n", pkg))
  } else {
    cat(sprintf("  %s already installed.\n", pkg))
  }
}

# --- Step 1: System-level dependencies ---
# NOTE: Before running this script, ensure you have:
#   - Java JDK 8 or 11 (run: sudo apt install openjdk-11-jdk)
#   - rJava system deps (run: sudo apt install r-cran-rjava)
#   - Hadoop installed and HADOOP_HOME set
#
# After installing Java, run: sudo R CMD javareconf

# --- Step 2: CRAN packages ---
cat("\n--- Installing CRAN packages ---\n")

# Core dependencies for rhdfs and rmr2
install_if_missing("rJava")
install_if_missing("Rcpp")
install_if_missing("reshape2")
install_if_missing("bitops")
install_if_missing("caTools")

# Data manipulation and visualization
install_if_missing("dplyr")
install_if_missing("tidyr")
install_if_missing("ggplot2")

# Statistical analysis
install_if_missing("cluster")
install_if_missing("factoextra")
install_if_missing("dunn.test")
install_if_missing("car")
install_if_missing("broom")
install_if_missing("pheatmap")

# devtools for GitHub installs
install_if_missing("devtools")

# --- Step 3: RHadoop packages from GitHub ---
cat("\n--- Installing RHadoop packages from GitHub ---\n")

library(devtools)

# rhdfs — HDFS interface for R
if (!requireNamespace("rhdfs", quietly = TRUE)) {
  cat("  Installing rhdfs from RevolutionAnalytics/rhdfs...\n")
  install_github("RevolutionAnalytics/rhdfs", force = FALSE)
  cat("  rhdfs installed.\n")
} else {
  cat("  rhdfs already installed.\n")
}

# rmr2 — MapReduce in R
if (!requireNamespace("rmr2", quietly = TRUE)) {
  cat("  Installing rmr2 from RevolutionAnalytics/rmr2...\n")
  install_github("RevolutionAnalytics/rmr2", force = FALSE)
  cat("  rmr2 installed.\n")
} else {
  cat("  rmr2 already installed.\n")
}

# --- Step 4: Verify installations ---
cat("\n--- Verifying installations ---\n")

verify_pkg <- function(pkg) {
  if (requireNamespace(pkg, quietly = TRUE)) {
    ver <- as.character(packageVersion(pkg))
    cat(sprintf("  OK  %s (v%s)\n", pkg, ver))
    return(TRUE)
  } else {
    cat(sprintf("  FAIL %s — not loadable\n", pkg))
    return(FALSE)
  }
}

core_pkgs <- c("rJava", "Rcpp", "dplyr", "ggplot2", "cluster",
               "factoextra", "car", "dunn.test", "rhdfs", "rmr2")
results <- sapply(core_pkgs, verify_pkg)

# --- Step 5: Initialize rhdfs ---
cat("\n--- Initializing rhdfs ---\n")
tryCatch({
  library(rhdfs)
  hdfs.init()
  cat("  rhdfs initialized successfully.\n")
}, error = function(e) {
  cat("  WARNING: rhdfs.init() failed. This is expected if Hadoop is not running.\n")
  cat("  Error:", conditionMessage(e), "\n")
  cat("  Start Hadoop and try again: $HADOOP_HOME/sbin/start-dfs.sh\n")
})

cat("\n============================================\n")
cat("Package installation complete!\n")
cat("============================================\n")
cat("\nNext steps:\n")
cat("1. Source the environment: source config/hadoop_env.sh\n")
cat("2. Start Hadoop: $HADOOP_HOME/sbin/start-dfs.sh\n")
cat("3. Run HDFS setup: Rscript setup/02_hdfs_setup.R\n")
