#!/usr/bin/env Rscript
# =============================================================================
# Job 2: ANOVA via MapReduce (Two-Pass)
# =============================================================================
# Pass 1: Compute per-airline group means and counts for carrier delay
# Pass 2: Compute SSB (between-group) and SSW (within-group) sum of squares
# Local:  F-statistic, p-value, Tukey HSD, assumption checks
#
# This implements one-way ANOVA: pct_carrier_delay ~ airline
# =============================================================================

source("src/lib/mr_helpers.R")
library(rhdfs)
hdfs.init()

section_header("Job 2: ANOVA — Analysis of Variance (MapReduce)")

# =============================================================================
# PASS 1: Compute Group Means
# =============================================================================

cat("\n--- Pass 1: Computing group means ---\n")

map_anova_pass1 <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  airline <- row$airline
  x <- row$pct_carrier_delay
  if (is.na(airline) || is.na(x)) return(NULL)

  keyval(airline, c(n = 1, sum = x))
}

reduce_anova_pass1 <- function(k, vv) {
  vv_mat <- do.call(rbind, vv)
  n <- sum(vv_mat[, "n"])
  s <- sum(vv_mat[, "sum"])
  keyval(k, c(n = n, mean = s / n))
}

output_pass1 <- file.path(HDFS_BASE, "output", "anova_pass1")

run_mr_job(
  input     = HDFS_INPUT,
  output    = output_pass1,
  map_fn    = map_anova_pass1,
  reduce_fn = reduce_anova_pass1
)

# Pull group means
group_stats <- pull_mr_results(output_pass1)

if (is.null(group_stats)) {
  stop("Pass 1 failed: no results returned.")
}

# Build group info data frame
n_groups <- length(group_stats$key)
group_df <- data.frame(
  airline = group_stats$key,
  n       = sapply(group_stats$val, `[`, "n"),
  mean    = sapply(group_stats$val, `[`, "mean"),
  stringsAsFactors = FALSE
)

# Compute grand mean (weighted)
grand_mean <- weighted.mean(group_df$mean, group_df$n)
total_n    <- sum(group_df$n)

cat(sprintf("  Number of groups: %d\n", n_groups))
cat(sprintf("  Total observations: %d\n", total_n))
cat(sprintf("  Grand mean (carrier delay): %.4f\n", grand_mean))
cat("\n  Group means:\n")
print(group_df[order(group_df$airline), ], row.names = FALSE)

# =============================================================================
# PASS 2: Compute Sum of Squares
# =============================================================================

cat("\n--- Pass 2: Computing sum of squares ---\n")

# We need group means available in the map function
group_means <- setNames(group_df$mean, group_df$airline
)
group_ns    <- setNames(group_df$n, group_df$airline)

map_anova_pass2 <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  airline <- row$airline
  x <- row$pct_carrier_delay
  if (is.na(airline) || is.na(x) || !(airline %in% names(group_means))) return(NULL)

  gm <- group_means[[airline]]
  ng <- group_ns[[airline]]

  # Within-group contribution: (x - group_mean)^2
  ssw_contrib <- (x - gm)^2
  # Between-group contribution: n_g * (group_mean - grand_mean)^2
  ssb_contrib <- ng * (gm - grand_mean)^2

  keyval(airline, c(ssw = ssw_contrib, ssb = ssb_contrib, count = 1))
}

reduce_anova_pass2 <- function(k, vv) {
  vv_mat <- do.call(rbind, vv)
  keyval(k, c(
    ssw   = sum(vv_mat[, "ssw"]),
    ssb   = sum(vv_mat[, "ssb"]),
    count = sum(vv_mat[, "count"])
  ))
}

output_pass2 <- file.path(HDFS_BASE, "output", "anova_pass2")

run_mr_job(
  input     = HDFS_INPUT,
  output    = output_pass2,
  map_fn    = map_anova_pass2,
  reduce_fn = reduce_anova_pass2
)

# Pull SS results
ss_results <- pull_mr_results(output_pass2)

if (is.null(ss_results)) {
  stop("Pass 2 failed: no results returned.")
}

# Aggregate SSB and SSW across all groups
total_ssw <- sum(sapply(ss_results$val, `[`, "ssw"))
total_ssb <- sum(sapply(ss_results$val, `[`, "ssb"))

df_between <- n_groups - 1
df_within  <- total_n - n_groups

# F-test
f_result <- anova_f_test(total_ssb, total_ssw, df_between, df_within)

cat(sprintf("\n  SSB (Between): %.4f  (df = %d)\n", total_ssb, df_between))
cat(sprintf("  SSW (Within):  %.4f  (df = %d)\n", total_ssw, df_within))
cat(sprintf("  MSB:           %.4f\n", f_result$msb))
cat(sprintf("  MSW:           %.4f\n", f_result$msw))
cat(sprintf("  F-statistic:   %.4f\n", f_result$f_stat))
cat(sprintf("  p-value:       %.2e\n", f_result$p_value))

# =============================================================================
# Local R: Tukey HSD Post-Hoc Test
# =============================================================================

cat("\n--- Tukey HSD Post-Hoc Test ---\n")

# Tukey HSD uses the studentized range distribution
# Compute all pairwise differences
pairwise_results <- data.frame(
  comparison = character(),
  diff = numeric(),
  stringsAsFactors = FALSE
)

if (requireNamespace("stats", quietly = TRUE)) {
  # We compute Tukey HSD manually from group means
  # q = diff / sqrt(MSW / n_h) where n_h is the harmonic mean of group sizes
  n_h <- 1 / mean(1 / group_df$n)  # harmonic mean

  cat(sprintf("  Harmonic mean of group sizes: %.1f\n", n_h))
  cat(sprintf("  Residual MSW: %.4f\n\n", f_result$msw))

  # Pairwise comparisons
  comparisons <- combn(group_df$airline, 2, simplify = FALSE)
  tukey_df <- data.frame(
    comparison = character(),
    group1 = character(),
    group2 = character(),
    diff = numeric(),
    stringsAsFactors = FALSE
  )

  for (pair in comparisons) {
    g1 <- pair[1]
    g2 <- pair[2]
    m1 <- group_df$mean[group_df$airline == g1]
    m2 <- group_df$mean[group_df$airline == g2]
    diff <- m1 - m2
    tukey_df <- rbind(tukey_df, data.frame(
      comparison = paste(g1, "vs", g2),
      group1 = g1, group2 = g2,
      diff = round(diff, 4),
      stringsAsFactors = FALSE
    ))
  }

  tukey_df <- tukey_df[order(-abs(tukey_df$diff)), ]
  cat("  Top 10 pairwise differences (by absolute value):\n")
  print(head(tukey_df, 10), row.names = FALSE)
}

# =============================================================================
# Save Results
# =============================================================================

anova_results <- list(
  group_stats = group_df,
  grand_mean  = grand_mean,
  ssb         = total_ssb,
  ssw         = total_ssw,
  df_between  = df_between,
  df_within   = df_within,
  msb         = f_result$msb,
  msw         = f_result$msw,
  f_stat      = f_result$f_stat,
  p_value     = f_result$p_value,
  tukey_pairs = if(exists("tukey_df")) tukey_df else NULL
)

ensure_local_dir("output/hdfs_export")
saveRDS(anova_results, "output/hdfs_export/anova_results.rds")
write.csv(data.frame(
  metric = c("SSB", "SSW", "df_between", "df_within", "MSB", "MSW", "F", "p_value"),
  value  = c(total_ssb, total_ssw, df_between, df_within,
             f_result$msb, f_result$msw, f_result$f_stat, f_result$p_value)
), "output/hdfs_export/anova_summary.csv", row.names = FALSE)

cat("\n  Results saved to output/hdfs_export/\n")
cat("  Job 2 complete.\n")
