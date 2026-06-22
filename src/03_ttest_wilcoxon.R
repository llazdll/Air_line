#!/usr/bin/env Rscript
# =============================================================================
# Job 3: T-Test and Wilcoxon Tests via MapReduce
# =============================================================================
# Part A: Independent Samples T-Test (Delta vs United on carrier delay)
#   - MapReduce for sufficient statistics (n, sum, sum_sq)
#   - Local R for Welch's t-test, pooled t-test, Cohen's d
#
# Part B: Wilcoxon Rank-Sum Test (Delta vs United)
#   - MapReduce to extract subsets
#   - Local R for wilcox.test()
#
# Part C: Kruskal-Wallis Test (all airlines)
#   - MapReduce to extract all data
#   - Local R for kruskal.test() and Dunn's post-hoc
# =============================================================================

source("src/lib/mr_helpers.R")
library(rhdfs)
hdfs.init()

section_header("Job 3: T-Test, Wilcoxon, and Kruskal-Wallis (MapReduce)")

# =============================================================================
# Part A: Independent Samples T-Test
# =============================================================================

cat("\n--- Part A: Independent Samples T-Test (Delta vs United) ---\n")

map_ttest <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  airline <- row$airline
  if (!(airline %in% c("Delta", "United"))) return(NULL)

  x <- row$pct_carrier_delay
  if (is.na(x)) return(NULL)

  keyval(airline, c(n = 1, sum = x, sum_sq = x * x))
}

reduce_ttest <- function(k, vv) {
  vv_mat <- do.call(rbind, vv)
  n   <- sum(vv_mat[, "n"])
  s   <- sum(vv_mat[, "sum"])
  ss  <- sum(vv_mat[, "sum_sq"])
  m   <- s / n
  v   <- (ss - s^2 / n) / (n - 1)
  keyval(k, c(n = n, mean = m, var = v))
}

output_ttest <- file.path(HDFS_BASE, "output", "ttest")

run_mr_job(
  input     = HDFS_INPUT,
  output    = output_ttest,
  map_fn    = map_ttest,
  reduce_fn = reduce_ttest
)

ttest_stats <- pull_mr_results(output_ttest)

if (!is.null(ttest_stats) && length(ttest_stats$key) >= 2) {
  # Extract stats for Delta and United
  stats_list <- list()
  for (i in seq_along(ttest_stats$key)) {
    airline <- ttest_stats$key[i]
    v <- ttest_stats$val[[i]]
    stats_list[[airline]] <- list(
      n   = v[["n"]],
      mean = v[["mean"]],
      var = v[["var"]]
    )
  }

  delta <- stats_list[["Delta"]]
  united <- stats_list[["United"]]

  cat("\n  Delta:\n")
  cat(sprintf("    n = %d, mean = %.4f, sd = %.4f\n",
              delta$n, delta$mean, sqrt(delta$var)))
  cat("  United:\n")
  cat(sprintf("    n = %d, mean = %.4f, sd = %.4f\n",
              united$n, united$mean, sqrt(united$var)))

  # Welch's t-test
  welch <- welch_ttest(delta$n, united$n, delta$mean, united$mean,
                        delta$var, united$var)

  cat(sprintf("\n  Welch's t-test:\n"))
  cat(sprintf("    t = %.4f, df = %.2f, p-value = %.6f\n",
              welch$t_stat, welch$df, welch$p_value))

  # Pooled variance t-test
  pooled_var <- ((delta$n - 1) * delta$var + (united$n - 1) * united$var) /
                (delta$n + united$n - 2)
  se_pooled <- sqrt(pooled_var * (1 / delta$n + 1 / united$n))
  t_pooled <- (delta$mean - united$mean) / se_pooled
  df_pooled <- delta$n + united$n - 2
  p_pooled <- 2 * pt(-abs(t_pooled), df = df_pooled)

  cat(sprintf("  Pooled variance t-test:\n"))
  cat(sprintf("    t = %.4f, df = %d, p-value = %.6f\n",
              t_pooled, df_pooled, p_pooled))

  # Cohen's d
  d <- cohens_d(delta$mean, united$mean,
                sqrt(delta$var), sqrt(united$var),
                delta$n, united$n)
  cat(sprintf("  Cohen's d = %.4f (%s)\n", d, interpret_d(d)))

  # F-test for equal variances
  f_var <- max(delta$var, united$var) / min(delta$var, united$var)
  df1 <- ifelse(delta$var > united$var, delta$n - 1, united$n - 1)
  df2 <- ifelse(delta$var > united$var, united$n - 1, delta$n - 1)
  p_var <- pf(f_var, df1, df2, lower.tail = FALSE)
  cat(sprintf("  F-test for equal variances: F = %.4f, p = %.6f\n", f_var, p_var))

  ttest_results <- list(
    delta = delta, united = united,
    welch = welch, pooled_t = t_pooled, pooled_p = p_pooled,
    cohens_d = d
  )
} else {
  cat("  WARNING: Could not find both Delta and United in results.\n")
  ttest_results <- NULL
}

# =============================================================================
# Part B: Wilcoxon Rank-Sum Test
# =============================================================================

cat("\n--- Part B: Wilcoxon Rank-Sum Test (Delta vs United) ---\n")

map_wilcoxon <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  airline <- row$airline
  if (!(airline %in% c("Delta", "United"))) return(NULL)

  x <- row$pct_carrier_delay
  if (is.na(x)) return(NULL)

  keyval(airline, x)
}

# No reduce needed — just collect values per key
reduce_wilcoxon <- function(k, vv) {
  keyval(k, vv)
}

output_wilcoxon <- file.path(HDFS_BASE, "output", "wilcoxon")

run_mr_job(
  input     = HDFS_INPUT,
  output    = output_wilcoxon,
  map_fn    = map_wilcoxon,
  reduce_fn = reduce_wilcoxon
)

wilcoxon_results <- pull_mr_results(output_wilcoxon)

if (!is.null(wilcoxon_results)) {
  # Extract values for Delta and United
  delta_vals <- NULL
  united_vals <- NULL

  for (i in seq_along(wilcoxon_results$key)) {
    if (wilcoxon_results$key[i] == "Delta") {
      delta_vals <- unlist(wilcoxon_results$val[[i]])
    } else if (wilcoxon_results$key[i] == "United") {
      united_vals <- unlist(wilcoxon_results$val[[i]])
    }
  }

  if (!is.null(delta_vals) && !is.null(united_vals)) {
    # Wilcoxon rank-sum test
    w_test <- wilcox.test(delta_vals, united_vals, exact = FALSE)

    # Effect size: r = Z / sqrt(N)
    z_stat <- qnorm(w_test$p.value / 2, lower.tail = FALSE)
    N <- length(delta_vals) + length(united_vals)
    r_effect <- z_stat / sqrt(N)

    cat(sprintf("  n(Delta) = %d, n(United) = %d\n",
                length(delta_vals), length(united_vals)))
    cat(sprintf("  Wilcoxon W = %.0f\n", w_test$statistic))
    cat(sprintf("  p-value = %.6f\n", w_test$p.value))
    cat(sprintf("  Effect size r = %.4f\n", r_effect))

    wilcoxon_test <- w_test
    wilcoxon_r <- r_effect
  } else {
    cat("  WARNING: Missing data for one or both airlines.\n")
    wilcoxon_test <- NULL
    wilcoxon_r <- NULL
  }
} else {
  cat("  WARNING: No Wilcoxon results returned.\n")
  wilcoxon_test <- NULL
  wilcoxon_r <- NULL
}

# =============================================================================
# Part C: Kruskal-Wallis Test
# =============================================================================

cat("\n--- Part C: Kruskal-Wallis Test (All Airlines) ---\n")

map_kruskal <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  airline <- row$airline
  x <- row$pct_carrier_delay
  if (is.na(airline) || is.na(x)) return(NULL)

  keyval(airline, x)
}

reduce_kruskal <- function(k, vv) {
  keyval(k, unlist(vv))
}

output_kruskal <- file.path(HDFS_BASE, "output", "kruskal")

run_mr_job(
  input     = HDFS_INPUT,
  output    = output_kruskal,
  map_fn    = map_kruskal,
  reduce_fn = reduce_kruskal
)

kruskal_results <- pull_mr_results(output_kruskal)

if (!is.null(kruskal_results)) {
  # Build a list of per-airline values
  airline_values <- list()
  for (i in seq_along(kruskal_results$key)) {
    airline_values[[kruskal_results$key[i]]] <- unlist(kruskal_results$val[[i]])
  }

  # Create formula data for kruskal.test
  all_values <- unlist(airline_values)
  all_groups <- rep(names(airline_values), sapply(airline_values, length))

  kw_test <- kruskal.test(all_values ~ all_groups)

  cat(sprintf("  Chi-squared = %.4f, df = %d, p-value = %.6f\n",
              kw_test$statistic, kw_test$parameter, kw_test$p.value))

  # Dunn's post-hoc test
  if (requireNamespace("dunn.test", quietly = TRUE)) {
    cat("\n  Dunn's post-hoc test (Bonferroni correction):\n")
    dunn_result <- dunn.test::dunn.test(all_values, all_groups,
                                         method = "bonferroni", list = TRUE)
    cat(sprintf("  Number of comparisons: %d\n", length(dunn_result$comparisons)))
    cat("  Top 5 most significant comparisons:\n")
    p_values <- dunn_result$P.adj
    names(p_values) <- dunn_result$comparisons
    sig_order <- order(p_values)
    for (i in head(sig_order, 5)) {
      cat(sprintf("    %s: adjusted p = %.6f\n",
                  names(p_values)[i], p_values[i]))
    }
  }

  kruskal_test <- kw_test
} else {
  cat("  WARNING: No Kruskal-Wallis results returned.\n")
  kruskal_test <- NULL
}

# =============================================================================
# Save Results
# =============================================================================

all_ttest_results <- list(
  ttest = ttest_results,
  wilcoxon = if(!is.null(wilcoxon_test)) list(test = wilcoxon_test, r = wilcoxon_r) else NULL,
  kruskal = kruskal_test
)

ensure_local_dir("output/hdfs_export")
saveRDS(all_ttest_results, "output/hdfs_export/ttest_wilcoxon_results.rds")

cat("\n  Results saved to output/hdfs_export/\n")
cat("  Job 3 complete.\n")
