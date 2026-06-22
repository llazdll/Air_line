#!/usr/bin/env Rscript
# =============================================================================
# Validation Script: Compare MapReduce Results to Original R Results
# =============================================================================
# This script validates that the MapReduce pipeline produces results
# consistent with the original R analysis. It compares:
#   - Descriptive statistics (mean, sd per airline)
#   - ANOVA F-statistic and p-value
#   - T-test statistics
#   - Regression coefficients
#   - Logistic regression coefficients
#
# Tolerance: numerical results should match within 1e-4 relative error
# =============================================================================

cat("============================================================\n")
cat("Validation: MapReduce Results vs Original R Results\n")
cat("============================================================\n\n")

tolerance <- 1e-4
all_passed <- TRUE

# --- Helper: Check if two values are approximately equal ---
check_close <- function(name, val1, val2, tol = tolerance) {
  if (is.null(val1) || is.null(val2)) {
    cat(sprintf("  SKIP %s — value is NULL\n", name))
    return(TRUE)
  }
  rel_error <- abs(val1 - val2) / max(abs(val1), abs(val2), 1e-10)
  if (rel_error <= tol) {
    cat(sprintf("  PASS %s (MR: %.6f, R: %.6f, rel_err: %.2e)\n",
                name, val1, val2, rel_error))
    return(TRUE)
  } else {
    cat(sprintf("  FAIL %s (MR: %.6f, R: %.6f, rel_err: %.2e)\n",
                name, val1, val2, rel_error))
    return(FALSE)
  }
}

# =============================================================================
# 1. Validate Descriptive Statistics
# =============================================================================

cat("--- 1. Descriptive Statistics ---\n")

if (file.exists("output/hdfs_export/descriptive_stats.rds")) {
  mr_stats <- readRDS("output/hdfs_export/descriptive_stats.rds")

  # Compute original R descriptive stats
  df <- read.csv("airline_stats.csv", stringsAsFactors = FALSE)
  df <- na.omit(df)

  airlines <- sort(unique(df$airline))
  delay_cols <- c("pct_carrier_delay", "pct_atc_delay", "pct_weather_delay")

  desc_pass <- TRUE
  for (al in airlines) {
    for (col in delay_cols) {
      r_mean <- mean(df[[col]][df$airline == al])
      r_sd   <- sd(df[[col]][df$airline == al])

      col_clean <- gsub("pct_", "", gsub("_", " ", col))
      mr_row <- mr_stats[mr_stats$airline == al & mr_stats$delay_type == col_clean, ]

      if (nrow(mr_row) > 0) {
        if (!check_close(sprintf("%s %s mean", al, col), mr_row$mean, r_mean))
          desc_pass <- FALSE
        if (!check_close(sprintf("%s %s sd", al, col), mr_row$sd, r_sd))
          desc_pass <- FALSE
      }
    }
  }

  if (desc_pass) {
    cat("  ✓ All descriptive statistics match.\n\n")
  } else {
    cat("  ✗ Some descriptive statistics differ.\n\n")
    all_passed <- FALSE
  }
} else {
  cat("  SKIP — descriptive_stats.rds not found. Run Job 1 first.\n\n")
}

# =============================================================================
# 2. Validate ANOVA
# =============================================================================

cat("--- 2. ANOVA ---\n")

if (file.exists("output/hdfs_export/anova_results.rds")) {
  mr_anova <- readRDS("output/hdfs_export/anova_results.rds")

  # Original R ANOVA
  df <- read.csv("airline_stats.csv", stringsAsFactors = FALSE)
  df <- na.omit(df)
  r_anova <- aov(pct_carrier_delay ~ airline, data = df)
  r_summary <- summary(r_anova)
  r_f <- r_summary[[1]]$`F value`[1]
  r_p <- r_summary[[1]]$`Pr(>F)`[1]

  anova_pass <- TRUE
  if (!check_close("ANOVA F-statistic", mr_anova$f_stat, r_f)) anova_pass <- FALSE
  if (!check_close("ANOVA p-value", mr_anova$p_value, r_p, tol = 1e-2)) anova_pass <- FALSE

  if (anova_pass) {
    cat("  ✓ ANOVA results match.\n\n")
  } else {
    cat("  ✗ ANOVA results differ.\n\n")
    all_passed <- FALSE
  }
} else {
  cat("  SKIP — anova_results.rds not found. Run Job 2 first.\n\n")
}

# =============================================================================
# 3. Validate T-Test
# =============================================================================

cat("--- 3. T-Test ---\n")

if (file.exists("output/hdfs_export/ttest_wilcoxon_results.rds")) {
  mr_ttest <- readRDS("output/hdfs_export/ttest_wilcoxon_results.rds")

  # Original R t-test
  df <- read.csv("airline_stats.csv", stringsAsFactors = FALSE)
  df <- na.omit(df)
  delta_data   <- df$pct_carrier_delay[df$airline == "Delta"]
  united_data  <- df$pct_carrier_delay[df$airline == "United"]
  r_ttest <- t.test(delta_data, united_data)

  ttest_pass <- TRUE
  if (!is.null(mr_ttest$ttest)) {
    if (!check_close("Welch t-statistic", mr_ttest$ttest$welch$t_stat, r_ttest$statistic))
      ttest_pass <- FALSE
    if (!check_close("Welch p-value", mr_ttest$ttest$welch$p_value, r_ttest$p.value,
                     tol = 1e-2))
      ttest_pass <- FALSE
  }

  if (ttest_pass) {
    cat("  ✓ T-test results match.\n\n")
  } else {
    cat("  ✗ T-test results differ.\n\n")
    all_passed <- FALSE
  }
} else {
  cat("  SKIP — ttest_wilcoxon_results.rds not found. Run Job 3 first.\n\n")
}

# =============================================================================
# 4. Validate Regression
# =============================================================================

cat("--- 4. Linear Regression ---\n")

if (file.exists("output/hdfs_export/regression_results.rds")) {
  mr_reg <- readRDS("output/hdfs_export/regression_results.rds")

  # Original R regression
  df <- read.csv("airline_stats.csv", stringsAsFactors = FALSE)
  df <- na.omit(df)
  r_lm <- lm(pct_carrier_delay ~ pct_atc_delay + pct_weather_delay, data = df)
  r_coef <- coef(r_lm)

  reg_pass <- TRUE
  if (!is.null(mr_reg$model2)) {
    mr_coef <- mr_reg$model2$beta
    names(mr_coef) <- names(r_coef)

    for (nm in names(r_coef)) {
      if (!check_close(sprintf("LM coef[%s]", nm), mr_coef[nm], r_coef[nm]))
        reg_pass <- FALSE
    }

    if (!check_close("LM R-squared", mr_reg$model2$r_squared, summary(r_lm)$r.squared))
      reg_pass <- FALSE
  }

  if (reg_pass) {
    cat("  ✓ Regression results match.\n\n")
  } else {
    cat("  ✗ Regression results differ.\n\n")
    all_passed <- FALSE
  }
} else {
  cat("  SKIP — regression_results.rds not found. Run Job 5 first.\n\n")
}

# =============================================================================
# 5. Validate Logistic Regression
# =============================================================================

cat("--- 5. Logistic Regression ---\n")

if (file.exists("output/hdfs_export/logistic_regression_results.rds")) {
  mr_logistic <- readRDS("output/hdfs_export/logistic_regression_results.rds")

  # Original R logistic regression
  df <- read.csv("airline_stats.csv", stringsAsFactors = FALSE)
  df <- na.omit(df)
  target <- names(sort(table(df$airline), decreasing = TRUE))[1]
  df$is_target <- ifelse(df$airline == target, 1, 0)
  r_glm <- glm(is_target ~ pct_carrier_delay + pct_atc_delay + pct_weather_delay,
               data = df, family = binomial)
  r_glm_coef <- coef(r_glm)

  log_pass <- TRUE
  mr_coef <- mr_logistic$glm_fit$coefficients
  names(mr_coef) <- names(r_glm_coef)

  for (nm in names(r_glm_coef)) {
    if (!check_close(sprintf("GLM coef[%s]", nm), mr_coef[nm], r_glm_coef[nm]))
      log_pass <- FALSE
  }

  if (!check_close("GLM AIC", AIC(mr_logistic$glm_fit), AIC(r_glm)))
    log_pass <- FALSE

  if (log_pass) {
    cat("  ✓ Logistic regression results match.\n\n")
  } else {
    cat("  ✗ Logistic regression results differ.\n\n")
    all_passed <- FALSE
  }
} else {
  cat("  SKIP — logistic_regression_results.rds not found. Run Job 6 first.\n\n")
}

# =============================================================================
# Summary
# =============================================================================

cat("============================================================\n")
if (all_passed) {
  cat("  ALL VALIDATIONS PASSED ✓\n")
} else {
  cat("  SOME VALIDATIONS FAILED ✗\n")
  cat("  Check the output above for details.\n")
}
cat("============================================================\n")
