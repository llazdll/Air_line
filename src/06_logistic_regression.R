#!/usr/bin/env Rscript
# =============================================================================
# Job 6: Logistic Regression (Hybrid: MapReduce + Local R)
# =============================================================================
# Part A: Data Preparation via MapReduce
#   - Determine the most frequent airline (target class)
#   - Create binary outcome: is_target vs others
#   - Extract relevant columns
#
# Part B: Logistic Regression (Local R)
#   - Fit glm() with binomial family on prepared data
#   - Odds ratios, confidence intervals
#   - Hosmer-Lemeshow goodness-of-fit test
#   - Classification table, accuracy, sensitivity, specificity
#   - ROC curve and AUC
# =============================================================================

source("src/lib/mr_helpers.R")
library(rhdfs)
hdfs.init()

section_header("Job 6: Logistic Regression (MapReduce + Local R)")

ensure_local_dir("output/plots")

# =============================================================================
# Part A: Data Preparation via MapReduce
# =============================================================================

cat("\n--- Part A: Data preparation (MapReduce) ---\n")

# Step 1: Count flights per airline to find the most frequent
map_count_airlines <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  airline <- row$airline
  if (is.na(airline)) return(NULL)

  keyval(airline, 1)
}

reduce_count_airlines <- function(k, vv) {
  keyval(k, sum(unlist(vv)))
}

run_mr_job(
  input     = HDFS_INPUT,
  output    = file.path(HDFS_BASE, "output", "logistic_prep", "counts"),
  map_fn    = map_count_airlines,
  reduce_fn = reduce_count_airlines
)

count_results <- pull_mr_results(file.path(HDFS_BASE, "output", "logistic_prep", "counts"))

if (is.null(count_results)) {
  stop("Failed to count airlines.")
}

# Find most frequent airline
airline_counts <- data.frame(
  airline = count_results$key,
  count   = sapply(count_results$val, function(x) x),
  stringsAsFactors = FALSE
)
airline_counts <- airline_counts[order(-airline_counts$count), ]

cat("  Airline counts:\n")
print(airline_counts, row.names = FALSE)

target_airline <- airline_counts$airline[1]
cat(sprintf("\n  Target airline (most frequent): %s (n = %d)\n",
            target_airline, airline_counts$count[1]))

# Step 2: Extract data with binary outcome
cat("\n  Extracting data with binary outcome...\n")

map_logistic_extract <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  carrier <- row$pct_carrier_delay
  atc     <- row$pct_atc_delay
  weather <- row$pct_weather_delay
  airline <- row$airline

  if (any(is.na(c(carrier, atc, weather))) || is.na(airline)) return(NULL)

  is_target <- ifelse(airline == target_airline, 1, 0)

  # Emit with a constant key to collect all rows
  keyval("data", c(is_target = is_target, carrier = carrier,
                    atc = atc, weather = weather))
}

reduce_logistic_extract <- function(k, vv) {
  keyval(k, do.call(rbind, vv))
}

run_mr_job(
  input     = HDFS_INPUT,
  output    = file.path(HDFS_BASE, "output", "logistic_prep", "extracted"),
  map_fn    = map_logistic_extract,
  reduce_fn = reduce_logistic_extract
)

extracted_results <- pull_mr_results(file.path(HDFS_BASE, "output", "logistic_prep", "extracted"))

if (is.null(extracted_results)) {
  stop("Failed to extract logistic regression data.")
}

# Build data frame
logistic_data <- as.data.frame(extracted_results$val[[1]])
logistic_data$is_target <- as.integer(logistic_data$is_target)

cat(sprintf("  Extracted %d rows\n", nrow(logistic_data)))
cat(sprintf("  Target airline (%s): %d (%.1f%%)\n",
            target_airline,
            sum(logistic_data$is_target),
            100 * mean(logistic_data$is_target)))

# =============================================================================
# Part B: Logistic Regression (Local R)
# =============================================================================

cat("\n--- Part B: Logistic Regression (Local R) ---\n")

# Fit logistic regression
glm_fit <- glm(is_target ~ carrier + atc + weather,
               data = logistic_data,
               family = binomial(link = "logit"))

cat("\n  Model Summary:\n")
print(summary(glm_fit))

# Check for complete separation
if (any(abs(coef(glm_fit)) > 10)) {
  cat("\n  WARNING: Large coefficients detected — possible complete separation.\n")
}

# Odds ratios and 95% CI
cat("\n  Odds Ratios and 95% Confidence Intervals:\n")
ci <- confint(glm_fit)
or_table <- data.frame(
  Variable = names(coef(glm_fit)),
  OR       = round(exp(coef(glm_fit)), 4),
  CI_lower = round(exp(ci[, 1]), 4),
  CI_upper = round(exp(ci[, 2]), 4),
  stringsAsFactors = FALSE
)
print(or_table, row.names = FALSE)

# Null and residual deviance
cat(sprintf("\n  Null deviance: %.2f on %d df\n",
            glm_fit$null.deviance, glm_fit$df.null))
cat(sprintf("  Residual deviance: %.2f on %d df\n",
            glm_fit$deviance, glm_fit$df.residual))
cat(sprintf("  AIC: %.2f\n", glm_fit$aic))

# =============================================================================
# Hosmer-Lemeshow Goodness-of-Fit Test
# =============================================================================

cat("\n  Hosmer-Lemeshow Goodness-of-Fit Test:\n")

hosmer_lemeshow <- function(y, y_hat, g = 10) {
  # Create deciles of predicted probabilities
  cut_points <- quantile(y_hat, probs = seq(0, 1, length.out = g + 1))
  cut_points[1] <- 0
  cut_points[g + 1] <- 1

  groups <- cut(y_hat, breaks = cut_points, include.lowest = TRUE,
                labels = FALSE)

  # Handle sparse groups
  while (any(table(groups) < 5) && g > 3) {
    g <- g - 1
    cut_points <- quantile(y_hat, probs = seq(0, 1, length.out = g + 1))
    cut_points[1] <- 0
    cut_points[g + 1] <- 1
    groups <- cut(y_hat, breaks = cut_points, include.lowest = TRUE,
                  labels = FALSE)
  }

  observed_0 <- tapply(1 - y, groups, sum)
  observed_1 <- tapply(y, groups, sum)
  expected_0 <- tapply(1 - y_hat, groups, sum)
  expected_1 <- tapply(y_hat, groups, sum)

  # Remove any NA groups
  valid <- complete.cases(observed_0, observed_1, expected_0, expected_1)
  observed_0 <- observed_0[valid]
  observed_1 <- observed_1[valid]
  expected_0 <- expected_0[valid]
  expected_1 <- expected_1[valid]

  chi_sq <- sum((observed_0 - expected_0)^2 / expected_0 +
                (observed_1 - expected_1)^2 / expected_1)
  df <- g - 2
  p_value <- pchisq(chi_sq, df, lower.tail = FALSE)

  list(chi_sq = chi_sq, df = df, p_value = p_value, g = g)
}

y_pred_prob <- predict(glm_fit, type = "response")
hl_test <- hosmer_lemeshow(logistic_data$is_target, y_pred_prob)

cat(sprintf("  Chi-squared = %.4f, df = %d, p-value = %.6f\n",
            hl_test$chi_sq, hl_test$df, hl_test$p_value))
if (hl_test$p_value > 0.05) {
  cat("  Result: Model fits well (p > 0.05, fail to reject H0)\n")
} else {
  cat("  Result: Model may not fit well (p <= 0.05)\n")
}

# =============================================================================
# Classification Table
# =============================================================================

cat("\n  Classification Table (threshold = 0.5):\n")

threshold <- 0.5
y_pred_class <- as.integer(y_pred_prob >= threshold)

TP <- sum(y_pred_class == 1 & logistic_data$is_target == 1)
TN <- sum(y_pred_class == 0 & logistic_data$is_target == 0)
FP <- sum(y_pred_class == 1 & logistic_data$is_target == 0)
FN <- sum(y_pred_class == 0 & logistic_data$is_target == 1)

accuracy    <- (TP + TN) / (TP + TN + FP + FN)
sensitivity <- TP / (TP + FN)  # True positive rate
specificity <- TN / (TN + FP)  # True negative rate
PPV         <- TP / (TP + FP)  # Positive predictive value
NPV         <- TN / (TN + FN)  # Negative predictive value

cat(sprintf("    TP = %d, TN = %d, FP = %d, FN = %d\n", TP, TN, FP, FN))
cat(sprintf("    Accuracy    = %.4f\n", accuracy))
cat(sprintf("    Sensitivity = %.4f\n", sensitivity))
cat(sprintf("    Specificity = %.4f\n", specificity))
cat(sprintf("    PPV         = %.4f\n", PPV))
cat(sprintf("    NPV         = %.4f\n", NPV))

# =============================================================================
# ROC Curve and AUC
# =============================================================================

cat("\n  Computing ROC curve and AUC...\n")

# Manual ROC computation using trapezoidal integration
roc_compute <- function(y_true, y_prob, n_thresholds = 100) {
  thresholds <- seq(0, 1, length.out = n_thresholds)

  tpr <- numeric(n_thresholds)  # Sensitivity
  fpr <- numeric(n_thresholds)  # 1 - Specificity

  for (i in seq_along(thresholds)) {
    pred <- as.integer(y_prob >= thresholds[i])
    tp <- sum(pred == 1 & y_true == 1)
    fp <- sum(pred == 1 & y_true == 0)
    fn <- sum(pred == 0 & y_true == 1)
    tn <- sum(pred == 0 & y_true == 0)

    tpr[i] <- ifelse(tp + fn > 0, tp / (tp + fn), 0)
    fpr[i] <- ifelse(fp + tn > 0, fp / (fp + tn), 0)
  }

  # Sort by FPR for proper AUC calculation
  ord <- order(fpr)
  fpr <- fpr[ord]
  tpr <- tpr[ord]

  # Trapezoidal integration for AUC
  auc <- sum(diff(fpr) * (head(tpr, -1) + tail(tpr, -1)) / 2)

  list(fpr = fpr, tpr = tpr, auc = auc)
}

roc_result <- roc_compute(logistic_data$is_target, y_pred_prob)
cat(sprintf("  AUC = %.4f\n", roc_result$auc))

# Plot ROC curve
if (requireNamespace("ggplot2", quietly = TRUE)) {
  roc_df <- data.frame(FPR = roc_result$fpr, TPR = roc_result$tpr)

  p_roc <- ggplot2::ggplot(roc_df, ggplot2::aes(x = FPR, y = TPR)) +
    ggplot2::geom_line(color = "blue", linewidth = 1) +
    ggplot2::geom_abline(intercept = 0, slope = 1, linetype = "dashed",
                         color = "gray50") +
    ggplot2::labs(title = sprintf("ROC Curve — Logistic Regression (AUC = %.4f)", roc_result$auc),
                  x = "False Positive Rate (1 - Specificity)",
                  y = "True Positive Rate (Sensitivity)") +
    ggplot2::theme_minimal() +
    ggplot2::coord_equal()

  ggplot2::ggsave("output/plots/roc_curve.png", p_roc,
                  width = 7, height = 7, dpi = 150)
  cat("  Saved: output/plots/roc_curve.png\n")
}

# =============================================================================
# Logistic Regression Probability Plot
# =============================================================================

cat("\n  Generating logistic probability plot...\n")

if (requireNamespace("ggplot2", quietly = TRUE)) {
  # Plot P(target) vs ATC delay, with other predictors at their means
  atc_range <- seq(min(logistic_data$atc), max(logistic_data$atc),
                   length.out = 200)
  newdata <- data.frame(
    carrier = mean(logistic_data$carrier),
    atc     = atc_range,
    weather = mean(logistic_data$weather)
  )
  prob_pred <- predict(glm_fit, newdata = newdata, type = "response")

  prob_df <- data.frame(atc_delay = atc_range, probability = prob_pred)

  p_logistic <- ggplot2::ggplot(prob_df,
                                 ggplot2::aes(x = atc_delay, y = probability)) +
    ggplot2::geom_line(color = "red", linewidth = 1) +
    ggplot2::labs(
      title = sprintf("P(%s) vs ATC Delay", target_airline),
      subtitle = sprintf("Carrier delay = %.2f%%, Weather delay = %.2f%% (held at means)",
                         mean(logistic_data$carrier), mean(logistic_data$weather)),
      x = "ATC Delay (%)",
      y = sprintf("Probability of %s", target_airline)
    ) +
    ggplot2::theme_minimal()

  ggplot2::ggsave("output/plots/logistic_regression_plot.png", p_logistic,
                  width = 8, height = 5, dpi = 150)
  cat("  Saved: output/plots/logistic_regression_plot.png\n")
}

# =============================================================================
# Save Results
# =============================================================================

logistic_results <- list(
  target_airline = target_airline,
  airline_counts = airline_counts,
  glm_fit        = glm_fit,
  odds_ratios    = or_table,
  hosmer_lemeshow = hl_test,
  classification  = list(
    accuracy = accuracy, sensitivity = sensitivity,
    specificity = specificity, PPV = PPV, NPV = NPV,
    TP = TP, TN = TN, FP = FP, FN = FN
  ),
  auc = roc_result$auc
)

ensure_local_dir("output/hdfs_export")
saveRDS(logistic_results, "output/hdfs_export/logistic_regression_results.rds")

cat("\n  Results saved to output/hdfs_export/\n")
cat("  Job 6 complete.\n")
