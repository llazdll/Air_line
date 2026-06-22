#!/usr/bin/env Rscript
# =============================================================================
# Job 5: Linear Regression via MapReduce
# =============================================================================
# Computes OLS sufficient statistics (X'X, X'y, y'y, n) via MapReduce,
# then performs inference locally in R.
#
# Models:
#   1. Simple:   carrier ~ atc
#   2. Multiple:  carrier ~ atc + weather
#   3. With airline: carrier ~ atc + weather + airline (factor)
#   4. Interaction:  carrier ~ atc * airline
#
# Map:   For each row, compute local X'X (outer product), X'y, y'y, n
# Reduce: Sum all local contributions to get global X'X, X'y, y'y, n
# Local: beta = (X'X)^{-1} X'y, SE, t-stats, p-values, R², AIC, VIF
# =============================================================================

source("src/lib/mr_helpers.R")
library(rhdfs)
hdfs.init()

section_header("Job 5: Linear Regression (MapReduce)")

ensure_local_dir("output/plots")

# =============================================================================
# Helper: Build X matrix row for different model specifications
# =============================================================================

# Get unique airlines for factor encoding
get_airline_levels <- function() {
  df <- read.csv("airline_stats.csv", stringsAsFactors = FALSE)
  sort(unique(df$airline))
}

airline_levels <- get_airline_levels()
n_airlines <- length(airline_levels)

cat(sprintf("  Airlines in dataset: %d\n", n_airlines))

# =============================================================================
# Model 1: carrier ~ atc (Simple Linear Regression)
# =============================================================================

cat("\n--- Model 1: carrier ~ atc ---\n")

map_lm_simple <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  y  <- row$pct_carrier_delay
  x1 <- row$pct_atc_delay
  if (any(is.na(c(y, x1)))) return(NULL)

  # X = [1, x1]
  X <- c(1, x1)
  keyval("stats", list(
    XtX = outer(X, X),
    Xty = X * y,
    yty = y * y,
    n   = 1
  ))
}

reduce_lm <- function(k, vv) {
  total_XtX <- Reduce("+", lapply(vv, `[[`, "XtX"))
  total_Xty <- Reduce("+", lapply(vv, `[[`, "Xty"))
  total_yty <- sum(sapply(vv, `[[`, "yty"))
  total_n   <- sum(sapply(vv, `[[`, "n"))
  keyval(k, list(XtX = total_XtX, Xty = total_Xty, yty = total_yty, n = total_n))
}

run_mr_job(
  input     = HDFS_INPUT,
  output    = file.path(HDFS_BASE, "output", "regression", "model1"),
  map_fn    = map_lm_simple,
  reduce_fn = reduce_lm
)

m1_results <- pull_mr_results(file.path(HDFS_BASE, "output", "regression", "model1"))

if (!is.null(m1_results)) {
  stats <- m1_results$val[[1]]
  compute_lm_stats <- function(stats, model_name) {
    XtX <- stats$XtX
    Xty <- stats$Xty
    yty <- stats$yty
    n   <- stats$n
    p   <- length(Xty)

    beta_hat <- solve(XtX, Xty)
    residual_ss <- yty - t(Xty) %*% beta_hat
    sigma_sq <- residual_ss / (n - p)
    var_beta <- as.numeric(sigma_sq) * solve(XtX)
    se_beta <- sqrt(diag(var_beta))
    t_stats <- beta_hat / se_beta
    p_values <- 2 * pt(-abs(t_stats), df = n - p)

    # R-squared
    y_mean_sq <- (sum(XtX[1, -1]) / n)^2  # correction: use proper total SS
    total_ss <- yty - (Xty[1]^2) / n  # yty - (sum y)^2 / n
    r_squared <- 1 - residual_ss / total_ss
    adj_r_squared <- 1 - (1 - r_squared) * (n - 1) / (n - p)

    # AIC
    aic <- n * log(residual_ss / n) + 2 * p

    cat(sprintf("\n  %s:\n", model_name))
    cat(sprintf("    n = %d, parameters = %d\n", n, p))
    cat(sprintf("    Coefficients:\n"))
    coef_names <- names(beta_hat)
    if (is.null(coef_names)) coef_names <- paste0("b", seq_along(beta_hat))
    for (i in seq_along(beta_hat)) {
      cat(sprintf("      %-20s = %8.4f (SE = %.4f, t = %.4f, p = %.6f)\n",
                  coef_names[i], beta_hat[i], se_beta[i], t_stats[i], p_values[i]))
    }
    cat(sprintf("    R² = %.4f, Adjusted R² = %.4f\n", r_squared, adj_r_squared))
    cat(sprintf("    AIC = %.2f\n", aic))
    cat(sprintf("    Residual SS = %.4f\n", residual_ss))

    list(beta = beta_hat, se = se_beta, t = t_stats, p = p_values,
         r_squared = r_squared, adj_r_squared = adj_r_squared,
         aic = aic, n = n, p = p, sigma_sq = as.numeric(sigma_sq),
         XtX = XtX, residual_ss = residual_ss, total_ss = total_ss)
  }

  m1_stats <- compute_lm_stats(stats, "Model 1: carrier ~ atc")
}

# =============================================================================
# Model 2: carrier ~ atc + weather (Multiple Regression)
# =============================================================================

cat("\n--- Model 2: carrier ~ atc + weather ---\n")

map_lm_multiple <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  y  <- row$pct_carrier_delay
  x1 <- row$pct_atc_delay
  x2 <- row$pct_weather_delay
  if (any(is.na(c(y, x1, x2)))) return(NULL)

  # X = [1, x1, x2]
  X <- c(1, x1, x2)
  keyval("stats", list(
    XtX = outer(X, X),
    Xty = X * y,
    yty = y * y,
    n   = 1
  ))
}

run_mr_job(
  input     = HDFS_INPUT,
  output    = file.path(HDFS_BASE, "output", "regression", "model2"),
  map_fn    = map_lm_multiple,
  reduce_fn = reduce_lm
)

m2_results <- pull_mr_results(file.path(HDFS_BASE, "output", "regression", "model2"))

if (!is.null(m2_results)) {
  m2_stats <- compute_lm_stats(m2_results$val[[1]], "Model 2: carrier ~ atc + weather")

  # VIF (Variance Inflation Factors)
  if (requireNamespace("car", quietly = TRUE)) {
    vif_values <- tryCatch(car::vif(lm(pct_carrier_delay ~ pct_atc_delay + pct_weather_delay,
                                        data = read.csv("airline_stats.csv", nrows = 100))),
                           error = function(e) NULL)
    if (!is.null(vif_values)) {
      cat("    VIF:\n")
      print(vif_values)
    }
  }
}

# =============================================================================
# Model 3: carrier ~ atc + weather + airline (with factor)
# =============================================================================

cat("\n--- Model 3: carrier ~ atc + weather + airline ---\n")

# For the airline factor model, we need to create dummy variables
# Reference level = first airline alphabetically
ref_airline <- airline_levels[1]
other_airlines <- airline_levels[-1]

map_lm_airline <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  y  <- row$pct_carrier_delay
  x1 <- row$pct_atc_delay
  x2 <- row$pct_weather_delay
  airline <- row$airline
  if (any(is.na(c(y, x1, x2))) || is.na(airline)) return(NULL)

  # Dummy variables for airline (reference = first level)
  dummies <- as.numeric(other_airlines == airline)
  # X = [1, x1, x2, dummy1, dummy2, ...]
  X <- c(1, x1, x2, dummies)
  keyval("stats", list(
    XtX = outer(X, X),
    Xty = X * y,
    yty = y * y,
    n   = 1
  ))
}

run_mr_job(
  input     = HDFS_INPUT,
  output    = file.path(HDFS_BASE, "output", "regression", "model3"),
  map_fn    = map_lm_airline,
  reduce_fn = reduce_lm
)

m3_results <- pull_mr_results(file.path(HDFS_BASE, "output", "regression", "model3"))

if (!is.null(m3_results)) {
  m3_stats <- compute_lm_stats(m3_results$val[[1]],
                                "Model 3: carrier ~ atc + weather + airline")
}

# =============================================================================
# Model Comparison (AIC)
# =============================================================================

cat("\n--- Model Comparison ---\n")
model_aic <- data.frame(
  Model = c("1: carrier ~ atc",
            "2: carrier ~ atc + weather",
            "3: carrier ~ atc + weather + airline"),
  AIC = c(m1_stats$aic, m2_stats$aic, m3_stats$aic),
  R_squared = c(m1_stats$r_squared, m2_stats$r_squared, m3_stats$r_squared),
  Adj_R_squared = c(m1_stats$adj_r_squared, m2_stats$adj_r_squared, m3_stats$adj_r_squared),
  stringsAsFactors = FALSE
)
model_aic <- model_aic[order(model_aic$AIC), ]
print(model_aic, row.names = FALSE)
cat(sprintf("\n  Best model (lowest AIC): %s\n", model_aic$Model[1]))

# =============================================================================
# Residuals and Diagnostic Plots
# =============================================================================

cat("\n--- Computing residuals (MapReduce) ---\n")

# Use Model 2 (multiple regression) for diagnostics
beta_m2 <- m2_stats$beta

map_residuals <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  y  <- row$pct_carrier_delay
  x1 <- row$pct_atc_delay
  x2 <- row$pct_weather_delay
  if (any(is.na(c(y, x1, x2)))) return(NULL)

  X <- c(1, x1, x2)
  y_hat <- sum(X * beta_m2)
  residual <- y - y_hat

  keyval("residuals", c(y = y, y_hat = y_hat, residual = residual,
                         residual_sq = residual^2))
}

reduce_residuals <- function(k, vv) {
  vv_mat <- do.call(rbind, vv)
  keyval(k, list(
    n          = nrow(vv_mat),
    sum_y      = sum(vv_mat[, "y"]),
    sum_y_hat  = sum(vv_mat[, "y_hat"]),
    sum_resid  = sum(vv_mat[, "residual"]),
    sum_resid2 = sum(vv_mat[, "residual_sq"])
  ))
}

run_mr_job(
  input     = HDFS_INPUT,
  output    = file.path(HDFS_BASE, "output", "regression_residuals"),
  map_fn    = map_residuals,
  reduce_fn = reduce_residuals
)

resid_results <- pull_mr_results(file.path(HDFS_BASE, "output", "regression_residuals"))

if (!is.null(resid_results)) {
  r <- resid_results$val[[1]]
  cat(sprintf("  Sum of residuals: %.6f (should be ~0)\n", r$sum_resid))
  cat(sprintf("  Sum of squared residuals: %.4f\n", r$sum_resid2))
}

# --- Diagnostic Plots (Local R) ---
cat("\n--- Generating diagnostic plots ---\n")

# Read a sample for plotting
set.seed(42)
df_full <- read.csv("airline_stats.csv", stringsAsFactors = FALSE)
df_full <- na.omit(df_full)
df_full$airline <- as.factor(df_full$airline)

# Fit model locally for diagnostic plots
lm_fit <- lm(pct_carrier_delay ~ pct_atc_delay + pct_weather_delay, data = df_full)

# 4-panel diagnostic plot
png("output/plots/regression_diagnostics.png", width = 1000, height = 800)
par(mfrow = c(2, 2))
plot(lm_fit, which = 1:4)
dev.off()
cat("  Saved: output/plots/regression_diagnostics.png\n")

# Regression plot with per-airline lines
if (requireNamespace("ggplot2", quietly = TRUE)) {
  p_reg <- ggplot2::ggplot(df_full, ggplot2::aes(x = pct_atc_delay, y = pct_carrier_delay,
                                                   color = airline)) +
    ggplot2::geom_point(alpha = 0.1, size = 0.5) +
    ggplot2::geom_smooth(method = "lm", se = FALSE, linewidth = 0.5) +
    ggplot2::geom_smooth(method = "lm", se = FALSE,
                         color = "black", linewidth = 1.2,
                         linetype = "dashed", aes(group = 1)) +
    ggplot2::labs(title = "Linear Regression: Carrier Delay vs ATC Delay",
                  subtitle = "Per-airline lines + global trend (dashed)",
                  x = "ATC Delay (%)", y = "Carrier Delay (%)",
                  color = "Airline") +
    ggplot2::theme_minimal()

  ggplot2::ggsave("output/plots/regression_plot.png", p_reg,
                  width = 10, height = 7, dpi = 150)
  cat("  Saved: output/plots/regression_plot.png\n")
}

# =============================================================================
# Save Results
# =============================================================================

regression_results <- list(
  model1 = m1_stats,
  model2 = m2_stats,
  model3 = m3_stats,
  model_comparison = model_aic,
  best_model = model_aic$Model[1]
)

ensure_local_dir("output/hdfs_export")
saveRDS(regression_results, "output/hdfs_export/regression_results.rds")
write.csv(model_aic, "output/hdfs_export/regression_model_comparison.csv", row.names = FALSE)

cat("\n  Results saved to output/hdfs_export/\n")
cat("  Job 5 complete.\n")
