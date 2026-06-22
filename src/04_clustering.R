#!/usr/bin/env Rscript
# =============================================================================
# Job 4: Clustering Analysis (Hybrid: MapReduce + Local R)
# =============================================================================
# Part A: Airline-Level Clustering
#   - MapReduce to compute mean delay profiles per airline
#   - Local R for hierarchical clustering and k-means
#
# Part B: Flight-Level Clustering (sampled)
#   - MapReduce to extract a random sample of flights
#   - Local R for k-means on the sample
#
# Part C: Heatmap of airline delay profiles
#   - Uses airline means from Part A
#   - Local R for heatmap visualization
# =============================================================================

source("src/lib/mr_helpers.R")
library(rhdfs)
hdfs.init()

section_header("Job 4: Clustering Analysis (MapReduce + Local R)")

ensure_local_dir("output/plots")

# =============================================================================
# Part A: Airline-Level Clustering
# =============================================================================

cat("\n--- Part A: Computing airline mean profiles (MapReduce) ---\n")

map_airline_profiles <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  airline <- row$airline
  carrier <- row$pct_carrier_delay
  atc     <- row$pct_atc_delay
  weather <- row$pct_weather_delay

  if (is.na(airline) || any(is.na(c(carrier, atc, weather)))) return(NULL)

  keyval(airline, c(n = 1, carrier = carrier, atc = atc, weather = weather))
}

reduce_airline_profiles <- function(k, vv) {
  vv_mat <- do.call(rbind, vv)
  n <- sum(vv_mat[, "n"])
  keyval(k, c(
    n = n,
    avg_carrier = sum(vv_mat[, "carrier"]) / n,
    avg_atc     = sum(vv_mat[, "atc"]) / n,
    avg_weather = sum(vv_mat[, "weather"]) / n
  ))
}

output_airline_profiles <- file.path(HDFS_BASE, "output", "clustering_airlines")

run_mr_job(
  input     = HDFS_INPUT,
  output    = output_airline_profiles,
  map_fn    = map_airline_profiles,
  reduce_fn = reduce_airline_profiles
)

profile_results <- pull_mr_results(output_airline_profiles)

if (is.null(profile_results)) {
  stop("Failed to compute airline profiles.")
}

# Build airline profile matrix
n_airlines <- length(profile_results$key)
airline_names <- profile_results$key
profile_matrix <- matrix(0, nrow = n_airlines, ncol = 3)
rownames(profile_matrix) <- airline_names
colnames(profile_matrix) <- c("avg_carrier", "avg_atc", "avg_weather")

for (i in seq_along(profile_results$key)) {
  v <- profile_results$val[[i]]
  profile_matrix[i, ] <- c(v[["avg_carrier"]], v[["avg_atc"]], v[["avg_weather"]])
}

cat("  Airline mean delay profiles:\n")
print(round(profile_matrix, 4))

# --- Hierarchical Clustering ---
cat("\n  Hierarchical clustering (Ward's D)...\n")

# Scale the profiles
profile_scaled <- scale(profile_matrix)
dist_matrix <- dist(profile_scaled, method = "euclidean")
hc <- hclust(dist_matrix, method = "ward.D2")

# Plot dendrogram
if (requireNamespace("factoextra", quietly = TRUE)) {
  p_dendro <- factoextra::fviz_dend(hc, k = 6,
                                     cex = 0.8,
                                     main = "Hierarchical Clustering of Airlines",
                                     xlab = "Airline", ylab = "Height")
  ggplot2::ggsave("output/plots/airline_dendrogram.png", p_dendro,
                  width = 10, height = 6, dpi = 150)
  cat("  Saved: output/plots/airline_dendrogram.png\n")
} else {
  png("output/plots/airline_dendrogram.png", width = 1000, height = 600)
  plot(hc, main = "Hierarchical Clustering of Airlines (Ward's D)",
       xlab = "Airline", ylab = "Height", cex = 0.8)
  rect.hclust(hc, k = 6, border = 2:7)
  dev.off()
  cat("  Saved: output/plots/airline_dendrogram.png\n")
}

# --- K-Means Clustering ---
cat("\n  K-means clustering...\n")

# Determine optimal k
max_k <- min(5, n_airlines - 1)

if (requireNamespace("factoextra", quietly = TRUE) && max_k >= 2) {
  # Elbow plot
  set.seed(42)
  elbow <- factoextra::fviz_nbclust(profile_scaled, kmeans, method = "wss",
                                     k.max = max_k) +
    ggplot2::ggtitle("Elbow Method for Optimal k")
  ggplot2::ggsave("output/plots/elbow_plot.png", elbow,
                  width = 8, height = 5, dpi = 150)
  cat("  Saved: output/plots/elbow_plot.png\n")

  # Silhouette plot
  sil <- factoextra::fviz_nbclust(profile_scaled, kmeans, method = "silhouette",
                                   k.max = max_k) +
    ggplot2::ggtitle("Silhouette Method for Optimal k")
  ggplot2::ggsave("output/plots/silhouette_plot.png", sil,
                  width = 8, height = 5, dpi = 150)
  cat("  Saved: output/plots/silhouette_plot.png\n")
}

# Run k-means with k=3 (or fewer if not enough airlines)
k_optimal <- min(3, max_k)
set.seed(42)
km_result <- kmeans(profile_scaled, centers = k_optimal, nstart = 25)

cat(sprintf("  K-means with k = %d:\n", k_optimal))
cat("  Cluster assignments:\n")
cluster_df <- data.frame(
  airline = airline_names,
  cluster = km_result$cluster,
  stringsAsFactors = FALSE
)
print(cluster_df[order(cluster_df$cluster), ], row.names = FALSE)

cat(sprintf("\n  Between-SS / Total-SS = %.2f%%\n",
            100 * km_result$betweenss / km_result$totss))

# K-means visualization
if (requireNamespace("factoextra", quietly = TRUE)) {
  p_kmeans <- factoextra::fviz_cluster(km_result, data = profile_scaled,
                                        geom = "text",
                                        main = "K-Means Clustering of Airlines",
                                        xlab = "PC1", ylab = "PC2")
  ggplot2::ggsave("output/plots/kmeans_airlines.png", p_kmeans,
                  width = 8, height = 6, dpi = 150)
  cat("  Saved: output/plots/kmeans_airlines.png\n")
}

# =============================================================================
# Part B: Flight-Level Clustering (Sampled)
# =============================================================================

cat("\n--- Part B: Flight-level clustering (sampled, MapReduce) ---\n")

# Extract a sample of flights via MapReduce
# We use a simple approach: emit all rows, then sample locally
# For a true random sample in MapReduce, we'd use a probabilistic filter

map_sample_flights <- function(k, line) {
  row <- parse_csv_line(line)
  if (is.null(row)) return(NULL)

  carrier <- row$pct_carrier_delay
  atc     <- row$pct_atc_delay
  weather <- row$pct_weather_delay
  airline <- row$airline

  if (any(is.na(c(carrier, atc, weather))) return(NULL)

  # Emit with a random key for distribution; we'll sample locally
  keyval(airline, c(carrier = carrier, atc = atc, weather = weather))
}

reduce_sample_flights <- function(k, vv) {
  # Collect all values; we'll sample from the pulled results
  keyval(k, do.call(rbind, vv))
}

output_sample <- file.path(HDFS_BASE, "output", "clustering_sample")

run_mr_job(
  input     = HDFS_INPUT,
  output    = output_sample,
  map_fn    = map_sample_flights,
  reduce_fn = reduce_sample_flights
)

sample_results <- pull_mr_results(output_sample)

if (!is.null(sample_results)) {
  # Combine all flight data
  all_flights <- do.call(rbind, lapply(sample_results$val, function(v) {
    if (is.matrix(v)) v else matrix(v, nrow = 1)
  }))
  colnames(all_flights) <- c("carrier", "atc", "weather")

  # Sample 5000 flights
  set.seed(42)
  sample_size <- min(5000, nrow(all_flights))
  sample_idx <- sample(nrow(all_flights), sample_size)
  flight_sample <- all_flights[sample_idx, ]

  cat(sprintf("  Total flights collected: %d\n", nrow(all_flights)))
  cat(sprintf("  Sample size: %d\n", sample_size))

  # Scale and run k-means
  flight_scaled <- scale(flight_sample)
  set.seed(42)
  km_flights <- kmeans(flight_scaled, centers = 6, nstart = 25)

  cat("  K-means (k=6) on flight sample:\n")
  cat(sprintf("  Between-SS / Total-SS = %.2f%%\n",
              100 * km_flights$betweenss / km_flights$totss))

  # Summarize clusters
  cluster_summary <- data.frame(
    cluster = 1:6,
    size = km_flights$size,
    avg_carrier = round(km_flights$centers[, "carrier"], 4),
    avg_atc = round(km_flights$centers[, "atc"], 4),
    avg_weather = round(km_flights$centers[, "weather"], 4)
  )
  cat("\n  Cluster summary:\n")
  print(cluster_summary, row.names = FALSE)

  # Visualize
  if (requireNamespace("factoextra", quietly = TRUE)) {
    p_km_flights <- factoextra::fviz_cluster(km_flights, data = flight_scaled,
                                              geom = "point", stand = FALSE,
                                              main = "K-Means Clustering of Flights (Sample)",
                                              xlab = "PC1", ylab = "PC2")
    ggplot2::ggsave("output/plots/kmeans_flights.png", p_km_flights,
                    width = 8, height = 6, dpi = 150)
    cat("  Saved: output/plots/kmeans_flights.png\n")
  }
}

# =============================================================================
# Part C: Heatmap
# =============================================================================

cat("\n--- Part C: Heatmap of airline delay profiles ---\n")

if (requireNamespace("ggplot2", quietly = TRUE) && requireNamespace("tidyr", quietly = TRUE)) {
  heatmap_df <- as.data.frame(profile_matrix)
  heatmap_df$airline <- rownames(profile_matrix)
  heatmap_long <- tidyr::pivot_longer(heatmap_df,
                                       cols = c("avg_carrier", "avg_atc", "avg_weather"),
                                       names_to = "delay_type",
                                       values_to = "percentage")
  heatmap_long$delay_type <- factor(heatmap_long$delay_type,
                                     levels = c("avg_carrier", "avg_atc", "avg_weather"),
                                     labels = c("Carrier Delay", "ATC Delay", "Weather Delay"))

  p_heatmap <- ggplot2::ggplot(heatmap_long,
                                ggplot2::aes(x = delay_type, y = airline, fill = percentage)) +
    ggplot2::geom_tile(color = "white") +
    ggplot2::scale_fill_gradient(low = "white", fill = "steelblue") +
    ggplot2::geom_text(ggplot2::aes(label = sprintf("%.2f", percentage)), size = 3) +
    ggplot2::labs(title = "Average Delay Percentages by Airline",
                  x = "Delay Type", y = "Airline", fill = "%") +
    ggplot2::theme_minimal() +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1))

  ggplot2::ggsave("output/plots/airline_heatmap.png", p_heatmap,
                  width = 8, height = 8, dpi = 150)
  cat("  Saved: output/plots/airline_heatmap.png\n")
}

# =============================================================================
# Save Results
# =============================================================================

clustering_results <- list(
  airline_profiles = profile_matrix,
  hc = hc,
  kmeans_airlines = km_result,
  kmeans_flights = if(exists("km_flights")) km_flights else NULL
)

ensure_local_dir("output/hdfs_export")
saveRDS(clustering_results, "output/hdfs_export/clustering_results.rds")

cat("\n  Results saved to output/hdfs_export/\n")
cat("  Job 4 complete.\n")
