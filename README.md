# Airline Delay Analysis — Hadoop MapReduce Pipeline

A comprehensive statistical analysis of airline delay data using **Apache Hadoop** with **R** (RHadoop ecosystem). This project demonstrates how classical statistical methods can be implemented using the MapReduce paradigm.

## Overview

This project analyzes the `airline_stats.csv` dataset (~33,000 flights) to study delay patterns across airlines. The analysis is implemented as a series of MapReduce jobs using R, with data stored in HDFS.

### Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│ airline_     │────▶│  HDFS        │────▶│  MapReduce Jobs │
│ stats.csv   │     │  (Storage)   │     │  (Processing)   │
└─────────────┘     └──────────────┘     └────────┬────────┘
                                                   │
                                          ┌────────▼────────┐
                                          │  Local R         │
                                          │  (Inference &    │
                                          │   Visualization) │
                                          └────────┬────────┘
                                                   │
                                          ┌────────▼────────┐
                                          │  Results &       │
                                          │  Plots           │
                                          └─────────────────┘
```

**Core principle:** MapReduce for data preparation and aggregation; local R for statistical modeling and visualization.

### Analysis Pipeline

| Job | Analysis | Method |
|-----|----------|--------|
| 1 | Descriptive Statistics | Pure MapReduce (summable statistics) |
| 2 | ANOVA | Two-pass MapReduce for SSB/SSW, local F-test |
| 3 | T-Test & Wilcoxon | MapReduce for sufficient stats, local tests |
| 4 | Clustering | MapReduce for airline means, local hclust/kmeans |
| 5 | Linear Regression | MapReduce for X'X/X'y, local OLS inference |
| 6 | Logistic Regression | MapReduce for data prep, local glm() |
| 7 | Visualization | Local ggplot2 on HDFS-sourced data |

## Prerequisites

### Required Software

| Component | Version | Purpose |
|-----------|---------|---------|
| R | 3.5+ (4.x recommended) | Base language |
| Java JDK | 8 or 11 | Hadoop runtime |
| Apache Hadoop | 2.x or 3.x | HDFS + MapReduce (pseudo-distributed) |

### R Packages

- **CRAN:** `rJava`, `Rcpp`, `dplyr`, `ggplot2`, `tidyr`, `cluster`, `factoextra`, `car`, `dunn.test`, `broom`, `pheatmap`, `reshape2`, `bitops`, `caTools`
- **GitHub (RHadoop):** `rhdfs`, `rmr2` (from RevolutionAnalytics)

## Setup Instructions

### 1. Install Hadoop (Pseudo-Distributed Mode)

```bash
# Download and extract Hadoop
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
sudo mv hadoop-3.3.6 /usr/local/hadoop

# Configure environment
export HADOOP_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH

# Configure Hadoop (core-site.xml, hdfs-site.xml, etc.)
# See Hadoop documentation for pseudo-distributed setup

# Format HDFS
hdfs namenode -format

# Start Hadoop
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

# Verify
jps  # Should show NameNode, DataNode, ResourceManager, NodeManager
```

### 2. Configure Environment

```bash
cd /path/to/big_data_project
source config/hadoop_env.sh
```

Edit `config/hadoop_env.sh` to match your Hadoop and Java installation paths.

### 3. Install R Packages

```bash
Rscript setup/01_install_r_packages.R
```

### 4. Upload Data to HDFS

```bash
Rscript setup/02_hdfs_setup.R
```

This creates the HDFS directory structure and uploads `airline_stats.csv`.

## Running the Pipeline

### Full Pipeline (Hadoop Mode)

```bash
source config/hadoop_env.sh
Rscript run_all.R
```

### Local Mode (No Hadoop Required)

For testing and development without a Hadoop cluster:

```bash
Rscript run_all.R --local
```

This runs all MapReduce jobs locally in R (no HDFS needed).

### Running Individual Jobs

```bash
Rscript src/01_descriptive_stats.R
Rscript src/02_anova.R
Rscript src/03_ttest_wilcoxon.R
Rscript src/04_clustering.R
Rscript src/05_regression.R
Rscript src/06_logistic_regression.R
Rscript src/07_visualization.R
```

### Validation

After running the pipeline, validate results against the original R analysis:

```bash
Rscript tests/validate_results.R
```

## Project Structure

```
big_data_project/
├── airline_stats.csv              # Dataset (~33K flights, 4 columns)
├── README.md                      # This file
├── run_all.R                      # Master orchestration script
│
├── config/
│   └── hadoop_env.sh              # Environment variables (HADOOP_CMD, JAVA_HOME)
│
├── setup/
│   ├── 01_install_r_packages.R    # Install all R packages
│   └── 02_hdfs_setup.R            # HDFS directory setup + data upload
│
├── src/
│   ├── lib/
│   │   └── mr_helpers.R           # Shared MapReduce helper functions
│   ├── 01_descriptive_stats.R     # Job 1: Descriptive statistics
│   ├── 02_anova.R                 # Job 2: ANOVA (two-pass MapReduce)
│   ├── 03_ttest_wilcoxon.R        # Job 3: T-test, Wilcoxon, Kruskal-Wallis
│   ├── 04_clustering.R            # Job 4: Hierarchical + K-means clustering
│   ├── 05_regression.R            # Job 5: Linear regression (OLS via MR)
│   ├── 06_logistic_regression.R   # Job 6: Logistic regression (hybrid)
│   └── 07_visualization.R         # Job 7: All 14 plots
│
├── output/
│   ├── hdfs_export/               # Local copies of HDFS results (.rds, .csv)
│   └── plots/                     # Generated PNG plots (14 files)
│
└── tests/
    └── validate_results.R         # Numerical validation of MR results
```

## Output

### HDFS Output Directory

```
/user/ruser/airline_analysis/
├── input/
│   └── airline_stats.csv
└── output/
    ├── descriptive/               # Per-airline mean, sd, min, max
    ├── anova_pass1/               # Group means and counts
    ├── anova_pass2/               # Sum of squares (SSB, SSW)
    ├── ttest/                     # Sufficient statistics (Delta vs United)
    ├── wilcoxon/                  # Extracted Delta/United values
    ├── kruskal/                   # Per-airline carrier delay values
    ├── clustering_airlines/       # Airline mean delay profiles
    ├── clustering_sample/         # Sampled flight data
    ├── regression/
    │   ├── model1/                # X'X, X'y for simple LM
    │   ├── model2/                # X'X, X'y for multiple LM
    │   └── model3/                # X'X, X'y for LM with airline factor
    ├── regression_residuals/      # Residual statistics
    └── logistic_prep/
        ├── counts/                # Airline frequency counts
        └── extracted/             # Data with binary outcome
```

### Generated Plots (14 files)

| Plot | Description |
|------|-------------|
| `delay_distributions.png` | Boxplots of delay types by airline |
| `anova_carrier_delay.png` | ANOVA boxplot with jittered points |
| `wilcoxon_comparison.png` | Violin + boxplot (Delta vs United) |
| `dendrogram.png` | Hierarchical clustering (flight sample) |
| `airline_dendrogram.png` | Hierarchical clustering (airline means) |
| `elbow_plot.png` | Elbow method for optimal k |
| `silhouette_plot.png` | Silhouette method for optimal k |
| `kmeans_airlines.png` | K-means clusters (airline profiles) |
| `kmeans_flights.png` | K-means clusters (flight sample) |
| `airline_heatmap.png` | Heatmap of delay profiles |
| `regression_diagnostics.png` | 4-panel LM diagnostic plots |
| `regression_plot.png` | Regression scatterplot with per-airline lines |
| `logistic_regression_plot.png` | Logistic probability curve |
| `roc_curve.png` | ROC curve with AUC |

## MapReduce Design Patterns

### Pattern 1: Summable Statistics (Descriptive Stats, ANOVA)

```
Map:    (row) → (key=group, value={n, sum, sum², min, max})
Reduce: (key, [values]) → (key, {n_total, mean, sd, min, max})
```

### Pattern 2: Sufficient Statistics (T-Test, Regression)

```
Map:    (row) → (key=group, value={n, sum, sum_sq})
Reduce: (key, [values]) → (key, {n, mean, var})
Local:  Compute test statistics from aggregated values
```

### Pattern 3: Matrix Sufficient Stats (Linear Regression)

```
Map:    (row) → (key, {X'X matrix, X'y vector, y'y scalar, n})
Reduce: (key, [values]) → (key, {ΣX'X, ΣX'y, Σy'y, Σn})
Local:  β = (X'X)⁻¹X'y, SE, t-stats, p-values, R²
```

### Pattern 4: Data Extraction (Wilcoxon, Logistic Regression)

```
Map:    (row) → (key=filter_group, value=relevant_columns)
Reduce: (key, [values]) → (key, all_values)
Local:  Run statistical tests on extracted data
```

## Key Technical Notes

- **rmr2 backend:** Use `backend="local"` for development, `backend="hadoop"` for cluster
- **Small dataset:** 33K rows (1.7 MB) is small for Hadoop — MapReduce overhead is expected
- **RHadoop packages:** `rhdfs` and `rmr2` are installed from GitHub (not CRAN)
- **rJava:** Must match Hadoop's Java version. Run `R CMD javareconf` if needed
- **CSV parsing:** Map functions receive raw text lines; helpers handle header skipping

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `rhdfs.init()` fails | Start Hadoop: `$HADOOP_HOME/sbin/start-dfs.sh` |
| `rJava` won't load | Run `sudo R CMD javareconf`, reinstall rJava |
| `rmr2` install fails | Ensure `devtools` and `Rcpp` are installed first |
| MapReduce job hangs | Check YARN is running: `$HADOOP_HOME/sbin/start-yarn.sh` |
| Out of memory | Reduce JVM heap in `mr_helpers.R` rmr options |
| CSV parsing errors | Check `airline_stats.csv` has no special characters |

## Dataset

**Source:** `airline_stats.csv` (~33,468 flights)

| Column | Type | Description |
|--------|------|-------------|
| `pct_carrier_delay` | numeric | % of delays caused by the carrier |
| `pct_atc_delay` | numeric | % of delays caused by air traffic control |
| `pct_weather_delay` | numeric | % of delays caused by weather |
| `airline` | character | Airline name (18 unique airlines) |

## License

This project is for educational purposes.
