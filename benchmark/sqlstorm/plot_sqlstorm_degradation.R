#!/usr/bin/env Rscript
# SQLStorm degradation plot (percentile-wise speedup distribution)
# Reads CSV with columns: query_index, query, mode, time_ms, state
# Plots DuckDB/SIMD-PAC speedup across query percentiles (RPT+ Fig 10 style)

# Configure user-local library path for package installation
user_lib <- Sys.getenv("R_LIBS_USER")
if (user_lib == "") {
  user_lib <- file.path(Sys.getenv("HOME"), "R", "libs")
}
if (!dir.exists(user_lib)) {
  dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
}
.libPaths(c(user_lib, .libPaths()))

required_packages <- c("ggplot2", "dplyr", "tidyr", "readr", "scales")
options(repos = c(CRAN = "https://cloud.r-project.org"))
installed <- rownames(installed.packages())
for (pkg in required_packages) {
  if (!(pkg %in% installed)) {
    message("Installing package: ", pkg)
    install.packages(pkg, dependencies = TRUE, lib = user_lib)
  }
}

suppressPackageStartupMessages({
  library(ggplot2)
  library(dplyr)
  library(tidyr)
  library(readr)
  library(scales)
})

# Font fallback: use Linux Libertine if available, otherwise serif
base_font <- tryCatch({
  if (any(grepl("Linux Libertine", systemfonts::system_fonts()$family, fixed = TRUE))) "Linux Libertine" else "serif"
}, error = function(e) "serif")

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) stop("Usage: Rscript plot_sqlstorm_degradation.R path/to/degradation.csv")
input_csv <- args[1]
output_dir <- dirname(input_csv)
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# Derive output filename from input (e.g. sqlstorm_degradation_tpch.csv -> sqlstorm_degradation_tpch.png)
input_basename <- tools::file_path_sans_ext(basename(input_csv))
out_file <- file.path(output_dir, paste0(input_basename, ".png"))

# Read CSV
raw <- readr::read_csv(input_csv, show_col_types = FALSE)

expected_cols <- c("query_index", "query", "mode", "time_ms", "state")
missing_cols <- setdiff(expected_cols, colnames(raw))
if (length(missing_cols) > 0) {
  stop("Missing expected columns in CSV: ", paste(missing_cols, collapse = ", "))
}

total_queries <- length(unique(raw$query_index))

# Filter to queries where PAC was applied (if pac_applied column exists)
if ("pac_applied" %in% colnames(raw)) {
  pac_applied_idx <- raw %>%
    filter(mode == "SIMD-PAC", pac_applied == TRUE) %>%
    pull(query_index)
  raw <- raw %>% filter(query_index %in% pac_applied_idx)
  message(sprintf("Filtered to %d query indices where PAC was applied", length(pac_applied_idx)))
}

# Filter to successful queries only, and only keep pairs where both modes succeeded
success <- raw %>% filter(state == "success")

duckdb_idx <- success %>% filter(mode == "DuckDB") %>% pull(query_index)
pac_idx <- success %>% filter(mode == "SIMD-PAC") %>% pull(query_index)
both_idx <- intersect(duckdb_idx, pac_idx)

if (length(both_idx) == 0) {
  message("No queries where both modes succeeded. Skipping plot.")
  quit(status = 0)
}

plot_data <- success %>% filter(query_index %in% both_idx)

# Compute per-query overhead: SIMD-PAC / DuckDB (>1 means PAC is slower)
paired <- plot_data %>%
  select(query_index, mode, time_ms) %>%
  tidyr::pivot_wider(names_from = mode, values_from = time_ms) %>%
  mutate(overhead = `SIMD-PAC` / DuckDB)

# --- Statistics ---
message(sprintf("\n=== Overhead Statistics (%d queries where both succeeded) ===",
                nrow(paired)))
message(sprintf("  DuckDB total:   %.1f ms", sum(paired$DuckDB)))
message(sprintf("  SIMD-PAC total: %.1f ms", sum(paired$`SIMD-PAC`)))
overhead_pct <- 100.0 * (sum(paired$`SIMD-PAC`) - sum(paired$DuckDB)) / sum(paired$DuckDB)
if (overhead_pct >= 0) {
  message(sprintf("  SIMD-PAC is %.1f%% slower overall", overhead_pct))
} else {
  message(sprintf("  SIMD-PAC is %.1f%% faster overall", -overhead_pct))
}
message(sprintf("  Per-query overhead (SIMD-PAC / DuckDB):"))
message(sprintf("    mean=%.2fx, median=%.2fx, min=%.2fx, max=%.2fx",
                mean(paired$overhead), median(paired$overhead),
                min(paired$overhead), max(paired$overhead)))
slower <- sum(paired$overhead > 1.0)
faster <- sum(paired$overhead < 1.0)
equal  <- sum(paired$overhead == 1.0)
message(sprintf("    slower: %d (%.1f%%), faster: %d (%.1f%%), equal: %d",
                slower, 100.0 * slower / nrow(paired),
                faster, 100.0 * faster / nrow(paired), equal))
message("===================================\n")

# Filter out queries where overhead < 0.9 (PAC more than 10% faster than DuckDB)
paired <- paired %>% filter(overhead >= 0.9)
message(sprintf("After filtering overhead >= 0.9x: %d queries remain", nrow(paired)))

# Sort by overhead and compute percentile
paired <- paired %>%
  arrange(overhead) %>%
  mutate(percentile = (row_number() - 0.5) / n())

# Annotate key percentile values (like RPT+ Figure 10)
annotation_pcts <- c(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)
annotations <- data.frame(percentile = annotation_pcts) %>%
  rowwise() %>%
  mutate(
    idx = which.min(abs(paired$percentile - percentile)),
    overhead = paired$overhead[idx]
  ) %>%
  ungroup() %>%
  mutate(label = sprintf("%.2fx", overhead))

# Greedy filter: drop annotations too close to any previously kept one
keep <- rep(TRUE, nrow(annotations))
for (i in 2:nrow(annotations)) {
  for (j in seq_len(i - 1)) {
    if (!keep[j]) next
    dy <- abs(log2(annotations$overhead[i]) - log2(annotations$overhead[j]))
    dx <- abs(annotations$percentile[i] - annotations$percentile[j])
    if (dy < 0.6 && dx < 0.12) {
      keep[i] <- FALSE
      break
    }
  }
}
annotations <- annotations[keep, ]

# Y-axis: log2 scale with 2^k breaks
overhead_range <- range(paired$overhead)
y_min_exp <- floor(log2(overhead_range[1]))
y_max_exp <- ceiling(log2(overhead_range[2]))
y_breaks <- 2^(y_min_exp:y_max_exp)

p <- ggplot(paired, aes(x = percentile, y = overhead)) +
  geom_hline(yintercept = 1.0, linetype = "dashed", linewidth = 1.2, color = "#95a5a6") +
  geom_line(color = "#00b300", linewidth = 2.0) +
  geom_point(data = annotations, aes(x = percentile, y = overhead),
             color = "#00b300", size = 4) +
  geom_text(data = annotations, aes(x = percentile, y = overhead, label = label),
            vjust = -1.2, hjust = 0.5, size = 9, family = base_font, fontface = "bold") +
  annotate("text", x = 0.98, y = 1.0, label = "Baseline: DuckDB",
           hjust = 1, vjust = 1.5, size = 8, family = base_font,
           color = "#95a5a6", fontface = "italic") +
  scale_x_continuous(breaks = seq(0, 1, by = 0.1),
                     expand = expansion(mult = c(0.04, 0.04))) +
  scale_y_continuous(trans = "log2", breaks = y_breaks,
                     labels = function(x) paste0(x, "x"),
                     expand = expansion(mult = c(0.15, 0.15))) +
  labs(x = "Overhead VS Query Percentile", y = NULL) +
  theme_bw(base_size = 40, base_family = base_font) +
  theme(
    panel.border = element_rect(linewidth = 1.0),
    panel.grid.major = element_line(linewidth = 0.5),
    panel.grid.minor = element_blank(),
    axis.ticks.length = unit(0.3, "cm"),
    axis.ticks = element_line(linewidth = 1.0),
    axis.text.x = element_text(size = 28),
    axis.text.y = element_text(size = 28),
    axis.title = element_text(size = 32),
    axis.title.y = element_blank(),
    plot.title = element_blank(),
    plot.margin = margin(20, 15, 5, 5)
  )

png(filename = out_file, width = 4000, height = 1350, res = 300)
  print(p)
dev.off()
message("Degradation plot saved to: ", out_file)
