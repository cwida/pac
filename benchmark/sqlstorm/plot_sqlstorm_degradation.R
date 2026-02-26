#!/usr/bin/env Rscript
# SQLStorm degradation line plot
# Reads CSV with columns: query_index, query, mode, time_ms, state
# Plots per-query latency over execution sequence with LOESS smoothing

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
  # Keep only query indices where the SIMD-PAC row has pac_applied == true
  pac_applied_idx <- raw %>%
    filter(mode == "SIMD-PAC", pac_applied == TRUE) %>%
    pull(query_index)
  raw <- raw %>% filter(query_index %in% pac_applied_idx)
  message(sprintf("Filtered to %d query indices where PAC was applied", length(pac_applied_idx)))
}

# Filter to successful queries only, and only keep pairs where both modes succeeded
success <- raw %>% filter(state == "success")

# Find query indices where both DuckDB and SIMD-PAC succeeded
duckdb_idx <- success %>% filter(mode == "DuckDB") %>% pull(query_index)
pac_idx <- success %>% filter(mode == "SIMD-PAC") %>% pull(query_index)
both_idx <- intersect(duckdb_idx, pac_idx)

if (length(both_idx) == 0) {
  message("No queries where both modes succeeded. Skipping plot.")
  quit(status = 0)
}

plot_data <- success %>% filter(query_index %in% both_idx)

# --- Speedup/slowdown statistics ---
paired <- plot_data %>%
  select(query_index, mode, time_ms) %>%
  tidyr::pivot_wider(names_from = mode, values_from = time_ms) %>%
  mutate(ratio = `SIMD-PAC` / DuckDB)

message(sprintf("\n=== Speedup/Slowdown Statistics (%d queries where both succeeded) ===",
                nrow(paired)))
message(sprintf("  DuckDB total:   %.1f ms", sum(paired$DuckDB)))
message(sprintf("  SIMD-PAC total: %.1f ms", sum(paired$`SIMD-PAC`)))
overhead_pct <- 100.0 * (sum(paired$`SIMD-PAC`) - sum(paired$DuckDB)) / sum(paired$DuckDB)
if (overhead_pct >= 0) {
  message(sprintf("  SIMD-PAC is %.1f%% slower overall", overhead_pct))
} else {
  message(sprintf("  SIMD-PAC is %.1f%% faster overall", -overhead_pct))
}
message(sprintf("  Per-query ratio (SIMD-PAC / DuckDB):"))
message(sprintf("    mean=%.2fx, median=%.2fx, min=%.2fx, max=%.2fx",
                mean(paired$ratio), median(paired$ratio),
                min(paired$ratio), max(paired$ratio)))
slower <- sum(paired$ratio > 1.0)
faster <- sum(paired$ratio < 1.0)
equal  <- sum(paired$ratio == 1.0)
message(sprintf("    slower: %d (%.1f%%), faster: %d (%.1f%%), equal: %d",
                slower, 100.0 * slower / nrow(paired),
                faster, 100.0 * faster / nrow(paired), equal))
message("===================================\n")

# Reassign sequential x-position for plotting (1, 2, 3, ...)
idx_map <- data.frame(
  query_index = sort(unique(both_idx)),
  x = seq_along(sort(unique(both_idx)))
)
plot_data <- plot_data %>% left_join(idx_map, by = "query_index")

# Factor mode for consistent ordering
plot_data$mode <- factor(plot_data$mode, levels = c("DuckDB", "SIMD-PAC"))

mode_colors <- c("DuckDB" = "#95a5a6", "SIMD-PAC" = "#00b300")
# Lighter versions for raw lines so grey is visible underneath green
line_colors <- c("DuckDB" = "#a0b0b0", "SIMD-PAC" = "#5de65d")

max_x <- max(plot_data$x)
# Choose tick interval to avoid overlapping labels
tick_interval <- if (max_x > 10000) 2000 else 1000
x_breaks <- seq(0, max_x, by = tick_interval)
if (!1 %in% x_breaks) x_breaks <- c(1, x_breaks[x_breaks > 0])

# Identify timed-out queries and map to sequential x positions
timeout_data <- raw %>%
  filter(state == "timeout") %>%
  inner_join(idx_map, by = "query_index") %>%
  distinct(x)

# Split data by mode for separate raw line colors
duckdb_lines <- plot_data %>% filter(mode == "DuckDB")
pac_lines <- plot_data %>% filter(mode == "SIMD-PAC")

p <- ggplot(plot_data, aes(x = x, y = time_ms)) +
  { if (nrow(timeout_data) > 0)
    geom_vline(data = timeout_data, aes(xintercept = x),
               color = "#e74c3c", linewidth = 0.5, alpha = 0.6, linetype = "dashed")
  } +
  geom_line(data = duckdb_lines, color = line_colors["DuckDB"], linewidth = 1.5, alpha = 0.5) +
  geom_line(data = pac_lines, color = line_colors["SIMD-PAC"], linewidth = 1.5, alpha = 0.5) +
  geom_smooth(aes(color = mode), method = "loess", se = FALSE, linewidth = 2.5) +
  scale_color_manual(values = mode_colors, name = NULL) +
  scale_x_continuous(breaks = x_breaks, labels = scales::comma) +
  scale_y_log10(
    labels = function(x) ifelse(x >= 100, paste0(x / 1000, "s"), paste0(x, "ms")),
    expand = expansion(mult = c(0.05, 0.05))
  ) +
  labs(x = "Query Index", y = NULL) +
  theme_bw(base_size = 40, base_family = base_font) +
  theme(
    panel.border = element_rect(linewidth = 1.0),
    panel.grid.major = element_line(linewidth = 1.0),
    panel.grid.minor = element_blank(),
    legend.position = "top",
    legend.title = element_blank(),
    legend.text = element_text(size = 28),
    legend.margin = margin(0, 0, -5, 0),
    legend.box.margin = margin(0, 0, -20, 0),
    axis.text.x = element_text(size = 24),
    axis.text.y = element_text(size = 24),
    axis.title = element_text(size = 32),
    plot.title = element_blank(),
    plot.margin = margin(20, 5, 5, 5)
  )

png(filename = out_file, width = 4000, height = 1800, res = 350)
  print(p)
dev.off()
message("Degradation plot saved to: ", out_file)
