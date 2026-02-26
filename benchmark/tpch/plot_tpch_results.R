#!/usr/bin/env Rscript
# TPC-H benchmark plotter
# Reads CSV with columns: query, mode, median_ms
# When "simple hash PAC" data is present, splits PAC bars into core + PU-key join overhead

# Configure user-local library path for package installation
user_lib <- Sys.getenv("R_LIBS_USER")
if (user_lib == "") {
  user_lib <- file.path(Sys.getenv("HOME"), "R", "libs")
}
if (!dir.exists(user_lib)) {
  dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
}
.libPaths(c(user_lib, .libPaths()))

required_packages <- c("ggplot2", "dplyr", "readr", "scales", "stringr")
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
  library(readr)
  library(scales)
  library(stringr)
})

# Font fallback: use Linux Libertine if available, otherwise serif
base_font <- tryCatch({
  if (any(grepl("Linux Libertine", systemfonts::system_fonts()$family, fixed = TRUE))) "Linux Libertine" else "serif"
}, error = function(e) "serif")

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) stop("Usage: Rscript plot_tpch_results.R path/to/results.csv [output_dir]")
input_csv <- args[1]
output_dir <- if (length(args) >= 2) args[2] else dirname(input_csv)
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

input_basename <- basename(input_csv)

# Try to extract scale factor from filename
sf_match <- regmatches(input_basename, regexec("sf([0-9]+(?:[._][0-9]+)?)", input_basename, perl = TRUE))[[1]]
if (length(sf_match) > 1) {
  sf_str <- gsub('_', '.', sf_match[2])
} else {
  sf_str <- NA_character_
}

# Read CSV (format: query, mode, median_ms)
raw <- suppressWarnings(readr::read_csv(input_csv, show_col_types = FALSE))

expected_cols <- c("query", "mode", "median_ms")
missing_cols <- setdiff(expected_cols, colnames(raw))
if (length(missing_cols) > 0) {
  stop("Missing expected columns in CSV: ", paste(missing_cols, collapse = ", "))
}

raw <- raw %>%
  mutate(query = as.character(query), mode = as.character(mode)) %>%
  filter(!is.na(median_ms) & !is.na(mode)) %>%
  mutate(is_failed = median_ms < 0)

if (nrow(raw) == 0) stop("No valid data to plot.")

# Extract query number
raw <- raw %>% mutate(qnum = as.integer(str_extract(query, "\\d+")))

# Omit queries that don't use PAC or are not allowed
omit_queries <- c(2, 3, 10, 11, 16, 18)
raw <- raw %>% filter(!(qnum %in% omit_queries))

# Keep only base queries (no variants like q08-nolambda)
raw <- raw %>% filter(query == sprintf("q%02d", qnum))

if (nrow(raw) == 0) stop("No data left after filtering.")

# Rename modes
raw <- raw %>% mutate(mode = case_when(
  mode == "baseline" ~ "DuckDB",
  mode == "SIMD PAC" ~ "SIMD-PAC",
  TRUE ~ mode
))

# Ordered query list
query_order <- raw %>% distinct(query, qnum) %>% arrange(qnum) %>% pull(query)
# Convert query labels to uppercase
query_labels <- toupper(query_order)

# Slowdown report
baseline_df <- raw %>% filter(mode == "DuckDB") %>% select(query, baseline_time = median_ms)
other_modes <- raw %>% filter(mode != "DuckDB", !is_failed) %>% select(query, mode, median_ms)
slowdown_df <- other_modes %>%
  left_join(baseline_df, by = "query") %>%
  filter(!is.na(baseline_time) & baseline_time > 0) %>%
  mutate(slowdown = median_ms / baseline_time)

if (nrow(slowdown_df) > 0) {
  mode_stats <- slowdown_df %>%
    group_by(mode) %>%
    summarize(
      worst = max(slowdown), worst_q = query[which.max(slowdown)],
      avg = mean(slowdown), median = median(slowdown),
      best = min(slowdown), best_q = query[which.min(slowdown)],
      .groups = "drop"
    )
  message("\n=== Slowdown Report (vs DuckDB) ===")
  for (i in seq_len(nrow(mode_stats))) {
    r <- mode_stats[i, ]
    message(sprintf("%s: worst %.1fx (%s), avg %.1fx, median %.1fx, best %.1fx (%s)",
                    r$mode, r$worst, r$worst_q, r$avg, r$median, r$best, r$best_q))
  }
  message("===================================\n")
}

# Check if simple hash PAC data is available for the split
has_simple_hash <- "simple hash PAC" %in% raw$mode

# Compute y-axis upper limit and failed bar position
valid_max <- max(raw$median_ms[!raw$is_failed], na.rm = TRUE)
y_upper <- valid_max * 1.5   # headroom for slowdown labels
fail_y <- y_upper * 1.7      # failed bars extend above the top border

# Build plot data
# Each row: query, x_pos (numeric), component (fill), time
duckdb_df <- raw %>% filter(mode == "DuckDB") %>% select(query, median_ms)
pac_df <- raw %>% filter(mode == "SIMD-PAC") %>% select(query, pac_time = median_ms)

if (has_simple_hash) {
  hash_df <- raw %>% filter(mode == "simple hash PAC") %>% select(query, hash_time = median_ms)
  # Join all three
  combined <- duckdb_df %>%
    inner_join(pac_df, by = "query") %>%
    inner_join(hash_df, by = "query") %>%
    mutate(
      pac_failed = pac_time < 0 | hash_time < 0,
      join_overhead = ifelse(pac_failed, 0, pmax(0, pac_time - hash_time)),
      core_time = ifelse(pac_failed, -1, hash_time)
    )
  # Build long-format plot data
  plot_data <- bind_rows(
    combined %>% transmute(query, bar = "DuckDB", component = "DuckDB", time = median_ms),
    combined %>% transmute(query, bar = "SIMD-PAC", component = "SIMD-PAC", time = core_time),
    combined %>% transmute(query, bar = "SIMD-PAC", component = "PU-key join", time = join_overhead)
  )
} else {
  # No split: simple DuckDB vs PAC
  combined <- duckdb_df %>% inner_join(pac_df, by = "query")
  plot_data <- bind_rows(
    combined %>% transmute(query, bar = "DuckDB", component = "DuckDB", time = median_ms),
    combined %>% transmute(query, bar = "SIMD-PAC", component = "SIMD-PAC", time = pac_time)
  )
}

# Assign numeric x positions with manual dodge
plot_data <- plot_data %>%
  mutate(
    qidx = match(query, query_order),
    x_pos = qidx + ifelse(bar == "DuckDB", -0.2, 0.2)  # SIMD-PAC on the right
  )

# Track original times and replace failures with visual position
plot_data <- plot_data %>% mutate(original_time = time)
plot_data <- plot_data %>% mutate(time = ifelse(time < 0, fail_y, time))

# Component ordering: PU-key join on top, core on bottom, DuckDB separate
if (has_simple_hash) {
  comp_levels <- c("PU-key join", "SIMD-PAC", "DuckDB")
  comp_colors <- c("DuckDB" = "#95a5a6", "SIMD-PAC" = "#4dff4d", "PU-key join" = "#009900")
} else {
  comp_levels <- c("DuckDB", "SIMD-PAC")
  comp_colors <- c("DuckDB" = "#95a5a6", "SIMD-PAC" = "#4dff4d")
}
plot_data$component <- factor(plot_data$component, levels = comp_levels)

# Title
title_sf <- ifelse(is.na(sf_str), "sf=unknown", paste0("SF=", sf_str))
plot_title <- paste0("TPC-H Benchmark, ", title_sf)

sf_for_name <- ifelse(is.na(sf_str), "unknown", gsub("\\.", "_", sf_str))

# ============================================================================
# Performance plot
# ============================================================================
has_pacdb <- "PAC-DB" %in% raw$mode

if (!has_pacdb) {
  # No PAC-DB data: simple DuckDB vs SIMD-PAC plot
  if (has_simple_hash) {
    pac_total <- plot_data %>% filter(bar == "SIMD-PAC") %>%
      group_by(query, x_pos) %>%
      summarize(total = sum(time), .groups = "drop")
    pac_core <- plot_data %>% filter(component == "SIMD-PAC")
    duckdb_bars <- plot_data %>% filter(bar == "DuckDB")

    p <- ggplot() +
      geom_col(data = duckdb_bars, aes(x = x_pos, y = time, fill = component), width = 0.35) +
      geom_col(data = pac_total, aes(x = x_pos, y = total, fill = "PU-key join"), width = 0.35) +
      geom_col(data = pac_core, aes(x = x_pos, y = time, fill = component), width = 0.35) +
      scale_fill_manual(values = comp_colors, name = NULL,
                        breaks = c("DuckDB", "SIMD-PAC", "PU-key join")) +
      scale_x_continuous(breaks = seq_along(query_order), labels = query_labels, expand = expansion(add = 0.4)) +
      labs(x = NULL, y = NULL)
  } else {
    p <- ggplot(plot_data, aes(x = x_pos, y = time, fill = component, width = 0.35)) +
      geom_col() +
      scale_fill_manual(values = comp_colors, name = NULL) +
      scale_x_continuous(breaks = seq_along(query_order), labels = query_labels, expand = expansion(add = 0.4)) +
      labs(x = NULL, y = NULL)
  }

  slowdown_labels <- slowdown_df %>%
    filter(mode == "SIMD-PAC") %>%
    mutate(
      qidx = match(query, query_order),
      x_pos = qidx + 0.2,
      y_pos = median_ms * 1.15,
      label = sprintf("%.1fx", slowdown)
    )

  # FAILED labels for bars with original_time < 0
  failed_bars <- plot_data %>% filter(original_time < 0) %>%
    distinct(query, bar, x_pos) %>%
    mutate(time = fail_y)

  p <- p +
    geom_text(data = slowdown_labels, aes(x = x_pos, y = y_pos, label = label),
              inherit.aes = FALSE, size = 8, vjust = 0, fontface = 'bold', family = base_font) +
    { if (nrow(failed_bars) > 0)
      geom_text(data = failed_bars, aes(x = x_pos, y = time, label = "FAILED"),
                inherit.aes = FALSE, angle = 90, vjust = 0.5, hjust = 1,
                size = 5, fontface = "bold", color = "black",
                family = base_font)
    } +
    scale_y_log10(labels = function(x) ifelse(x >= 100, paste0(x / 1000, "s"), paste0(x, "ms"))) +
    coord_cartesian(ylim = c(NA, y_upper), clip = 'off') +
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
      axis.text.x = element_text(angle = 45, hjust = 1, size = 24),
      axis.text.y = element_text(size = 24),
      axis.title = element_text(size = 32),
      plot.title = element_blank(),
      plot.margin = margin(2, 5, 5, 5)
    )

  out_file <- file.path(output_dir, paste0("tpch_benchmark_plot_sf", sf_for_name, "_paper.png"))
  png(filename = out_file, width = 4000, height = 1450, res = 200)
    print(p)
  dev.off()
  message("Plot saved to: ", out_file)

} else {
  pacdb_plot_df <- raw %>% filter(mode == "PAC-DB") %>% select(query, pacdb_time = median_ms)

  if (has_simple_hash) {
    paper_combined <- duckdb_df %>%
      inner_join(pac_df, by = "query") %>%
      inner_join(pacdb_plot_df, by = "query") %>%
      inner_join(hash_df, by = "query") %>%
      mutate(
        pac_failed = pac_time < 0 | hash_time < 0,
        join_overhead = ifelse(pac_failed, 0, pmax(0, pac_time - hash_time)),
        core_time = ifelse(pac_failed, -1, hash_time)
      )
    paper_plot_data <- bind_rows(
      paper_combined %>% transmute(query, bar = "DuckDB", component = "DuckDB", time = median_ms),
      paper_combined %>% transmute(query, bar = "PAC-DB", component = "PAC-DB", time = pacdb_time),
      paper_combined %>% transmute(query, bar = "SIMD-PAC", component = "SIMD-PAC", time = core_time),
      paper_combined %>% transmute(query, bar = "SIMD-PAC", component = "PU-key join", time = join_overhead)
    )
    paper_comp_levels <- c("PU-key join", "SIMD-PAC", "PAC-DB", "DuckDB")
    paper_comp_colors <- c("DuckDB" = "#95a5a6", "PAC-DB" = "#a8d4ff", "SIMD-PAC" = "#4dff4d", "PU-key join" = "#009900")
  } else {
    paper_combined <- duckdb_df %>%
      inner_join(pac_df, by = "query") %>%
      inner_join(pacdb_plot_df, by = "query")
    paper_plot_data <- bind_rows(
      paper_combined %>% transmute(query, bar = "DuckDB", component = "DuckDB", time = median_ms),
      paper_combined %>% transmute(query, bar = "PAC-DB", component = "PAC-DB", time = pacdb_time),
      paper_combined %>% transmute(query, bar = "SIMD-PAC", component = "SIMD-PAC", time = pac_time)
    )
    paper_comp_levels <- c("SIMD-PAC", "PAC-DB", "DuckDB")
    paper_comp_colors <- c("DuckDB" = "#95a5a6", "PAC-DB" = "#a8d4ff", "SIMD-PAC" = "#4dff4d")
  }

  # Assign x positions: DuckDB left, PAC-DB center, SIMD-PAC right
  paper_plot_data <- paper_plot_data %>%
    mutate(
      qidx = match(query, query_order),
      x_pos = qidx + case_when(
        bar == "DuckDB"   ~ -0.27,
        bar == "PAC-DB"   ~  0.0,
        bar == "SIMD-PAC" ~  0.27
      )
    )
  paper_plot_data$component <- factor(paper_plot_data$component, levels = paper_comp_levels)

  # Track original times and replace failures with visual position
  paper_plot_data <- paper_plot_data %>% mutate(original_time = time)
  paper_plot_data <- paper_plot_data %>% mutate(time = ifelse(time < 0, fail_y, time))

  # Slowdown labels for both PAC-DB and SIMD-PAC
  paper_slowdown <- raw %>%
    filter(mode %in% c("PAC-DB", "SIMD-PAC"), !is_failed) %>%
    left_join(baseline_df, by = "query") %>%
    filter(!is.na(baseline_time) & baseline_time > 0) %>%
    mutate(
      slowdown = median_ms / baseline_time,
      bar = ifelse(mode == "PAC-DB", "PAC-DB", "SIMD-PAC"),
      qidx = match(query, query_order),
      x_pos = qidx + ifelse(bar == "PAC-DB", 0.0, 0.27),
      y_pos = median_ms * 1.15,
      label = sprintf("%.1fx", slowdown)
    )

  if (has_simple_hash) {
    paper_pac_total <- paper_plot_data %>% filter(bar == "SIMD-PAC") %>%
      group_by(query, x_pos) %>%
      summarize(total = sum(time), .groups = "drop")
    paper_pac_core <- paper_plot_data %>% filter(component == "SIMD-PAC")
    paper_other_bars <- paper_plot_data %>% filter(bar %in% c("DuckDB", "PAC-DB"))

    p_paper <- ggplot() +
      geom_col(data = paper_other_bars, aes(x = x_pos, y = time, fill = component), width = 0.25) +
      geom_col(data = paper_pac_total, aes(x = x_pos, y = total, fill = "PU-key join"), width = 0.25) +
      geom_col(data = paper_pac_core, aes(x = x_pos, y = time, fill = component), width = 0.25) +
      scale_fill_manual(values = paper_comp_colors, name = NULL,
                        breaks = c("DuckDB", "PAC-DB", "SIMD-PAC", "PU-key join")) +
      scale_x_continuous(breaks = seq_along(query_order), labels = query_labels, expand = expansion(add = 0.4)) +
      labs(x = NULL, y = NULL)
  } else {
    p_paper <- ggplot(paper_plot_data, aes(x = x_pos, y = time, fill = component, width = 0.25)) +
      geom_col() +
      scale_fill_manual(values = paper_comp_colors, name = NULL) +
      scale_x_continuous(breaks = seq_along(query_order), labels = query_labels, expand = expansion(add = 0.4)) +
      labs(x = NULL, y = NULL)
  }

  # FAILED labels for bars with original_time < 0
  paper_failed_bars <- paper_plot_data %>% filter(original_time < 0) %>%
    distinct(query, bar, x_pos) %>%
    mutate(time = fail_y)

  p_paper <- p_paper +
    geom_text(data = paper_slowdown, aes(x = x_pos, y = y_pos, label = label),
              inherit.aes = FALSE, size = 5, vjust = 0, fontface = 'bold', family = base_font) +
    { if (nrow(paper_failed_bars) > 0)
      geom_text(data = paper_failed_bars, aes(x = x_pos, y = time, label = "FAILED"),
                inherit.aes = FALSE, angle = 90, vjust = 0.5, hjust = 1,
                size = 5, fontface = "bold", color = "black",
                family = base_font)
    } +
    scale_y_log10(labels = function(x) ifelse(x >= 100, paste0(x / 1000, "s"), paste0(x, "ms"))) +
    coord_cartesian(ylim = c(NA, y_upper), clip = 'off') +
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
      axis.text.x = element_text(angle = 45, hjust = 1, size = 24),
      axis.text.y = element_text(size = 24),
      axis.title = element_text(size = 32),
      plot.title = element_blank(),
      plot.margin = margin(2, 5, 5, 5)
    )

  out_file_paper <- file.path(output_dir, paste0("tpch_benchmark_plot_sf", sf_for_name, "_paper.png"))
  png(filename = out_file_paper, width = 4000, height = 1500, res = 350)
    print(p_paper)
  dev.off()
  message("Paper plot saved to: ", out_file_paper)
}
