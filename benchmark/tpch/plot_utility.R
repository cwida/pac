#!/usr/bin/env Rscript
# Standalone utility boxplot plotter
# Auto-discovers utility CSVs in benchmark/tpch/utility/ and benchmark/clickbench/utility/
# If both exist, produces a side-by-side plot; otherwise plots whichever is available.
# Usage: Rscript plot_utility.R [output_dir]

# Configure user-local library path for package installation
user_lib <- Sys.getenv("R_LIBS_USER")
if (user_lib == "") {
  user_lib <- file.path(Sys.getenv("HOME"), "R", "libs")
}
if (!dir.exists(user_lib)) {
  dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
}
.libPaths(c(user_lib, .libPaths()))

required_packages <- c("ggplot2", "dplyr", "readr", "stringr", "gridExtra")
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
  library(stringr)
  library(gridExtra)
})

# Font fallback: use Linux Libertine if available, otherwise serif
base_font <- tryCatch({
  if (any(grepl("Linux Libertine", systemfonts::system_fonts()$family, fixed = TRUE))) "Linux Libertine" else "serif"
}, error = function(e) "serif")

# Resolve paths relative to this script's location
script_dir <- tryCatch(
  dirname(normalizePath(sys.frame(1)$ofile)),
  error = function(e) {
    # Fallback for Rscript invocation
    args <- commandArgs(trailingOnly = FALSE)
    file_arg <- grep("^--file=", args, value = TRUE)
    if (length(file_arg) > 0) {
      dirname(normalizePath(sub("^--file=", "", file_arg)))
    } else {
      getwd()
    }
  }
)

benchmark_root <- dirname(script_dir)  # benchmark/

tpch_util_dir <- file.path(benchmark_root, "tpch", "utility")
clickbench_util_dir <- file.path(benchmark_root, "clickbench", "utility")

args <- commandArgs(trailingOnly = TRUE)
output_dir <- if (length(args) >= 1) args[1] else file.path(benchmark_root, "tpch")
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# ============================================================================
# Helper functions
# ============================================================================
load_utility_csvs <- function(dir_path) {
  csvs <- list.files(dir_path, pattern = "^q\\d+\\.csv$", full.names = TRUE)
  if (length(csvs) == 0) return(NULL)
  bind_rows(lapply(csvs, function(f) {
    qname <- toupper(tools::file_path_sans_ext(basename(f)))
    lines <- readLines(f, n = 1)
    ncols <- length(strsplit(lines, ",")[[1]])
    if (ncols >= 3) {
      df <- readr::read_csv(f, col_names = c("utility", "recall", "precision"),
                            col_types = "ddd", show_col_types = FALSE)
    } else {
      df <- readr::read_csv(f, col_names = c("utility", "recall"),
                            col_types = "dd", show_col_types = FALSE)
      df$precision <- NA_real_
    }
    df$query <- qname
    df
  }))
}

print_utility_stats <- function(data, q_levels, label) {
  message(sprintf("\n=== Utility Summary: %s (per query) ===", label))
  has_prec <- any(!is.na(data$precision))
  if (has_prec) {
    message(sprintf("%-6s %6s %8s %8s %8s %8s %8s %8s %8s",
                    "Query", "N", "Mean", "Median", "SD", "Min", "Max", "Recall", "Prec"))
  } else {
    message(sprintf("%-6s %6s %8s %8s %8s %8s %8s %8s",
                    "Query", "N", "Mean", "Median", "SD", "Min", "Max", "Recall"))
  }
  util_summary <- data %>%
    group_by(query) %>%
    summarize(
      n = n(),
      mean_u = mean(utility), median_u = median(utility),
      sd_u = sd(utility), min_u = min(utility), max_u = max(utility),
      mean_recall = mean(recall), mean_precision = mean(precision, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(match(query, q_levels))
  for (i in seq_len(nrow(util_summary))) {
    r <- util_summary[i, ]
    if (has_prec) {
      message(sprintf("%-6s %6d %8.3f %8.3f %8.3f %8.3f %8.3f %8.3f %8.3f",
                      r$query, r$n, r$mean_u, r$median_u, r$sd_u, r$min_u, r$max_u,
                      r$mean_recall, r$mean_precision))
    } else {
      message(sprintf("%-6s %6d %8.3f %8.3f %8.3f %8.3f %8.3f %8.3f",
                      r$query, r$n, r$mean_u, r$median_u, r$sd_u, r$min_u, r$max_u,
                      r$mean_recall))
    }
  }
  overall <- data %>%
    summarize(mean_u = mean(utility), median_u = median(utility), sd_u = sd(utility),
              mean_recall = mean(recall), mean_precision = mean(precision, na.rm = TRUE))
  if (has_prec) {
    message(sprintf("\nOverall: mean=%.3f, median=%.3f, sd=%.3f, recall=%.3f, precision=%.3f",
                    overall$mean_u, overall$median_u, overall$sd_u,
                    overall$mean_recall, overall$mean_precision))
  } else {
    message(sprintf("\nOverall: mean=%.3f, median=%.3f, sd=%.3f, recall=%.3f",
                    overall$mean_u, overall$median_u, overall$sd_u, overall$mean_recall))
  }
  message("===================================\n")
}

make_utility_plot <- function(data, q_levels, subtitle = NULL) {
  data$query <- factor(data$query, levels = q_levels)
  has_prec <- any(!is.na(data$precision))

  # Summarize per query
  qstats <- data %>%
    group_by(query) %>%
    summarize(
      mean_util = mean(utility),
      sd_util = sd(utility),
      mean_recall = mean(recall),
      mean_precision = mean(precision, na.rm = TRUE),
      .groups = "drop"
    )
  qstats$query <- factor(qstats$query, levels = q_levels)

  # Scale factor: map recall/precision (0-1) onto the utility y-axis range
  max_util <- max(qstats$mean_util + qstats$sd_util, na.rm = TRUE) * 1.2
  scale_f <- max_util  # 1.0 on right axis = max_util on left axis

  # Build long-format for recall/precision background bars
  if (has_prec) {
    rp_long <- bind_rows(
      qstats %>% transmute(query, metric = "Recall", value = mean_recall * scale_f),
      qstats %>% transmute(query, metric = "Precision", value = mean_precision * scale_f)
    )
    rp_long$metric <- factor(rp_long$metric, levels = c("Recall", "Precision"))
    rp_colors <- c("Recall" = "#74b9ff", "Precision" = "#ffeaa7")
  } else {
    rp_long <- qstats %>% transmute(query, metric = "Recall", value = mean_recall * scale_f)
    rp_long$metric <- factor(rp_long$metric, levels = c("Recall"))
    rp_colors <- c("Recall" = "#74b9ff")
  }

  p <- ggplot() +
    # Background: recall/precision bars (wide, semi-transparent)
    geom_col(data = rp_long, aes(x = query, y = value, fill = metric),
             position = position_dodge(width = 0.9), width = 0.85, alpha = 0.45) +
    # Foreground: utility boxplots
    geom_boxplot(data = data, aes(x = query, y = utility),
                 fill = "white", color = "black", outlier.size = 3, width = 0.4) +
    scale_fill_manual(
      values = rp_colors,
      name = NULL
    ) +
    scale_y_continuous(
      limits = c(0, max_util),
      expand = expansion(mult = c(0, 0.05)),
      sec.axis = sec_axis(~ . / scale_f, name = "Recall / Precision",
                          breaks = seq(0, 1, 0.2),
                          labels = function(x) sprintf("%.1f", x))
    ) +
    labs(x = NULL, y = "Utility (MRE %)") +
    theme_bw(base_size = 40, base_family = base_font) +
    theme(
      panel.border = element_rect(linewidth = 1.0),
      panel.grid.major = element_line(linewidth = 1.0),
      panel.grid.minor = element_blank(),
      legend.position = "top",
      legend.title = element_blank(),
      legend.text = element_text(size = 46),
      legend.margin = margin(0, 0, -5, 0),
      legend.box.margin = margin(0, 0, -20, 0),
      axis.text.x = element_text(angle = 45, hjust = 1, size = 42),
      axis.text.y = element_text(size = 42),
      axis.title.y.left = element_text(size = 44),
      axis.title.y.right = element_text(size = 44),
      plot.title = element_text(size = 44, hjust = 0.5),
      plot.margin = margin(2, 5, 5, 5)
    )

  if (!is.null(subtitle)) {
    p <- p + ggtitle(subtitle)
  }

  p
}

# ============================================================================
# Discover available utility data
# ============================================================================
tpch_omit_queries <- c(2, 3, 10, 11, 16, 18)

tpch_data <- NULL
if (dir.exists(tpch_util_dir)) {
  tpch_data <- load_utility_csvs(tpch_util_dir)
  if (!is.null(tpch_data)) {
    tpch_data <- tpch_data %>%
      mutate(qnum = as.integer(str_extract(query, "\\d+"))) %>%
      filter(!(qnum %in% tpch_omit_queries)) %>%
      arrange(qnum)
    if (nrow(tpch_data) == 0) tpch_data <- NULL
  }
}

clickbench_data <- NULL
if (dir.exists(clickbench_util_dir)) {
  clickbench_data <- load_utility_csvs(clickbench_util_dir)
  if (!is.null(clickbench_data)) {
    clickbench_data <- clickbench_data %>%
      mutate(qnum = as.integer(str_extract(query, "\\d+"))) %>%
      arrange(qnum)
    if (nrow(clickbench_data) == 0) clickbench_data <- NULL
  }
}

has_tpch <- !is.null(tpch_data)
has_clickbench <- !is.null(clickbench_data)

if (!has_tpch && !has_clickbench) {
  stop("No utility data found in either:\n  ", tpch_util_dir, "\n  ", clickbench_util_dir)
}

# ============================================================================
# Print stats and build plots
# ============================================================================
if (has_tpch) {
  tpch_q_levels <- tpch_data %>% distinct(query, qnum) %>% arrange(qnum) %>% pull(query)
  print_utility_stats(tpch_data, tpch_q_levels, "TPC-H")
}
if (has_clickbench) {
  cb_q_levels <- clickbench_data %>% distinct(query, qnum) %>% arrange(qnum) %>% pull(query)
  print_utility_stats(clickbench_data, cb_q_levels, "ClickBench")
}

out_file <- file.path(output_dir, "tpch_utility_boxplot.png")

if (has_tpch && has_clickbench) {
  # Side-by-side layout
  p_tpch <- make_utility_plot(tpch_data, tpch_q_levels, subtitle = "TPC-H")
  p_cb <- make_utility_plot(clickbench_data, cb_q_levels, subtitle = "ClickBench")

  # Extract legend from one plot, use shared legend on top
  g_tpch <- ggplotGrob(p_tpch)
  g_cb <- ggplotGrob(p_cb)

  # Remove individual legends; we'll add a shared one
  p_tpch_nl <- p_tpch + theme(legend.position = "none")
  p_cb_nl <- p_cb + theme(legend.position = "none")

  # Get shared legend from the plot with more metrics (prefer one with precision)
  legend_source <- if (any(!is.na(clickbench_data$precision))) p_cb else p_tpch
  tmp <- ggplotGrob(legend_source)
  leg_idx <- which(sapply(tmp$grobs, function(x) x$name) == "guide-box")
  shared_legend <- tmp$grobs[[leg_idx]]

  png(filename = out_file, width = 8000, height = 1650, res = 200)
    grid.arrange(
      shared_legend,
      arrangeGrob(p_tpch_nl, p_cb_nl, ncol = 2),
      nrow = 2, heights = c(1, 12)
    )
  dev.off()
  message("Combined utility plot saved to: ", out_file)

} else if (has_tpch) {
  p <- make_utility_plot(tpch_data, tpch_q_levels)
  png(filename = out_file, width = 4000, height = 1450, res = 200)
    print(p)
  dev.off()
  message("TPC-H utility plot saved to: ", out_file)

} else {
  p <- make_utility_plot(clickbench_data, cb_q_levels)
  png(filename = out_file, width = 4000, height = 1450, res = 200)
    print(p)
  dev.off()
  message("ClickBench utility plot saved to: ", out_file)
}
