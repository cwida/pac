#!/usr/bin/env Rscript
# Utility experiment plot: naive (N x pac_sum) vs optimized (pac_sum_counters + list_transform)
# Reads results.csv, plots side-by-side boxplots of relative error by num_ratios and variant.
#
# Usage: Rscript plot.R [results.csv]

# Configure user-local library path
user_lib <- Sys.getenv("R_LIBS_USER")
if (user_lib == "") {
  user_lib <- file.path(Sys.getenv("HOME"), "R", "libs")
}
if (!dir.exists(user_lib)) {
  dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
}
.libPaths(c(user_lib, .libPaths()))

required_packages <- c("ggplot2", "dplyr", "readr")
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
})

# Font fallback: use Linux Libertine if available, otherwise serif
base_font <- tryCatch({
  if (any(grepl("Linux Libertine", systemfonts::system_fonts()$family, fixed = TRUE))) "Linux Libertine" else "serif"
}, error = function(e) "serif")

# Resolve input
args <- commandArgs(trailingOnly = TRUE)
script_dir <- tryCatch(
  dirname(normalizePath(sys.frame(1)$ofile)),
  error = function(e) {
    file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
    if (length(file_arg) > 0) dirname(normalizePath(sub("^--file=", "", file_arg))) else getwd()
  }
)

input_csv <- if (length(args) >= 1) args[1] else file.path(script_dir, "results.csv")
if (!file.exists(input_csv)) stop("Results file not found: ", input_csv)
output_dir <- dirname(input_csv)

# Read data
raw <- readr::read_csv(input_csv, show_col_types = FALSE)
# Columns: run, query, num_ratios, variant, value, true_value

# Compute relative error %
data <- raw %>%
  mutate(
    rel_error = 100.0 * abs(value - true_value) / pmax(abs(true_value), 0.00001)
  )

# Print summary
message("\n=== Utility Summary (mean relative error %) ===")
summary_tbl <- data %>%
  group_by(num_ratios, variant) %>%
  summarize(
    n = n(),
    mean = mean(rel_error),
    median = median(rel_error),
    sd = sd(rel_error),
    .groups = "drop"
  ) %>%
  arrange(num_ratios, variant)

message(sprintf("%-10s %-12s %6s %8s %8s %8s", "num_ratios", "variant", "N", "Mean", "Median", "SD"))
for (i in seq_len(nrow(summary_tbl))) {
  r <- summary_tbl[i, ]
  message(sprintf("%-10d %-12s %6d %8.4f %8.4f %8.4f", r$num_ratios, r$variant, r$n, r$mean, r$median, r$sd))
}

# Factor levels
data$num_ratios <- factor(data$num_ratios)
data$variant <- factor(data$variant, levels = c("naive", "optimized"),
                       labels = c("Naive (N\u00D7 noise)", "Optimized (1\u00D7 noise)"))

# Colors matching the project style
variant_colors <- c("Naive (N\u00D7 noise)" = "#e74c3c", "Optimized (1\u00D7 noise)" = "#00b300")

p <- ggplot(data, aes(x = num_ratios, y = rel_error, fill = variant)) +
  geom_boxplot(outlier.size = 2, width = 0.7, position = position_dodge(width = 0.8)) +
  scale_fill_manual(values = variant_colors, name = NULL) +
  scale_y_continuous(expand = expansion(mult = c(0, 0.05))) +
  labs(
    x = "Number of Ratio Expressions",
    y = "Relative Error (%)"
  ) +
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
    legend.position = "top",
    legend.title = element_blank(),
    legend.text = element_text(size = 30),
    legend.margin = margin(0, 0, -5, 0),
    legend.box.margin = margin(0, 0, -15, 0),
    plot.title = element_blank(),
    plot.margin = margin(5, 15, 5, 5)
  )

out_file <- file.path(output_dir, "utility_listtransform.png")
png(filename = out_file, width = 4000, height = 1350, res = 300)
  print(p)
dev.off()
message("Plot saved to: ", out_file)
