#!/usr/bin/env Rscript
# plot_microbench_results.R
#
# Scans results/* subfolders and generates simple COUNT plots
# (ungrouped, grouped_seq, grouped_scat)

suppressPackageStartupMessages({
  if (!requireNamespace("ggplot2", quietly = TRUE)) install.packages("ggplot2")
  if (!requireNamespace("dplyr", quietly = TRUE)) install.packages("dplyr")
  if (!requireNamespace("readr", quietly = TRUE)) install.packages("readr")
  if (!requireNamespace("stringr", quietly = TRUE)) install.packages("stringr")
  if (!requireNamespace("scales", quietly = TRUE)) install.packages("scales")

  library(ggplot2)
  library(dplyr)
  library(readr)
  library(stringr)
  library(scales)
})

results_root <- "results"
if (!dir.exists(results_root)) {
  stop("results directory not found: ", results_root)
}

# ------------------------------------------------------------------------------
# Discover architecture directories
# ------------------------------------------------------------------------------

candidates <- list.files(results_root, full.names = FALSE)
arch_dirs <- file.path(
  results_root,
  candidates[file.info(file.path(results_root, candidates))$isdir]
)

if (length(arch_dirs) == 0) {
  arch_dirs <- results_root
}

# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------

find_latest_file <- function(dirpath, prefix) {
  files <- list.files(
    dirpath,
    pattern = paste0("^", prefix, "_\\d{8}_\\d{6}.*\\.csv$"),
    full.names = TRUE
  )

  if (length(files) == 0) return(NULL)

  stamps <- str_extract(basename(files), "\\d{8}_\\d{6}")
  valid  <- !is.na(stamps)
  if (!any(valid)) return(NULL)

  files  <- files[valid]
  stamps <- stamps[valid]
  nums   <- as.numeric(gsub("_", "", stamps))

  files[which.max(nums)]
}

normalize_df <- function(df) {
  nm <- names(df)

  if ("wall_sec" %in% nm && !"time_sec" %in% nm) df$time_sec <- df$wall_sec
  if ("time_sec" %in% nm && !"wall_sec" %in% nm) df$wall_sec <- df$time_sec

  if ("rows_m" %in% nm && !"data_size_m" %in% nm) df$data_size_m <- df$rows_m
  if ("data_size_m" %in% nm && !"rows_m" %in% nm) df$rows_m <- df$data_size_m

  if (!"variant" %in% nm) df$variant <- NA_character_
  if (!"test"    %in% nm) df$test    <- NA_character_
  if (!"groups"  %in% nm) df$groups  <- NA
  if (!"rows_m"  %in% nm) df$rows_m  <- NA

  df
}

# ------------------------------------------------------------------------------
# Main processing per architecture
# ------------------------------------------------------------------------------

process_arch <- function(arch_dir) {
  message("Processing: ", arch_dir)

  file <- find_latest_file(arch_dir, "count")
  if (is.null(file)) {
    message("  no count file found")
    return(invisible(NULL))
  }

  message("  using: ", file)

  df <- tryCatch(
    read_csv(file, show_col_types = FALSE),
    error = function(e) {
      message("  failed to read: ", e$message)
      NULL
    }
  )
  if (is.null(df)) return(invisible(NULL))

  df <- normalize_df(df)

  raw_time_col <- if ("wall_sec" %in% names(df)) "wall_sec" else "time_sec"
  df$wall_sec_raw   <- suppressWarnings(as.numeric(df[[raw_time_col]]))
  # (NO OVERRIDE) use raw wall times for all rows, including ungrouped

  # treat -1 as missing (NA) so single crash markers don't zero out the mean for a group/variant
  df$wall_sec_clean <- df$wall_sec_raw
  df$wall_sec_clean[!is.na(df$wall_sec_clean) & df$wall_sec_clean == -1] <- NA_real_

  if ("wall_times" %in% names(df)) {
    first_from_times <- function(s) {
      if (is.na(s)) return(NA_real_)
      toks <- str_extract_all(as.character(s), "-?\\d+\\.?\\d*")[[1]]
      if (length(toks) == 0) NA_real_ else as.numeric(toks[1])
    }

    wt_first <- vapply(df$wall_times, first_from_times, NA_real_)
    idx <- which(is.na(df$wall_sec_clean) | df$wall_sec_clean == 0)
    df$wall_sec_clean[idx] <- wt_first[idx]
  }

  df$wall_sec_plot <- as.numeric(df$wall_sec_clean)

  df$groups_label <- vapply(
    df$groups,
    function(x) {
      n <- suppressWarnings(as.numeric(as.character(x)))
      if (is.na(n)) return(as.character(x))
      if (n >= 1e6) return(paste0(n / 1e6, "M"))
      if (n >= 1e3) return(formatC(n, format = "d", big.mark = ","))
      as.character(n)
    },
    FUN.VALUE = ""
  )

  df$rows_m_num <- suppressWarnings(as.numeric(as.character(df$rows_m)))
  df$rows_m_label <- vapply(
    df$rows_m_num,
    function(r) {
      if (is.na(r)) NA_character_
      else if (r %% 1000 == 0 && r >= 1000) paste0(r / 1000, "B rows")
      else paste0(r, "M rows")
    },
    FUN.VALUE = ""
  )

  df$variant <- as.factor(ifelse(is.na(df$variant), "unknown", df$variant))
  df$groups  <- as.factor(df$groups)

  # --------------------------------------------------------------------------
  # Plot helpers
  # --------------------------------------------------------------------------

  make_plot <- function(sub, outname, title) {
    sub <- sub %>% filter(!is.na(wall_sec_clean))
    if (nrow(sub) == 0) return()

    p <- ggplot(sub, aes(groups_label, wall_sec_plot, fill = variant)) +
      geom_col(position = position_dodge2(width = 0.9)) +
      facet_wrap(~ rows_m_label, nrow = 1, scales = "free_x") +
      labs(x = "groups", y = "Wall time (s)", fill = "Variant", title = title) +
      theme_bw(base_size = 14) +
      theme(
        legend.position = "top",
        axis.text.x = element_text(angle = 45, hjust = 1),
        plot.title = element_text(face = "bold", hjust = 0.5)
      )

    ggsave(file.path(arch_dir, outname), p, width = 12, height = 6, dpi = 300)
  }

  # --------------------------------------------------------------------------
  # Subsets
  # --------------------------------------------------------------------------

  sub_ungrouped <- df %>%
    filter(test == "ungrouped") %>%
    mutate(variant = recode(variant,
      standard = "No PAC",
      default  = "PAC Optimized"
    ))

  sub_grouped_seq <- df %>%
    filter(test %in% c("grouped_seq", "grouped")) %>%
    mutate(variant = recode(variant,
      standard = "No PAC",
      default  = "PAC Optimized"
    ))

  sub_grouped_scat <- df %>%
    filter(grepl("scat", test)) %>%
    mutate(variant = recode(variant,
      standard = "No PAC",
      default  = "PAC Optimized"
    ))

  make_plot(sub_ungrouped,     "count_ungrouped.png",     paste(basename(arch_dir), "ungrouped"))
  make_plot(sub_grouped_seq,   "count_grouped_seq.png",   paste(basename(arch_dir), "grouped_seq"))
  make_plot(sub_grouped_scat,  "count_grouped_scat.png",  paste(basename(arch_dir), "grouped_scat"))

  # --------------------------------------------------------------------------
  # Slowdown plot for ungrouped: include all variants (do not collapse)
  # --------------------------------------------------------------------------
  sub_ungrouped_all <- df %>%
    filter(test == "ungrouped") %>%
    filter(!is.na(wall_sec_plot)) %>%
    mutate(variant = as.character(variant))

  if (nrow(sub_ungrouped_all) > 0) {
    # aggregate mean wall time per (groups_label, rows_m_label, variant)
    agg <- sub_ungrouped_all %>%
      group_by(groups_label, rows_m_label, variant) %>%
      summarize(mean_sec = mean(wall_sec_plot, na.rm = TRUE), .groups = "drop")

    # compute baseline per (groups_label, rows_m_label)
    # prefer the 'standard' variant if present, otherwise fall back to the fastest variant
    baseline <- agg %>%
      group_by(groups_label, rows_m_label) %>%
      do({
        dfg <- .
        std <- dfg %>% filter(variant == "standard")
        if (nrow(std) == 1 && !is.na(std$mean_sec) && std$mean_sec > 0) {
          data.frame(baseline_mean = std$mean_sec)
        } else {
          # choose the fastest (min mean_sec) as baseline
          minv <- dfg %>% filter(!is.na(mean_sec)) %>% arrange(mean_sec) %>% slice(1)
          if (nrow(minv) == 1 && !is.na(minv$mean_sec) && minv$mean_sec > 0) {
            data.frame(baseline_mean = minv$mean_sec)
          } else {
            data.frame(baseline_mean = NA_real_)
          }
        }
      }) %>%
      ungroup()

    # join baseline back and compute slowdown
    agg_bs <- agg %>%
      left_join(baseline, by = c("groups_label", "rows_m_label")) %>%
      mutate(slowdown = ifelse(is.na(baseline_mean) | baseline_mean == 0, NA_real_, mean_sec / baseline_mean))

    # prepare plotting dataframe: drop invalid slowdown values
    plot_df <- agg_bs %>% filter(!is.na(slowdown) & is.finite(slowdown))

    if (nrow(plot_df) > 0) {
      pslow <- ggplot(plot_df, aes(x = groups_label, y = slowdown, fill = variant)) +
        geom_col(position = position_dodge2(width = 0.9)) +
        facet_wrap(~ rows_m_label, nrow = 1, scales = "free_x") +
        labs(x = "groups", y = "Slowdown (x)", fill = "Variant", title = paste(basename(arch_dir), "ungrouped slowdown (all variants)")) +
        theme_bw(base_size = 14) +
        theme(
          legend.position = "top",
          axis.text.x = element_text(angle = 45, hjust = 1),
          plot.title = element_text(face = "bold", hjust = 0.5)
        ) +
        scale_y_continuous(labels = scales::number_format(accuracy = 0.01))

      ggsave(file.path(arch_dir, "slowdown_ungrouped_allvariants.png"), pslow, width = 12, height = 6, dpi = 300)
    } else {
      message("  no valid slowdown data for ungrouped (all variants)")
    }
  } else {
    message("  no ungrouped rows to compute slowdown")
  }

  # --------------------------------------------------------------------------
  # Also process sum/avg results (sum_avg_*.csv) and produce one PNG per
  # combination of (test, aggregate, dtype). Each PNG contains two stacked
  # panels: absolute mean per-variant and slowdown per-variant (vs baseline).
  # Baseline preference: variant == 'standard' if present, otherwise the
  # fastest variant per (groups, rows) is used.
  # --------------------------------------------------------------------------
  file_sum <- find_latest_file(arch_dir, "sum_avg")
  if (!is.null(file_sum)) {
    message("  using sum/avg: ", file_sum)
    df_sum <- tryCatch(
      read_csv(file_sum, show_col_types = FALSE),
      error = function(e) {
        message("  failed to read sum_avg: ", e$message)
        NULL
      }
    )

    if (!is.null(df_sum)) {
      df_sum <- normalize_df(df_sum)

      raw_time_col_sum <- if ("wall_sec" %in% names(df_sum)) "wall_sec" else "time_sec"
      df_sum$wall_sec_raw   <- suppressWarnings(as.numeric(df_sum[[raw_time_col_sum]]))
      df_sum$wall_sec_clean <- df_sum$wall_sec_raw
      df_sum$wall_sec_clean[!is.na(df_sum$wall_sec_clean) & df_sum$wall_sec_clean == -1] <- NA_real_

      if ("wall_times" %in% names(df_sum)) {
        first_from_times <- function(s) {
          if (is.na(s)) return(NA_real_)
          toks <- str_extract_all(as.character(s), "-?\\d+\\.?\\d*")[[1]]
          if (length(toks) == 0) NA_real_ else as.numeric(toks[1])
        }

        wt_first <- vapply(df_sum$wall_times, first_from_times, NA_real_)
        idx <- which(is.na(df_sum$wall_sec_clean) | df_sum$wall_sec_clean == 0)
        df_sum$wall_sec_clean[idx] <- wt_first[idx]
      }

      df_sum$wall_sec_plot <- as.numeric(df_sum$wall_sec_clean)

      df_sum$groups_label <- vapply(
        df_sum$groups,
        function(x) {
          n <- suppressWarnings(as.numeric(as.character(x)))
          if (is.na(n)) return(as.character(x))
          if (n >= 1e6) return(paste0(n / 1e6, "M"))
          if (n >= 1e3) return(formatC(n, format = "d", big.mark = ","))
          as.character(n)
        },
        FUN.VALUE = ""
      )

      df_sum$rows_m_num <- suppressWarnings(as.numeric(as.character(df_sum$rows_m)))
      df_sum$rows_m_label <- vapply(
        df_sum$rows_m_num,
        function(r) {
          if (is.na(r)) NA_character_
          else if (r %% 1000 == 0 && r >= 1000) paste0(r / 1000, "B rows")
          else paste0(r, "M rows")
        },
        FUN.VALUE = ""
      )

      df_sum$variant <- as.factor(ifelse(is.na(df_sum$variant), "unknown", df_sum$variant))

      # focus on rows with valid time
      df_sum_valid <- df_sum %>% filter(!is.na(wall_sec_plot))

      if (nrow(df_sum_valid) == 0) {
        message("  no valid rows in sum_avg file")
      } else {
        # find all combinations of (test, aggregate, dtype)
        combos <- df_sum_valid %>% distinct(test, aggregate, dtype)
        sanitize <- function(x) gsub("[^A-Za-z0-9_-]", "_", x)

        for (i in seq_len(nrow(combos))) {
          tname <- combos$test[i]
          aname <- combos$aggregate[i]
          dname <- combos$dtype[i]

          subset_rows <- df_sum_valid %>% filter(test == tname & aggregate == aname & dtype == dname)
          if (nrow(subset_rows) == 0) next

          # aggregate mean wall time per (groups_label, rows_m_label, variant)
          agg_combo <- subset_rows %>%
            group_by(groups_label, rows_m_label, variant) %>%
            summarize(mean_sec = mean(wall_sec_plot, na.rm = TRUE), .groups = "drop")

          if (nrow(agg_combo) == 0) {
            message("  no aggregated data for combo: ", tname, ",", aname, ",", dname)
            next
          }

          # compute baseline per (groups_label, rows_m_label)
          baseline_combo <- agg_combo %>%
            group_by(groups_label, rows_m_label) %>%
            do({
              dfg <- .
              std <- dfg %>% filter(variant == "standard")
              if (nrow(std) >= 1 && !is.na(std$mean_sec[1]) && std$mean_sec[1] > 0) {
                data.frame(baseline_mean = std$mean_sec[1])
              } else {
                minv <- dfg %>% filter(!is.na(mean_sec)) %>% arrange(mean_sec) %>% slice(1)
                if (nrow(minv) == 1 && !is.na(minv$mean_sec) && minv$mean_sec > 0) {
                  data.frame(baseline_mean = minv$mean_sec)
                } else {
                  data.frame(baseline_mean = NA_real_)
                }
              }
            }) %>% ungroup()

          agg_bs_combo <- agg_combo %>% left_join(baseline_combo, by = c("groups_label", "rows_m_label")) %>%
            mutate(slowdown = ifelse(is.na(baseline_mean) | baseline_mean == 0, NA_real_, mean_sec / baseline_mean))

          # prepare absolute and slowdown plots (variants shown as different fills)
          p_abs <- ggplot(agg_combo, aes(x = groups_label, y = mean_sec, fill = variant)) +
            geom_col(position = position_dodge2(width = 0.9)) +
            facet_wrap(~ rows_m_label, nrow = 1, scales = "free_x") +
            labs(x = "groups", y = "Mean wall time (s)", fill = "Variant",
                 title = paste(basename(arch_dir), paste(tname, aname, dname, sep = " - ")) ) +
            theme_bw(base_size = 14) +
            theme(legend.position = "top", axis.text.x = element_text(angle = 45, hjust = 1), plot.title = element_text(face = "bold", hjust = 0.5))

          plot_slow_df <- agg_bs_combo %>% filter(!is.na(slowdown) & is.finite(slowdown))
          if (nrow(plot_slow_df) > 0) {
            p_slow <- ggplot(plot_slow_df, aes(x = groups_label, y = slowdown, fill = variant)) +
              geom_col(position = position_dodge2(width = 0.9)) +
              facet_wrap(~ rows_m_label, nrow = 1, scales = "free_x") +
              labs(x = "groups", y = "Slowdown (x)", fill = "Variant",
                   title = paste(basename(arch_dir), paste(tname, aname, dname, "(slowdown)", sep = " - ")) ) +
              theme_bw(base_size = 14) +
              theme(legend.position = "top", axis.text.x = element_text(angle = 45, hjust = 1), plot.title = element_text(face = "bold", hjust = 0.5)) +
              scale_y_continuous(labels = scales::number_format(accuracy = 0.01))
          } else {
            # produce an empty placeholder plot when slowdown cannot be computed
            p_slow <- ggplot() + theme_void() + labs(title = "No valid slowdown data")
          }

          # save two separate PNGs: absolute mean and slowdown
          fname_abs <- paste0("sumavg_", sanitize(tname), "_", sanitize(aname), "_", sanitize(dname), "_abs.png")
          out_abs <- file.path(arch_dir, fname_abs)
          ggsave(out_abs, p_abs, width = 12, height = 6, dpi = 300)
          message("  wrote: ", out_abs)

          fname_slow <- paste0("sumavg_", sanitize(tname), "_", sanitize(aname), "_", sanitize(dname), "_slowdown.png")
          out_slow <- file.path(arch_dir, fname_slow)
          # if p_slow is a placeholder empty plot it's still safe to save
          ggsave(out_slow, p_slow, width = 12, height = 6, dpi = 300)
          message("  wrote: ", out_slow)
        }
      }
    }
  } else {
    message("  no sum/avg file found for arch: ", arch_dir)
  }
}

# ------------------------------------------------------------------------------
# Run
# ------------------------------------------------------------------------------

for (d in arch_dirs) {
  tryCatch(process_arch(d),
           error = function(e) message("Error in ", d, ": ", e$message))
}

message("Done")
