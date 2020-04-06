source("SETTINGS.R")
source("helpers.R")

#########################################################
## Load stored error table summaries for normal error time series
load(file = paste0(SETTINGS$path_data, "appVSest_summary.rda"))

#########################################################
## Build a joined summary
summary <- cbind(
    summary_dta[["mae_stand_ts"]],
    summary_dta[["mae_stand_ts"]],
    summary_dta[["mae_norm_ts"]],
    summary_dta[["mse_norm_ts"]],
    summary_dta[["mae_stand_ts_d1"]],
    summary_dta[["mae_stand_ts_d1"]],
    summary_dta[["mae_norm_ts_d1"]],
    summary_dta[["mse_norm_ts_d1"]])
                                        # save colnames and rownames
cnames <- rep(c("MAE","MSE","MAE","MSE"), 2)
rnames <- rownames(summary)
                                        # round dataframe and convert to char.
summary <- format(round(summary, digits = 2), nsmall = 2)
summary <- sapply(summary, as.character)
                                        # use saved colnames and rownames
rownames(summary) <- rnames
colnames(summary) <- cnames

#########################################################
## Make rownames to 2 columns in the beginning
summary <- cbind(data.frame(Approximation = splitrownames(rnames = rownames(summary),
                                                          by = " VS ")[["left_from_BY"]],
                            Estimator = splitrownames(rnames = rownames(summary),
                                                      by = " VS ")[["right_from_BY"]]),
                 summary)


#########################################################
## Add columns as row for custom column header
cnames  <- as.character(rbind(colnames(summary)))
summary <- rbind(cnames, summary)

#########################################################
Add places for significance stars
sig_tuples_1 <- c("10-1","11-1","12-1","13-1",
                  "10-2","11-2","12-2","13-2",
                  "7-3","8-3","9-3","10-3",
                  "7-4","8-4","9-4","10-4",
                  "10-5","11-5","12-5","13-5",
                  "10-6","11-6","12-6","13-6",
                  "10-7","11-7","12-7","13-7",
                  "10-8","11-8","12-8","13-8")


summary <- add_sig_stars(df = summary,
                         tuples_1stars = sig_tuples_1,
                         tuples_2stars = NULL,
                         tuples_3stars = NULL)

#########################################################
## Add Multicolumn row
addtorow <- list()
addtorow$pos <- list(0)
addrow1 <-"\\hline \\multicolumn{2}{l}{ } & \\multicolumn{4}{c}{Raw Data} & \\multicolumn{4}{c}{First Differences} \\\\"
addrow2 <- "\\hline \\multicolumn{2}{l}{ } & \\multicolumn{2}{c}{standardized} & \\multicolumn{2}{c}{normalized} & \\multicolumn{2}{c}{standardized} & \\multicolumn{2}{c}{normalized} \\\\"
addtorow$command <- paste0(addrow1, addrow2)

#########################################################
## Finally print latex table
subtable_latex        <- xtable(summary,
                                align = "lllrrrrrrrr")
print(subtable_latex
    , file=paste0(SETTINGS$path_tables, "appVSest_errors_w_mcs.tex")
    , sanitize.text.function = function(x) x
    , floating = FALSE
    , include.rownames=FALSE
    , add.to.row=addtorow
    , include.colnames=FALSE,
    , hline.after = c(0, 1, 5, 9, nrow(summary)))
