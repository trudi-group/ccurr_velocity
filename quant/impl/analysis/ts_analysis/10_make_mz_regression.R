source("SETTINGS.R")
source("helpers.R")
source("10_make_mz_regression_helper.R")
load(file = paste0(SETTINGS$path_data, "tsdata.rda"))

########################################################
## Prepare normalized, standardized and differenced data
                                        # prepare normalized estimators
nest <- rename_df(col_drop(normalize_df(d$est), "VNaive"))
napp <- rename_df(normalize_df(d$app))

                                        # prepare standardized estimators
sest <- rename_df(col_drop(standardize_df(d$est), "VNaive"))
sapp <- rename_df(standardize_df(d$app))

                                        # prepare normalized estimators
d1nest <- diff_df(rename_df(col_drop(normalize_df(d$est), "naive")), "$\\Delta$ ")
d1napp <- diff_df(rename_df(normalize_df(d$app)), "$\\Delta$ ")

                                        # prepare standardized estimators
d1sest <- diff_df(rename_df(col_drop(standardize_df(d$est), "naive")), "$\\Delta$ ")
d1sapp <- diff_df(rename_df(standardize_df(d$app)), "$\\Delta$ ")



########################################################
## Gather Input for MZ-Regressions and run regressions
mz_input <- list(list(d1nest, d1napp),
                 list(d1sest, d1sapp))
mz_pathdiff <- list("normalized_d1",
                    "standardized_d1")
mz <- list()
for(i in 1:length(mz_input)){
    ## do mz regressions
    mz[[i]] <- mz_table(est      = mz_input[[i]][[1]],
                        app      = mz_input[[i]][[2]],
                        r_digits = 2)
}
mz <- do.call(cbind, mz)

########################################################

########################################################
## Add significance stars
sigCols <- c(3,5,6, 9,11,12)
for(sigCol in sigCols){
    mz[ ,sigCol] <- add_sig_stars_automatic(mz[ ,sigCol])
}

########################################################
## Make latex tables

########################################################
## Add column names in column, as xtable gets into trouble
                                        # [-1] because first rown
mz <- cbind(data.frame(`Approximation` = splitrownames(rnames = rownames(mz),
                                                       by = " VS ")[["left_from_BY"]],
                       Estimator = splitrownames(rnames = rownames(mz),
                                                 by = " VS ")[["right_from_BY"]],
                       stringsAsFactors = FALSE),
            mz)
## add colnames as row for xtables
cnames <- as.character(rbind(colnames(mz)))
mz <- rbind(cnames, mz)


## Add Multicolumn row
addtorow <- list()
addtorow$pos <- list(0)
addtorow$command <- "\\hline \\multicolumn{2}{l}{ } & \\multicolumn{6}{c}{Normalized} & \\multicolumn{6}{c}{Standardized} \\\\"

########################################################
## Print table
subtable_latex        <- xtable(mz, align = "lllrrrrrrrrrrrr")#ccccccccc
print(subtable_latex
    , file=paste0(SETTINGS$path_tables, "appVSest_mz_table.tex")
    , sanitize.text.function = function(x) x
    , floating = FALSE
    , include.rownames=FALSE
    , add.to.row=addtorow
    , include.colnames=FALSE
    , hline.after = c(0, 1, 5, 9, nrow(mz)))
