source("SETTINGS.R")
source("helpers.R")
load(file = paste0(SETTINGS$path_data, "tsdata.rda"))

##################################################################
## prepare extraction of data
data <- get_sub_df(df = d$all,
                   pattern = c("v_app",
                               "v_est",
                               "m_total",
                               "m_circ",
                               "tx_vol_infl",
                               "tx_vol_clean",
                               "price_usd"),
                   type = "match")


##################################################################
## prepare corr table
corrtable <- round(cor(rename_df(data)), digits = 2)
corrtable <- cbind(rownames(corrtable), corrtable)

##################################################################
## Rotate heads
rownames(corrtable) <- rownames(corrtable)
cols <- paste0("\\rothead{",colnames(corrtable), "}", collapse = " & ")
cols <- paste0(cols, "\\\\")

##################################################################
## Print table
subtable_latex <- xtable(corrtable, align = "ccccccccccccccccc")
subtable_latex <- print(subtable_latex
                      , file=paste0(SETTINGS$path_tables, "corrtable_all.tex")
                      , include.colnames = FALSE
                      , include.rownames = FALSE
                      , add.to.row = list(pos = as.list(0)
                                        , command = cols)
                      , sanitize.text.function = function(x) x
                      , floating = FALSE
                      , booktabs = FALSE
                      , hline.after = c(0)
                      , format.args = list(width = 1)
)
