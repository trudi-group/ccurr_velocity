source("SETTINGS.R")
source("helpers.R")
load(file = paste0(SETTINGS$path_data, "tsdata.rda"))
options("tikzLatex"="/usr/local/texlive/2018/bin/x86_64-linux/latex")

## prepare extraction of data
data <- get_sub_df(df = d$all,
                   pattern = c("m_total","m_circ_wh_bill","price_usd",
                               "tx_vol_clean", "v_est_m_total",
                               "v_est_m_circ_wb"),
                   type = "match")

## prepare corr table
corrtable <- cor(rename_df(data))


## colnames unfortunately to long
rownames(corrtable)[rownames(corrtable) == "$\\mathtt{Vol.(deflated)}$"] <- "$\\mathtt{Vol}$"
colnames(corrtable)[colnames(corrtable) == "$\\mathtt{Vol.(deflated)}$"] <- "$\\mathtt{Vol}$"

## Make latex tables
tikz(file = paste0(SETTINGS$path_figs, "corrplot_components.tex"), width = 3, height = 3.5)
print(ggcorrplot(corrtable,
                 hc.order = FALSE,
                 type = "lower",
                 show.legend = FALSE,
                 colors = c("white","white","white"),
                 outline.color = "grey",
                 lab = TRUE))
dev.off()


