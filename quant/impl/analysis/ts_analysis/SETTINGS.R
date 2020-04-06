library(psych)
library(ggplot2)
library(reshape2)
library(stringr)
library(pageviews)
library(anytime)
library(dplyr)
library(tidyverse)
library(lubridate)
library(blockwatch)
library(stargazer)
library(RColorBrewer)
library(stargazer)
library(corrplot)
library(tikzDevice)
library(vars)
library(xtable)
library(MCS)
library(ggcorrplot)
library(car)
library(dplyr)

options(scipen=999)
options("tikzLatex"="/usr/local/texlive/2018/bin/x86_64-linux/latex")
#options("tikzLatex"="/usr/bin/pdflatex")
options(stringsAsFactors = FALSE)

n <- 20
qual_col_pals = brewer.pal.info[brewer.pal.info$category == 'qual',]

btc_per_satoshi          <- 100000000
scaling_multiplicator    <- 1000000

key = readLines("/home/ingolf/APIKEYS/blockwatch.txt")
#key = "nonexistent"
blockwatch.api_key(key)

SETTINGS <- list()
#SETTINGS$start_date              <- "2011-01-01"
#SETTINGS$end_date                <- "2013-12-30"
SETTINGS$start_date              <- "2013-06-01"
SETTINGS$end_date                <- "2019-06-01"
SETTINGS$path_data               <- #"../../../../data/velopaper_data/"
#SETTINGS$path_data               <- "../../../../data/bcdata_raw/clustering_test/"
SETTINGS$path_data               <- "../../../../data/velopaper_data/"
SETTINGS$blockwatch_refresh      <- FALSE #
#SETTINGS$file                    <- "velo_daily_e_2019_06_01_clustering_off.csv"
#SETTINGS$file                    <- "velo_daily_e_2019_06_01_clustering_ON.csv"
SETTINGS$file                    <- "data.csv"
## SETTINGS$path_tables             <- "../../../../data/bcdata_raw/clustering_test/ts_tables/"
## SETTINGS$path_figs               <- "../../../../data/bcdata_raw/clustering_test/ts_figs/"
SETTINGS$path_tables             <- "../../../../text/ts_tables/"
SETTINGS$path_figs               <- "../../../../text/ts_figs/"
SETTINGS$stor_desc_filenames     <- c("descriptives_est_and_app.tex",
                                      "descriptives_nest_and_napp.tex",
                                      "descriptives_sest_and_sapp.tex",
                                      "descriptives_other.tex")
SETTINGS$palette_1               <- unlist(mapply(brewer.pal,
                                                  qual_col_pals$maxcolors,
                                                  rownames(qual_col_pals)))
SETTINGS$palette_2               <- c("#999999", "#E69F00",
                                      "#56B4E9", "#009E73",
                                      "#F0E442", "#0072B2",
                                      "#D55E00", "#CC79A7")
SETTINGS$palette_3               <- c("#332288", "#88CCEE",
                                      "#44AA99", "#117733",
                                      "#999933", "#AA4499",
                                      "#DDCC77", "#882255")
SETTINGS$titles_for_plots        <- FALSE#TRUE #
SETTINGS$btc_per_satoshi         <- btc_per_satoshi
SETTINGS$scaling_multiplicator   <- scaling_multiplicator
