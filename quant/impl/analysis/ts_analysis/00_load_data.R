### prepare environment
source("SETTINGS.R")
source("helpers.R")
source("00_load_data_helpers.R")

##################################################################
## Load data from different sources
dta_velo        <- get_data_vola(SETTINGS = SETTINGS)
dta_blockwatch  <- get_data_blockwatch(SETTINGS = SETTINGS, download = SETTINGS$blockwatch_refresh)
dta_cmc         <- get_data_cmc(path = "../../../../data/marketmeasures_raw/marketmeasures.csv",
                                SETTINGS = SETTINGS)
#dta_wiki        <- get_data_wiki(SETTINGS)

##################################################################
## Merge data from different sources to single dataframe
dta <- merge(x = dta_blockwatch,
             y = dta_velo,
             by.x = "time",
             by.y = "date")
dta <- merge(x = dta,
             y = dta_cmc,
             by.x = "time",
             by.y = "time")
## dta <- merge(x = dta,
##              y = dta_wiki,
##              by.x = "time",
##              by.y = "date",
##              all.x = TRUE)

##################################################################
## Save data
save(dta, file = paste0(SETTINGS$path_data, "ts_datadump.rda"))
