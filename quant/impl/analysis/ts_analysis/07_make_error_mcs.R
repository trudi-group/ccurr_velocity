source("SETTINGS.R")
source("helpers.R")
source("07_make_error_mcs_helpers.R")

#########################################################
## Load stored error table summaries for normal error time series

load(file = paste0(SETTINGS$path_data, "appVSest.rda"))

#########################################################
## Run MCS test
mcs_from_errordata(mae_errordata = error_dta[["mae_stand_ts"]],
                   mse_errordata = error_dta[["mse_stand_ts"]],
                   storagepath = "stand",
                   sigLev = 0.01)

mcs_from_errordata(mae_errordata = error_dta[["mae_stand_ts"]],
                   mse_errordata = error_dta[["mse_stand_ts"]],
                   storagepath = "stand",
                   sigLev = 0.05)

mcs_from_errordata(mae_errordata = error_dta[["mae_norm_ts"]],
                   mse_errordata = error_dta[["mse_norm_ts"]],
                   storagepath = "norm")

mcs_from_errordata(mae_errordata = error_dta[["mae_stand_ts_d1"]],
                   mse_errordata = error_dta[["mse_stand_ts_d1"]],
                   storagepath = "stand_d1")

mcs_from_errordata(mae_errordata = error_dta[["mae_norm_ts_d1"]],
                   mse_errordata = error_dta[["mse_norm_ts_d1"]],
                   storagepath = "norm_d1")
