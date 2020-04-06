source("SETTINGS.R")
source("helpers.R")
source("05_make_error_data_helpers.R")
load(file = paste0(SETTINGS$path_data, "tsdata.rda"))

#########################################################
## Prepare normalized, standardized and differenced data
                                        # prepare normalized estimators
nest <- rename_df(normalize_df(d$est))
napp <- rename_df(normalize_df(d$app))

                                        # prepare standardized estimators
sest <- rename_df(standardize_df(d$est))
sapp <- rename_df(standardize_df(d$app))

                                        # prepare normalized estimators
d1nest <- diff_df(rename_df(normalize_df(d$est)), "[1d]")
d1napp <- diff_df(rename_df(normalize_df(d$app)), "[1d]")

                                        # prepare standardized estimators
d1sest <- diff_df(rename_df(standardize_df(d$est)), "[1d]")
d1sapp <- diff_df(rename_df(standardize_df(d$app)), "[1d]")


#########################################################
## Loop throgh data to create error summaries and data

                                        # Prepare variables for loop
types  <- list("mse","mae")
dta_app    <- list(napp, sapp, d1napp, d1sapp)
dta_est    <- list(nest, sest, d1nest, d1sest)
naming_types  <- list("mse","mae")
naming_dta    <- list("norm_ts","stand_ts","norm_ts_d1","stand_ts_d1")
datevecs      <- list(d$date,
                      d$date,
                      d$date[-1],
                      d$date[-1])

                                        # Loop
error_dta   <- list()
summary_dta <- list()
for(type in types){
    for(i in 1:length(dta_app)){
        data_id <- paste(type, naming_dta[[i]], sep="_")
                                        # Make error data
        error_dta[[data_id]] <- make_error_data(df_est = col_drop(dta_est[[i]], "triv"),
                                                df_app = dta_app[[i]],
                                                datevec = datevecs[[i]],
                                                type   = type)
        
                                        # Make summaries
        summary_dta[[data_id]] <- make_summary_from_data(type    = type,
                                                         errordf = error_dta[[data_id]])
    }
}
                                        # Save error data
save(error_dta, file = paste0(SETTINGS$path_data,"appVSest.rda"))

                                        # Save summaries
save(summary_dta, file = paste0(SETTINGS$path_data,"appVSest_summary.rda"))
