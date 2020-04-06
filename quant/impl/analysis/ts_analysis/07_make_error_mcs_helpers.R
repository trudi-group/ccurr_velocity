#' Function executing the MCS tests and saving the resulting outputs.
#' 
#' @param mae_errordata ... deviations (MAE) between approximations and measures for velocity as dataframe
#' @param mse_errordata ... deviations (MSE) between approximations and measures for velocity as dataframe
#' @param storagepath ... path to directory where the results are to be stored.
#' @param sigLev ... the significance levels used for the MCS tests.
#' 
#' @return -
#'
mcs_from_errordata <- function(mae_errordata,
                               mse_errordata,
                               storagepath,
                               sigLev = 0.01){
    filterterms <- c("total", "circWba", "circMcaLifo", "circMcaFifo")

    mcs_mae_errordata <- list()
    mcs_mse_errordata <- list()

    for (filterterm in filterterms){

        mae_errordata_subset <- mae_errordata[,colnames(mae_errordata)[grepl(filterterm,
                                                                             colnames(mae_errordata))]]
        colnames(mae_errordata_subset) <- str_replace_all(str_replace_all(colnames(mae_errordata_subset), "[^[:alnum:]]", "-"),"mathtt", "")

        mse_errordata_subset <- mae_errordata[,colnames(mse_errordata)[grepl(filterterm,
                                                                             colnames(mse_errordata))]]
        colnames(mse_errordata_subset) <- str_replace_all(colnames(mse_errordata_subset), "[^[:alnum:]]", "-")

        print(paste0("<<<< START: ",storagepath ," - MAE - ", filterterm ," >>>"))
        mcs_mae_errordata[[filterterm]] <- MCSprocedure(Loss=mae_errordata_subset,
                                                        alpha=sigLev,
                                                        B=5000,
                                                        statistic='Tmax',
                                                        cl=NULL)
        print(paste0("<<<< START: ",storagepath," - MSE - ", filterterm ," >>>"))
        mcs_mse_errordata[[filterterm]] <- MCSprocedure(Loss=mse_errordata_subset,
                                                        alpha=sigLev,
                                                        B=5000,
                                                        statistic='Tmax',
                                                        cl=NULL)
    }
    
    save(mcs_mae_errordata, file = paste0(SETTINGS$path_data,
                                          "mae_appVSest_mcs_",
                                          storagepath,
                                          ".rda"))
    save(mcs_mse_errordata, file = paste0(SETTINGS$path_data,
                                          "mse_appVSest_mcs_",
                                          storagepath,
                                          ".rda"))
}


