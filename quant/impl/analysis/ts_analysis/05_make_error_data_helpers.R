#' Function generating a dataframe with deviations between the approximation data and measure data for velocity
#' 
#' @param SETTINGS ... the path to the tsanalysis/SETTINGS.R config file.
#' @param df_est ... velocity data, measured
#' @param df_app ... velocity data, estimated
#' @param type ... switch variable, currently only "mse" and "mae"
#' @param datevec ... vector with time data 
#' 
#' @return a dataframe with error data
#'
make_error_data <- function(SETTTINGS = SETTTINGS,
                            df_est,
                            df_app,
                            type,
                            datevec = d$date ){

    if(type == "mae"){

        mae_from_norm <- data.frame(date = datevec)
        for(i in 1:ncol(df_app)){
            mae_from_norm_element           <- as.data.frame(sapply(df_est, .mae_special, df_app, i))
            colnames(mae_from_norm_element) <- paste0(colnames(df_app)[i]," VS ", colnames(df_est))
            mae_from_norm                   <- cbind(mae_from_norm, mae_from_norm_element)
        }
        result <- mae_from_norm

    }else if(type == "mse"){

        mse_from_norm <- data.frame(date = datevec)
        for(i in 1:ncol(df_app)){
            mse_from_norm_element           <- as.data.frame(sapply(df_est, .mse_special, df_app, i))
            colnames(mse_from_norm_element) <- paste0(colnames(df_app)[i]," VS ", colnames(df_est))
            mse_from_norm                   <- cbind(mse_from_norm, mse_from_norm_element)
        }
        result <- mse_from_norm
    }
    
    return(result)
}


#' Function summing up deviations from the error data and putting the sums into a dataframe. 
#' 
#' @param SETTINGS ... the path to the tsanalysis/SETTINGS.R config file.
#' @param errordf ... the dataframe with the deviations between measured and approximated velocity
#' @param type ... error type that is to be calculated. Just used to format column names.
#'
#' @return a dataframe with the summed up errors.
#'
make_summary_from_data <- function(type = "mse", errordf){
    
    summary_errordf <- as.data.frame(sapply(errordf[, -1], sum))
    summary_errordf <- as.data.frame(summary_errordf[order(colnames(summary_errordf))])
    colnames(summary_errordf) <- toupper(type)

    return(summary_errordf)
}

