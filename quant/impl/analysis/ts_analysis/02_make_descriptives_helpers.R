#' Function getting converting a customized dataframe with descriptives to latex code  
#' 
#' 
#' @param SETTINGS ... the path to the tsanalysis/SETTINGS.R config file.
#'
#'
texmake_desc <- function(desc
                                        #,descriptions
                         ){
    for(i in 1:length(desc)){
        subtable_df           <- desc[[i]][ ,c("n","mean", "median","min","max","sd", "kurtosis")]
        colnames(subtable_df) <- c("Obs.", "Mean", "Med.","Min.", "Max.", "Std. Dev.", "Kurtosis")
        subtable_latex        <- xtable(subtable_df,
                                        align = c("l", rep("r", 7)),
                                        booktabs = T)
                                        #"lccccccc"
        print(subtable_latex,
              file       = paste0(SETTINGS$path_tables, SETTINGS$stor_desc_filenames[i]),
              floating   = FALSE,
              #scalebox='0.9',
              ## ,add.to.row = list(list(nrow(subtable_df)),
              ##                   paste0("\\hline  \\multicolumn{7}{L{9cm}}{\\textbf{Note: }",
              ##                          descriptions[i])
              sanitize.text.function = function(x) x
      )
    }
}


#' Helper function just wrappeing the describe package "describe" function 
#' 
#' 
#' @param x ... a dataframe to calculate descriptives on (see psych documentations)
#'
#' @return ... dataframe with descriptives
#'
.dscrbe <- function(x){
    y <- as.data.frame(psych::describe(x))
    return(y)
}

#' Helper function used as simplifying wrapper for putting data from different dataframes together for later calculating descriptives in the right order
#' 
#' 
#' @param SETTINGS ... the path to the tsanalysis/SETTINGS.R config file.
#' @param est_data ... velocity measures 
#' @param app_data ... velocity approximators
#' @param other_data ... additional data
#' @param col_excl ... a vector with strings of column names to exclude
#'
#' @return ... dataframe to calculate descriptives on
#'
prep_for_dscrbe <- function(est_data,
                            app_data,
                            other_data,
                            col_excl){
    dta                 <- dplyr::bind_cols(app_data,
                                            est_data,
                                 other_data)
    ordered_cols        <- colnames(dta)[order(gsub("_","",colnames(dta)))]
    dta                 <- dta[, ordered_cols]
    keep_indicator      <- !colnames(dta) %in% col_excl 

    dta <- dta[keep_indicator]

    return(dta)

}


#' Function getting a dataframe with descriptives from velocity estimators, approximators and "other data" (raw variables, etc.)
#' 
#' 
#' @param SETTINGS ... the path to the tsanalysis/SETTINGS.R config file.
#' @param SETTINGS ... the path to the tsanalysis/SETTINGS.R config file.
#' @param est_data ... velocity measures 
#' @param app_data ... velocity approximators
#' @param other_data ... additional data
#' @param col_excl ... a vector with strings of column names to exclude
#'
#' @return ... dataframe with descriptives. 
#'
dscrbe <- function(est_data,
                   app_data,
                   other_data,
                   col_excl = "v_app_naive"){
    dta_to_dscrbe <- prep_for_dscrbe(est_data   = est_data,
                                     app_data   = app_data,
                                     other_data = other_data,
                                     col_excl = col_excl)

    dta_to_dscrbe <- rename_df(dta_to_dscrbe)

    description <- .dscrbe(dta_to_dscrbe)

    return(description)
}
