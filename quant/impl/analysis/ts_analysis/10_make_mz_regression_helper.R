#' Helper function executing the MZ-regressions
#' 
#' @param estimator ... velocity measure
#' @param approximator ... velocity approximation
#'  
#' @return ... dataframe with results of the MZ-regression
#'
mzr <- function(estimator,
                approximator){
  
  mz_reg <- lm(estimator ~ approximator)
  
  adjR2 <- summary(mz_reg)$adj.r.squared
  
  alpha.est   <- summary(mz_reg)$coefficients["(Intercept)","Estimate"]
  alpha.tval  <- summary(mz_reg)$coefficients["(Intercept)","t value"]
  alpha.pval  <- summary(mz_reg)$coefficients["(Intercept)","Pr(>|t|)"]
  
  # handler for singularities and semi-singularities if approximator is straight line
  Ftest = tryCatch({linearHypothesis(mz_reg, c("(Intercept) = 0", "approximator = 1"))}, error = function(e) {e <- NA})
  Ftest.pval  <- if(all(is.na(Ftest))){NA} else {Ftest$`Pr(>F)`[2]}
  Ftest.F     <- if(all(is.na(Ftest))){NA} else {Ftest$F[2]}
  
  beta.est = tryCatch({summary(mz_reg)$coefficients["approximator","Estimate"]}, error = function(e) {e <- NA})
  beta.est    <- if(all(is.na(Ftest))){NA} else {beta.est}
  beta.tval   <- if(all(is.na(Ftest))){NA} else {summary(mz_reg)$coefficients["approximator","t value"]}
  beta.pval   <- if(all(is.na(Ftest))){NA} else {summary(mz_reg)$coefficients["approximator","Pr(>|t|)"]}
  
  
  
  result <- data.frame(
      adjR2      = adjR2,
      alpha.est  = alpha.est,
      alpha.pval = alpha.pval,
      beta.est   = beta.est,
      beta.pval  = beta.pval,
      Ftest.pval = Ftest.pval
  )

    names(result) <- c(
        "$R^{2}_{adj}$"
       ,"$\\alpha$"
       ,"$p^{\\alpha}$"
       ,"$\\beta$"
       ,"$p^{\\beta}$"
       ,"$p^{F-Test}$"
    )   
  
  return(result)
}


#' Function exectuing the mz-regression for the all the velocity measures and approximators
#' 
#' @param app ... dataframe with approximators for velocity
#' @param est ... dataframe with estimators for velocity
#' @param r_digits ... number of digits to be displayed in dataframe 
#' 
#' @return a dataframe collecting the results from all mz-regressions
#'
mz_table <- function(est,
                     app,
                     r_digits = 2){
    mz <- NULL
    mz_rownames <- NULL
    for(i in 1:ncol(app)){
        for(j in 1:ncol(est)){

            estimator           <- est[,j]
            approximator        <- app[,i]

            append <- mzr(estimator = estimator, approximator = approximator)
            
            if(i == 1 & j == 1){mz <- append}
            else{mz <- rbind(mz, append)}
            mz_rownames_append <- paste0(colnames(app)[i]," VS ", colnames(est)[j])
            mz_rownames        <- c(mz_rownames, mz_rownames_append)
            
        }
    }
    rownames(mz) <- mz_rownames
    mz <- format(round(mz, digits = r_digits), nsmall = 2)
    return(mz)
}
