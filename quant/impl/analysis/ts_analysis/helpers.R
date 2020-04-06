date_indctr <- function(datevec, date){
    ind <- as.Date(datevec, format = "%m/%d/%y") == anydate(date)
    return(ind)
}
date_wiki_format <- function(x){
    paste0(str_replace_all(as.character(x), "-", ""),"00")
}

.normalize <- function(x){
    normalized = (x-min(x))/(max(x)-min(x))
    return(normalized)
}

.standardize <- function(x){
    normalized = (x-mean(x))/sd(x)
    return(normalized)
}

normalize_df <- function(x){
    x_norm = as.data.frame(sapply(x, .normalize))
    return(x_norm)
}

standardize_df <- function(x){
    x_stand = as.data.frame(sapply(x, .standardize))
    return(x_stand)
}


.mse_special <- function(x,d_app,i){
    (x-d_app[ ,i])^2
}

.mae_special <- function(x,d_app,i){
    abs(x-d_app[ ,i])
}

wDate <- function(df){
    df <- cbind(d$date,df)
    colnames(df)[1] <- "date"
    return(df)
}


rename_df <- function(df){
    i_rep_all <- NULL
    for(i in 1:length(colnames(df))){
        i_rep <- switch(colnames(df)[i],
                        date = "Date",
                        v_est_m_naive = "$V^{\\mathtt{msr}}_{\\mathtt{triv}}$",
                        v_est_m_total = "$V^{\\mathtt{msr}}_{\\mathtt{total}}$",
                        v_est_m_circ_wb = "$V^{\\mathtt{msr}}_{\\mathtt{circWba}}$",
                        v_est_m_circ_mc_lifo = "$V^{\\mathtt{msr}}_{\\mathtt{circMcaLifo}}$",
                        v_est_m_circ_mc_fifo = "$V^{\\mathtt{msr}}_{\\mathtt{circMcaFifo}}$",
                        v_app_coindd = "$V^{\\mathtt{app}}_{\\mathtt{cdd}}$",
                        v_app_turnover = "$V^{\\mathtt{app}}_{\\mathtt{turn}}$",
                        v_app_naive = "$V^{\\mathtt{app}}_{\\mathtt{triv}}$",
                        days_destroyed = "INT VAR",
                        dormancy = "INT VAR",
                        turnover = "INT VAR",
                        tx_vol = "INT VAR",
                        tx_vol_infl = "$\\mathtt{Vol.(inflated)}$",
                        tx_vol_clean = "$\\mathtt{Vol.(deflated)}$",
                        tx_number = "INT VAR",
                        tx_fees = "INT VAR",
                        m_total = "$M_{\\mathtt{total}}$",
                        m_circ_wh_bill = "$M_{\\mathtt{circWba}}$",
                        m_circ_mc_lifo = "$M_{\\mathtt{circMcaLifo}}$",
                        m_circ_mc_fifo = "$M_{\\mathtt{circMcaFifo}}$",
                        marketcap = "INT VAR",
                        price_usd = "$P^{\\mathtt{USD}/\\mathtt{BTC}}$",
                        volume_usd = "$\\mathtt{Vol. (off-chain)}$",
                        returns = "$\\mathtt{Return} (\\mathtt{USD}/\\mathtt{BTC})$",
                        volatility = "$\\mathtt{Volatility} (\\mathtt{USD}/\\mathtt{BTC})$",
                        views_wiki = "INT VAR",
                        tx_cdd = "$\\mathtt{bdd}$")
        i_rep_all <- c(i_rep_all, i_rep)
    }
    colnames(df) <- i_rep_all
    return(df)
}
rename_df_with_mark_for_scaling <- function(df){
    i_rep_all <- NULL
    for(i in 1:length(colnames(df))){
        i_rep <- switch(colnames(df)[i],
                        date = "Date",
                        v_est_m_naive = "$V^{\\mathtt{msr}}_{\\mathtt{triv}}$",
                        v_est_m_total = "$V^{\\mathtt{msr}}_{\\mathtt{total}}$",
                        v_est_m_circ_wb = "$V^{\\mathtt{msr}}_{\\mathtt{circWba}}$",
                        v_est_m_circ_mc_lifo = "$V^{\\mathtt{msr}}_{\\mathtt{circMcaLifo}}$",
                        v_est_m_circ_mc_fifo = "$V^{\\mathtt{msr}}_{\\mathtt{circMcaFifo}}$",
                        v_app_coindd = "$V^{\\mathtt{app}}_{\\mathtt{cdd}}$",
                        v_app_turnover = "$V^{\\mathtt{app}}_{\\mathtt{turn}}$",
                        v_app_naive = "$V^{\\mathtt{app}}_{\\mathtt{triv}}$",
                        days_destroyed = "INT VAR",
                        dormancy = "INT VAR",
                        turnover = "INT VAR",
                        tx_vol = "INT VAR",
                        tx_vol_infl = "$^{\\ast}\\mathtt{Vol.(inflated)}$",
                        tx_vol_clean = "$^{\\ast}\\mathtt{Vol.(deflated)}$",
                        tx_number = "INT VAR",
                        tx_fees = "INT VAR",
                        m_total = "$^{\\ast}M_{\\mathtt{total}}$",
                        m_circ_wh_bill = "$^{\\ast}M_{\\mathtt{circWba}}$",
                        m_circ_mc_lifo = "$^{\\ast}M_{\\mathtt{circMcaLifo}}$",
                        m_circ_mc_fifo = "$^{\\ast}M_{\\mathtt{circMcaFifo}}$",
                        marketcap = "INT VAR",
                        price_usd = "$P^{\\mathtt{USD}/\\mathtt{BTC}}$",
                        volume_usd = "$^{\\ast}\\mathtt{Vol. (off-chain)}$",
                        returns = "$\\mathtt{Return} (\\mathtt{USD}/\\mathtt{BTC})$",
                        volatility = "$\\mathtt{Volatility} (\\mathtt{USD}/\\mathtt{BTC})$",
                        views_wiki = "INT VAR",
                        tx_cdd = "$^{\\ast}\\mathtt{bdd}$")
        i_rep_all <- c(i_rep_all, i_rep)
    }
    colnames(df) <- i_rep_all
    return(df)
}

get_sub_df <- function(df, pattern, type){
    if(type == "exact"){
        df <- df[ ,pattern]
    }else if(type == "match"){
        df <- df[,grepl(paste(pattern, collapse="|"), colnames(df))]
    }
    return(df)
}


diff_df <-function(x, diffname){
    x_diff <- as.data.frame(sapply(x,diff))
    colnames(x_diff) <- paste0(diffname, colnames(x_diff))
    return(x_diff)
}


add_sig_stars <- function(df
                        , tuples_1stars
                        , tuples_2stars
                        , tuples_3stars
                        , rowadd = 0
                        , coladd = 2
                          ){
    
    for (r in 1:nrow(df)){
        for (c in 1:ncol(df)){
            if(paste0(r,"-",c) %in% tuples_1stars){df[r+rowadd,c+coladd] <- paste0(" \\(^{\\dag}\\) ",
                                                                                   df[r+rowadd,c+coladd])} 
        }
    }

    for (r in 1:nrow(df)){
        for (c in 1:ncol(df)){
            if(paste0(r,"-",c) %in% tuples_2stars){df[r+rowadd,c+coladd] <- paste0(" \\(^{\\ddag}\\) ",
                                                                                   df[r+rowadd,c+coladd])} 
        }
    }
    
    for (r in 1:nrow(df)){
        for (c in 1:ncol(df)){
            if(paste0(r,"-",c) %in% tuples_3stars){df[r+rowadd,c+coladd] <- paste0(" \\(^{\\ast}\\) ",
                                                                                   df[r+rowadd,c+coladd])} 
        }
    }
    return(df)
}


add_sig_stars_automatic <- function(vec,
                            sigLevs = c(0.10, 0.05, 0.01)
                            ){

    vec_num   <- suppressWarnings(as.numeric(vec))
    vec_char  <- vec
    vec_char[is.na(vec_num )] <- "\\( - \\)"
    for(sigLev in sigLevs){
        for(i in 1:length(vec)){

            if(!is.na(vec_num[i]) && vec_num[i] < sigLev){
                vec_char[i] <- paste0(vec_char[i], "\\(^{\\ast} \\)")
            }
            
        }
    }
return(vec_char)
}


cleanOutliers <- function(num.sd = 10, df_raw, type = "forLapply"){
  
    if("time" %in% colnames(df_raw)){
        time <- df_raw$time
    }
    df <- df_raw[ ,!colnames(df_raw) %in% "time", drop = FALSE]
    
    threshold_upper <- function(y){thu <- mean(y, na.rm = TRUE) + num.sd*sd(y, na.rm = TRUE); return(thu)}
    
    df <- as.data.frame(lapply(df, function(x){replace(x, x > threshold_upper(x), threshold_upper(x))}))
    num_repl <- lapply(df, function(x){sum(x > threshold_upper(x), na.rm = TRUE)})
    names(num_repl) <- colnames(df)
    
    if("time" %in% colnames(df_raw)){
        df <- cbind(time, df)
    }

    if(type != "forLapply"){
    res <- list(df, num_repl)
    names(res) <- c("data", "number_of_replacements")
    }else{
        res <- df
    }
    
    return(res)
    
}

splitrownames <- function(rnames, by = " VS "){
    rnames_split <- strsplit(x=rnames, " VS ")    
    rnames_1 <- as.character(unlist(lapply(rnames_split, function(x){x[[1]]})))
    rnames_2 <- as.character(unlist(lapply(rnames_split, function(x){x[[2]]})))
    res <- list(rnames_1, rnames_2)
    names(res) <- c("left_from_BY", "right_from_BY")
    return(res)
}

col_drop <- function(df, term){
    df <- df[, !grepl(term,colnames(df)), drop = F]
    return(df)
}
