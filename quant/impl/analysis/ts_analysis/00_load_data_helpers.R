#' Function retrieving data saved by velo script 
#' 
#' 
#' @param SETTINGS ... the path to the tsanalysis/SETTINGS.R config file.
#'
#' @return ... a dataframe with the data from the velo script filtered for the time period indicated in the settings file
#' 
get_data_vola <- function(SETTINGS){
                                        # read in file
    df   <- read.csv(paste0(SETTINGS$path_data, SETTINGS$file))
                                        # minor preprocessing
    df$date <- paste0("20", as.character(df$date))
    df$date <- anytime(as.character(df$date),
                       asUTC = TRUE)
    df$date <- as.Date(df$date, unit = "day")
                                        # extracting right time period
    indctr  <- (df$date > anytime(SETTINGS$start_date) &
                 df$date < SETTINGS$end_date)
    df <- df[indctr, ]

    return(df)
}

#' Function getting data from the blockwatch data broker
#' 
#' 
#' @param SETTINGS ... the path to the tsanalysis/SETTINGS.R config file.
#' @param download ... is a boolean flag, if data should be downloaded freshly or cached data is to be used
#'
#' @return a dataframe with the days destroyed data from the blockwatch database filterd for the time period indicated in the settings file
#'
get_data_blockwatch <- function(download = FALSE, SETTINGS){
                                        # only one time download and save.
                                        # Later only load
    if(download){
        df_txly = blockwatch.table("BTC/BLOCK",
                                   time.rg=paste0(SETTINGS$start_date,",",SETTINGS$end_date),
                                   columns="days_destroyed",
                                   paginate=TRUE)
        save(df_txly, file = paste0(SETTINGS$path_data, "blocksci_dormancy.rda"))
    }
    
    load(file = paste0(SETTINGS$path_data, "/blocksci_dormancy.rda"))
                                        # rounded timestamps to days
    df_txly$time <- round_date(df_txly$time, unit = "day")
                                        # group data by time
    df_daily <- df_txly %>%
        mutate(time = as.Date(time, format = "%m/%d/%y")) %>%
        group_by(time) %>%
        summarize(days_destroyed = sum(days_destroyed)) %>%
        as.data.frame

    return(df_daily)
}

#' Function getting data from coinmarketcap.com
#' 
#' 
#' @param SETTINGS ... the path to the tsanalysis/SETTINGS.R config file.
#' @param download ... is a boolean flag, if data should be downloaded freshly or cached data is to be used
#'
#' @return a dataframe with the days destroyed data from the blockwatch database filterd for the time period indicated in the settings file
#'
get_data_cmc <- function(path, SETTINGS){

                                        # load data from a already downloaded source
    df       <- read.csv(path)
    df$time  <- as.Date(anytime(df$bitcoin.timestamp))

                                        # get subset
    indctr   <- (dta_velo$date > anytime(SETTINGS$start_date) &
                 dta_velo$date < anytime(SETTINGS$end_date))
    df           <- df[indctr, ]
    colnames(df) <- str_remove(string = colnames(df), pattern="bitcoin.")

    return(df)
}

#' Function getting data from wikipedia (not used here)
#' 
#' 
#' @param SETTINGS ... the path to the tsanalysis/SETTINGS.R config file.
#'
#' @return a dataframe with wikipedia search hits filterd for the time period indicated in the settings file
#'
get_data_wiki <- function(SETTINGS){
    df_raw <- article_pageviews(article="Bitcoin",
                                start=date_wiki_format(SETTINGS$start_date),
                                end=date_wiki_format(SETTINGS$end_date))
    df <- df_raw[ ,c("date", "views")]
    colnames(df)[2] <- "views_wiki"
    df$date <- as.Date(df$date)

    return(df)
}


#' Helper function to change the format of the string date from wikipedia downloads
#' 
#' @param x ... the string representing a date in the human format f.e. "2013-06-01". 
#'
#' @return ... string in the wikipedia-coder-format f.e. "2013060100".  
#'
date_wiki_format <- function(x){
    paste0(str_replace_all(as.character(x), "-", ""),"00")
}
