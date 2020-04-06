### prepare environment
source("SETTINGS.R")
source("helpers.R")
source("01_calc_measures_helpers.R")
load(file = paste0(SETTINGS$path_data, "ts_datadump.rda"))

##################################################################
### prepare basic variables from the dataframes
m_total        <- scaledown(dta$m_total)
m_circ_mc_lifo <- scaledown(dta$m_circ_mc_lifo_1)
m_circ_mc_fifo <- scaledown(dta$m_circ_mc_fifo_1)
m_circ_wb      <- scaledown(dta$m_circ_wh_bill_1)

tx_vol         <- scaledown(dta$tx_vol)
tx_vol_infl    <- scaledown(dta$tx_vol)
tx_vol_clean   <- scaledown(dta$tx_vol - dta$tx_vol_issues_chouts)

tx_number      <- scaledown(dta$tx_number, type="no_exchangerate")

cdd            <- scaledown(dta$days_destroyed, type="no_exchangerate")
dorm           <- cdd / tx_vol_infl
turnover       <- (1 / dorm)*365

volume_usd         <- scaledown(dta$volume_usd)
tx_fees            <- scaledown(dta$tx_fees)
marketcap          <- scaledown(dta$market_cap_by_available_supply)

price_usd          <- dta$price_usd
returns            <- dta$return_wrt_price_usd.simple
volatility         <- dta$vol.squaredreturns
views_wiki         <- dta$views_wiki

##################################################################
### prepare dataframe with target variables from the basic variables

data                      <- dta[ , "time", drop = FALSE]
data$v_est_m_naive        <- tx_vol_infl / m_total
data$v_est_m_total        <- tx_vol_clean / m_total
data$v_est_m_circ_wb      <- tx_vol_clean / m_circ_wb
data$v_est_m_circ_mc_lifo <- tx_vol_clean / m_circ_mc_lifo
data$v_est_m_circ_mc_fifo <- tx_vol_clean / m_circ_mc_fifo

data$v_app_coindd         <- cdd 
data$v_app_turnover       <- turnover
data$v_app_naive          <- tx_vol_infl/m_total

data$dormancy             <- dorm
data$turnover             <- turnover
data$m_total              <- m_total
data$m_circ_mc_lifo       <- m_circ_mc_lifo 
data$m_circ_mc_fifo       <- m_circ_mc_fifo 
data$m_circ_wh_bill       <- m_circ_wb
data$tx_vol               <- tx_vol 
data$tx_vol_infl          <- tx_vol_infl
data$tx_vol_clean         <- tx_vol_clean

data$volume_usd           <- volume_usd
data$tx_fees              <- tx_fees
data$tx_cdd               <- cdd
data$marketcap            <- marketcap

data$price_usd            <- price_usd
data$returns              <- returns
data$volatility           <- volatility
data$views_wiki           <- views_wiki

##################################################################
### Drop Inf and -Inf
data[sapply(data, is.infinite)] <- NA

##################################################################
### Save in smaller subsets for convenience
d <- list()
d$date       <- data$time
d$est        <- data[ ,str_detect(colnames(data), pattern = "v_est")]
d$est_old    <- data[ ,str_detect(colnames(data), pattern = "v_est_m_total|v_est_m_naive")]
d$est_new    <- data[ ,str_detect(colnames(data), pattern = "(v_est_m_circ)")]
d$app        <- data[ ,str_detect(colnames(data), pattern = "v_app")]
d$other      <- data[ ,!grepl("time", colnames(data)) & !grepl("v_est|v_app", colnames(data))]
d$all        <- data

##################################################################
### Clean outliers

d_nooutl_packed <- lapply(d, function(x) if(is.data.frame(x)) cleanOutliers(x, num.sd = 10) else x)

d_withoutl  <-d
d           <-d_nooutl_packed


##################################################################
### Save
save(d, file = paste0(SETTINGS$path_data, "tsdata.rda"))
save(d_withoutl, file = paste0(SETTINGS$path_data, "tsdata_wOutl.rda"))



