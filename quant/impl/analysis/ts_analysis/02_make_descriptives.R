source("SETTINGS.R")
source("helpers.R")
source("02_make_descriptives_helpers.R")
load(file = paste0(SETTINGS$path_data, "tsdata_wOutl.rda"))
load(file = paste0(SETTINGS$path_data, "tsdata.rda"))

##################################################################
## prepare normalized estimators
nest <- normalize_df(d_withoutl$est)
napp <- normalize_df(d_withoutl$app)

##################################################################
## prepare standardized estimators
sest <- standardize_df(d$est)
sapp <- standardize_df(d$app)

##################################################################
## prepare descriptive tables over raw, normalized and standardized data
## without counting *_naive duplicated 
                                        # initialize
desc <- list()

                                        # for raw estimators and approximators
desc$est_and_app      <- dscrbe(est_data   = d_withoutl$app,
                                app_data   = d_withoutl$est,
                                other_data = NULL,
                                col_excl   = "v_app_naive")

                                        # for normalized estimators and approximators
desc$nest_and_napp    <- dscrbe(est_data   = nest,
                                app_data   = napp,
                                other_data = NULL,
                                col_excl   = "v_app_naive")

                                        # for standardized estimators and approximators
desc$sest_and_sapp    <- dscrbe(est_data   = sest,
                                app_data   = sapp,
                                other_data = NULL,
                                col_excl   = "v_app_naive")

                                        # for other raw data
other <- get_sub_df(df       = d$all,
                    pattern  = c("v_app",
                                 "v_est",
                                 "m_total",
                                 "m_circ",
                                 "tx_vol_infl",
                                 "tx_vol_clean",
                                 "price_usd"),
                    type     = "match")
desc$other    <- dscrbe(est_data   = NULL,
                        app_data   = NULL,
                        other_data = other,
                        col_excl   = "v_app_naive")
vars_scaled <- c("$M_{\\mathtt{circMcaFifo}}$",                
                 "$M_{\\mathtt{circMcaLifo}}$",
                 "$M_{\\mathtt{circWba}}$",
                 "$M_{\\mathtt{total}}$",
                 "$\\mathtt{Vol.(deflated)}$",
                 "$\\mathtt{Vol.(inflated)}$")
rown_to_mod <- rownames(desc$other) %in% vars_scaled
rownames(desc$other)[rown_to_mod] <- paste0("$ ^{\\ast} ", str_sub(rownames(desc$other),
                                                                  start=2))[rown_to_mod]


## Write descriptive table to tex
texmake_desc(desc = desc)


