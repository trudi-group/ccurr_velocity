source("SETTINGS.R")
source("helpers.R")
source("03_make_desc_plot_helpers.R")
load(file = paste0(SETTINGS$path_data, "tsdata.rda"))


##################################################################
## construct melted plotting dataframes for ESTimators
v_est_melted     <- melt(rename_df(wDate(d$est)), id.var = "Date")
v_nest_melted    <- melt(rename_df(wDate(normalize_df(d$est))), id.var = "Date")
v_sest_melted    <- melt(rename_df(wDate(standardize_df(d$est))), id.var = "Date")
v_est_old_melted <- melt(rename_df(wDate(d$est_old)), id.var = "Date")
v_est_new_melted <- melt(rename_df(wDate(d$est_new)), id.var = "Date")

##################################################################
## construct plotting dataframes for APProximators
indic_excl_appnaive <- !colnames(d$app) %in% "v_app_naive"
v_app_melted        <- melt(rename_df(wDate(d$app[indic_excl_appnaive])), id.var = "Date")
v_napp_melted       <- melt(rename_df(wDate(normalize_df(d$app[indic_excl_appnaive]))), id.var = "Date")
v_sapp_melted       <- melt(rename_df(wDate(standardize_df(d$app[indic_excl_appnaive]))), id.var = "Date")
v_app_old_melted    <- melt(rename_df(wDate(d$app_old[indic_excl_appnaive])), id.var = "Date")
v_app_new_melted    <- melt(rename_df(wDate(d$app_new[indic_excl_appnaive])), id.var = "Date")

##################################################################
## construct plotting dataframes for other mixtures
                                        # ... for different trading volume data
vol_melted  <- melt(rename_df(wDate(get_sub_df(df      = d$all,
                                               pattern = c("tx_vol_infl", "tx_vol_clean"),
                                               type    = "exact"))), id.var = "Date")

nvol_melted <- melt(rename_df(wDate(normalize_df(get_sub_df(df      = d$all,
                                                            pattern = c("tx_vol_infl", "tx_vol_clean"),
                                                            type    = "exact")))), id.var = "Date")

svol_melted <- melt(rename_df(wDate(standardize_df(get_sub_df(df      = d$all,
                                                              pattern = c("tx_vol_infl", "tx_vol_clean"),
                                                              type    = "exact")))), id.var = "Date")

comp_melted  <- melt(rename_df(wDate(get_sub_df(df       = d$all,
                                                pattern  = c("tx_vol_clean", "m_total", "m_circ_wh_bill"),
                                                type     = "exact"))), id.var = "Date")

ncomp_melted <- melt(rename_df(wDate(normalize_df(get_sub_df(df       = d$all,
                                                             pattern  = c("tx_vol_infl", "tx_vol_clean"),
                                                             type     = "exact")))), id.var = "Date")

scomp_melted <- melt(rename_df(wDate(standardize_df(get_sub_df(df      = d$all,
                                                               pattern = c("tx_vol_infl", "tx_vol_clean"),
                                                               type    = "exact")))), id.var = "Date")


##################################################################
## How Do the new measures look like
                         # Old velocity estimates
make_plot_desc(SETTINGS      = SETTINGS,
               df_melted     = v_est_old_melted,
               xaxis_legend  = 'Date',
               yaxis_legend  = 'Est. avg. turnovers',
               diagram_title = "Old velocity estimates",
               storage_name  = "desc_old_raw")
                                        # New velocity estimates
make_plot_desc(SETTINGS      = SETTINGS,
               df_melted     = v_est_new_melted,
               xaxis_legend  = 'Date',
               yaxis_legend  = 'Est. avg. turnovers',
               diagram_title = "New velocity estimates",
               storage_name  = "desc_new_raw")
                                        # Components of velocity calculation
make_plot_desc(SETTINGS      = SETTINGS,
               df_melted     = comp_melted,
               xaxis_legend  = 'Date',
               yaxis_legend  = 'BTC (in M)',
               diagram_title = "Components of velocity calculation",
               storage_name  = "desc_components")
                                        # Old and new velocity estimates
make_plot_desc(SETTINGS      = SETTINGS,
               df_melted     = v_est_melted,
               xaxis_legend  = 'Date',
               yaxis_legend  = 'Est. avg. turnovers',
               diagram_title = "Old and new velocity estimates",
               storage_name  = "desc_all_est_raw",
               legendrows    = 3)
                                        # Old and new velocity approximations
make_plot_desc(SETTINGS      = SETTINGS,
               df_melted     = v_app_melted,
               xaxis_legend  = 'Date',
               yaxis_legend  = 'Norm. est. avg. turnovers',
               diagram_title = "Old and new velocity approximations",
               storage_name  = "desc_all_app_raw")
                                        # Normalized old and new velocity estimates
make_plot_desc(SETTINGS      = SETTINGS,
               df_melted     = v_nest_melted,
               xaxis_legend  = 'Date',
               yaxis_legend  = 'Norm. est. avg. turnovers',
               diagram_title = "Normalized old and new velocity estimates",
               storage_name  = "desc_all_est_norm",
               legendrows    = 3)
                                        # Normalized old and new velocity approximations
make_plot_desc(SETTINGS      = SETTINGS,
               df_melted     = v_napp_melted,
               xaxis_legend  = 'Date',
               yaxis_legend  = 'Norm. est. avg. turnovers',
               diagram_title = "Normalized old and new velocity approximations",
               storage_name  = "desc_all_app_norm")
                                        # Stnd. old and new velocity estimates
make_plot_desc(SETTINGS      = SETTINGS,
               df_melted     = v_sest_melted,
               xaxis_legend  = 'Date',
               yaxis_legend  = 'Stnd. est. avg. turnovers',
               diagram_title = "Stnd. old and new velocity estimates",
               storage_name  = "desc_all_est_stand",
               legendrows    = 3)
                                        # Stnd. old and new velocity approximations
make_plot_desc(SETTINGS      = SETTINGS,
               df_melted     = v_sapp_melted,
               xaxis_legend  = 'Date',
               yaxis_legend  = 'Stnd. est. avg. turnovers',
               diagram_title = "Stnd. old and new velocity approximations",
               storage_name  = "desc_all_app_stand")



