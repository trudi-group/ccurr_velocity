#' Function getting converting a customized dataframe with descriptives to latex code  
#' 
#' 
#' @param SETTINGS ... the path to the tsanalysis/SETTINGS.R config file.
#' @param df_melted ... dataframes produced with the melt function of the reshape packae.
#' @param xaxis_legend ... legend of the x-axis
#' @param yaxis_legend ... legend of the y-axis
#' @param storage_name ... path to store figure 
#' @param legendrows ... number of rows in which to store legend
#' @param palette ... color palette
#' 
#'
#' @return ... -
#' 
make_plot_desc <- function(SETTINGS,
                           df_melted,
                           xaxis_legend,
                           yaxis_legend,
                           storage_name,
                           legendrows = 2,
                           palette = SETTINGS$palette_3,
                           diagram_title){
    if(SETTINGS$titles_for_plots == FALSE){diagram_title <- NULL}
    tikz(file = paste0(SETTINGS$path_figs, storage_name, ".tex"),
         width = 3.5,
         height = 3.5)
    print(
        ggplot(df_melted, aes(x=Date, y=value, col=variable)) +
        geom_line()+
        ggtitle(diagram_title)+
        xlab(xaxis_legend) +
        ylab(yaxis_legend)+
        scale_colour_manual(name = FALSE, values=palette) +
        theme(legend.position="bottom",
              plot.background = element_rect(fill = "white"),
              panel.background = element_rect(fill = "white"),
              axis.line.x = element_line(color = "black"),
              panel.grid.minor = element_line(size = 0.25, linetype = 'solid', colour = "grey"),
              legend.title=element_blank()) +
        guides(colour=guide_legend(nrow=legendrows,byrow=TRUE))
        
    )
    dev.off()
}

