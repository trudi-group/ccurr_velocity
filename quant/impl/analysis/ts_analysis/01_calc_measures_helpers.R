#' Helper function to scale a numeric vector with a multiplicator set in the gen. settings file and optionally calculation from BTC to Satoshis 
#' 
#' 
#' @param settings ... the path to the tsanalysis/SETTINGS.R config file.
#' @param type ... switch variable: currently only "satoshis_to_btc" as optinal scaling from Sathoshis to Bitcoin
#' @param inputvec ... a numeric vector to be scaled
#'
#' @return a scaled, numeric vector
#'
scaledown <- function(inputvec,
                      settings = SETTINGS,
                      type     = "satoshis_to_btc"){

    if(type == "satoshis_to_btc"){
        inputvec_scaled =  inputvec / (settings$scaling_multiplicator * settings$btc_per_satoshi)
    }else if(type == "no_exchangerate"){
        inputvec_scaled =  inputvec / settings$scaling_multiplicator
    }
    
    return(inputvec_scaled)
}
