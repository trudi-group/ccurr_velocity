#!/usr/bin/env python3
# -------> Note: Please start from the directory "crypto_velocity/quant/impl/"
import logging
from os           import getcwd, makedirs
from os.path      import exists
from argparse     import ArgumentParser
from sys          import stdout
from colorstrings import ColorStrings as cs

#==[ parse arguments ]==========================================================
def setup_parse_args (
):
    """
    Parse commandline arguments
    """
    parser = ArgumentParser(
        description=
        'This program computes the possesion change rate of coins on the'
        + ' bitcoin blockchain.'
        )
    #if nargs=1 is enabled, arguments are passed as list instead of string
    parser.add_argument(
          '-s'
        , '--start_date'
        , action='store'
        , default="01/03/2009"
        , help="Analysis starting date"
    )
    parser.add_argument(
          '-e'
        , '--end_date'
        , action='store'
        , default="12/20/2018"
        , help="Analysis ending date"
    )
    parser.add_argument(
          '-l'
        , '--log_level'
        , action='store'
        , default=logging.INFO
        , help="Set logging level"
    )
    parser.add_argument(
          '-lp'
        , '--path_log'
        , action='store'
        , default="/log/" #works like ./log/
        , help="Logging path"
    )
    parser.add_argument(
          '-i'
        , '--path_data_input'
        , action='store'
        , default="/home/usr_btc/txdata"
        , help="Path to input blockchain data"
    )
    parser.add_argument(
          '-oc'
        , '--path_data_output'
        , action='store'
        , default="../../../data/bcdata_raw/archive"
        , help="Path to exported transactionswise database of BTC volumes"
    )
    parser.add_argument(
          '-he'
        , '--heur_choice'
        , action='store'
        , default="legacy_improved"
        , help="Sets Heuristic for change outputs"
    )
    parser.add_argument(
          '-t'
        , '--test'
        , action='store'
        , default=0
        , help="Set testing mode level"
    )
    parser.add_argument(
          '-cpu'
        , '--cpu_count'
        , action='store'
        , default=-1
        , help="set extra cpu core count manually for debugging"
    )
    parser.add_argument(
          '-wndw'
        , '--time_window'
        , action='store'
        , default="1"
        #, default="1,30,90"
        , help="look back time windows for dormancy and days destroyed in days"+
        " separated by commata, e.g., '20, 30, 90'"
    )
    parser.add_argument(
          '-c'
        , '--path_cluster'
        , action='store'
        , default="/home/usr_btc/cluster"
        , help="Path to clustering cache"
    )
    parser.add_argument(
              '-cls'
            , '--count_clustering_only'
            , action='store'
            , default=0
            , help="Toggle script for only counting adresses in clusters."
        )
    args = parser.parse_args()
    return(args)

#==[ logging and output helpers ]===============================================
def setup_logging(
    path_log,
    log_level,
):
    """
    Setup logging functionality.
    """
    def setup_logging_formating_per_log_level(logging):
        """
        Setup of log level formattings.
        """
        logging.addLevelName(
            logging.ERROR,   "{}{}  {}".format(
                cs.RED,
                logging.getLevelName(logging.ERROR),
                cs.BES,
            )
        )
        logging.addLevelName(
            logging.WARNING, "{}{}{}".format(
                cs.MAG,
                logging.getLevelName(logging.WARNING),
                cs.BES,
            )
        )
        logging.addLevelName(
            logging.INFO,    "{}       {}".format(
                cs.CYB,
                cs.BES,
            )
        )
        logging.addLevelName(
            logging.DEBUG,   "{}{}  {}".format(
                cs.CYA,
                logging.getLevelName(logging.DEBUG),
                cs.BES,
            )
        )

        return

    #--set formatting per log level---------------------------------------------
    setup_logging_formating_per_log_level(logging)

    #--setup logging------------------------------------------------------------
    logger = logging.getLogger(__name__)
    out_hdlr = logging.StreamHandler(stdout)
    out_hdlr.setFormatter(
        logging.Formatter(
              ''
            + '%(levelname)s'
            + ' '
            + cs.WHI
            + '%(message)s'
            + cs.RES
        )
    )
    out_hdlr.setLevel(log_level)
    logger.setLevel(log_level)
    logger.addHandler(out_hdlr)
    logging.basicConfig(
        filename=getcwd() + path_log,
        level=log_level
    )
    logging.basicConfig(
        format=
              '%(asctime)s'
            + '  %(levelname)-10s'
            + ' %(processName)s'
            + '  %(name)s'
            + ' %(message)s',
        datefmt="%Y-%m-%d-%H-%M-%S"
    )

    return logger

def setup_output_path (path_data_output):
    """
    Check whether output directories exist and if not, create them.
    """
    if not exists("{}_csv".format(path_data_output)):
        makedirs("{}_csv".format(path_data_output))

    if not exists("{}_ds".format(path_data_output)):
        makedirs("{}_ds".format(path_data_output))

#===============================================================================
if __name__ == "__main__":
    print("{}This is only a file with helper functions!{}".format(cs.RED,cs.RES))
    exit(0)
