# !/usr/bin/env python3
# -------> Note: Please start from the directory "crypto_velocity/quant/impl/"
#==[ import from own modules ]==================================================
from helpers                import setup_parse_args
from helpers                import setup_output_path
from helpers                import setup_logging
from velo                   import Velo
from multiprocess_framework import Multiprocess
from colorstrings           import ColorStrings as cs

#==[ all other imports ]========================================================
import logging
from signal          import signal, SIGINT
from multiprocessing import Process

#==[ register signal_handler to kill all subprocesses ]=========================
def signal_handler(sig, frame):
    Multiprocess.processes_kill_all()
    exit(0)

signal(SIGINT, signal_handler)

#==[ main function ]============================================================
def main():
    """
    """
    #--setup: parse commandline arguments---------------------------------------
    args = setup_parse_args()

    #--setup: logging and data output-------------------------------------------
    logger = setup_logging(
        path_log=args.path_log + "velocity_data_log",
        log_level=args.log_level,
    )

    setup_output_path(args.path_data_output)

    #--setup: main objects for using BlockSci-----------------------------------
    Velo.setup(
        logger=logger,
        args=args,
    )

    Multiprocess.setup(
        logger=logger,
        args=args
    )

    #--Retrieval of basic blockchain data, money supply and velocity measures---
    results_raw = Multiprocess.run()

    #--get csv of final pandas data frame---------------------------------------
    Velo.get_results_finalized(
        results_raw=results_raw,
        index_label="date",
    )

    print("Exiting program")
    exit(0)

if __name__ == "__main__":
    main()
