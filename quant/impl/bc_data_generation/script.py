# !/usr/bin/env python3
# -------> Note: Please start from the directory "crypto_velocity/quant/impl/"
# --> own imports
from helpers  import setup_parse_args
from helpers  import setup_output_path
from helpers  import setup_logging
from velo     import Velo
from multiprocess_framework import Multiprocess
# <-- own imports


import blocksci
import operator
import time
import os
import pandas as pd
import logging
import argparse
import sys
import signal
import csv
import numpy as np
import hashlib
import multiprocessing
import threading
from datetime import date
from datetime import datetime
from datetime import timedelta
from numpy import concatenate
from pandas import DatetimeIndex
from multiprocessing import Process, Queue, JoinableQueue
from colorstrings import colorStrings as cs

#==[ register signal_handler to kill all subprocesses ]=========================
def signal_handler(sig, frame):
    mp.processes_kill_all()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

#==[ main function ]============================================================
def main():
    """
    """
    #--setup: parse commandline arguments---------------------------------------
    args = setup_parse_args()

    #--setup: logging and data output-------------------------------------------
    logger = setup_logging(
        logging,
        path_log=os.getcwd() + args.path_log + "velocity_data_log",
        log_level=args.log_level,
    )
    Multiprocess.logger = logger

    setup_output_path(args.path_data_output)

    #--setup: main objects for using BlockSci-----------------------------------
    Velo.setup(
        path_data_input=args.path_data_input,
        path_data_output=args.path_data_output,
        path_cluster=args.path_cluster,
        logger=logger,
        heur_input=args.heur_input,
        test=args.test,
        start_date=args.start_date,
        end_date=args.end_date,
        windows_for_competing_msrs=args.windows_for_competing_msrs,
        cnt_cls_only=args.count_clustering_only,
    )

    #--Retrieval of basic blockchain data, money supply and velocity measures---
    results = Multiprocess.get_data_for_df(
        args.start_date,
        args.end_date,
        int(args.period),
        args.test,
        args.log_level,
        int(args.cpu_count),
        args.path_data_output,
    )

    if args.test > 0: return
    elif args.test == -1:
        ress = results["process_id"]
        last_e = -1
        prt = ""
        for e in ress:
            if e != last_e +1:
                # Velo.logger.warning(
                #     "Out of order! (last_e, e) = ({:03}, {:03})".format(
                #         last_e,
                #         e,
                #     )
                # )
                break

            if e % 6 == 0 and e > 0:
                Velo.logger.info(prt)
                prt = ""
            prt += "Result of {:03} | ".format(ress[e])

            last_e += 1

        Velo.logger.info(prt)
        print("Exiting multiprocessing test")
        exit(0)

    #--retrieving data from all processes---------------------------------------
    Velo.get_results_of_processes(results)

    #--get csv of final pandas data frame---------------------------------------
    Velo.get_results_finalized()

    print("Exiting program")
    exit(0)

if __name__ == "__main__":
    main()
