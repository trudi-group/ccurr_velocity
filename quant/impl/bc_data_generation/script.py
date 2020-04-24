# !/usr/bin/env python3
# -------> Note: Please start from the directory "crypto_velocity/quant/impl/"
# --> own imports
from helpers  import parse_args
from helpers  import logging_set_log_level_formatting
from helpers  import logging_setup
from velo     import Velo
from multiprocess_framework import Multiprocess as mp
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

#===[ Logging setup ]===========================================================
logger = logging.getLogger(__name__)
logging_set_log_level_formatting(logging)

#===[ register signal_handler to kill all subprocesses ]========================
def signal_handler(sig, frame):
    mp.processes_kill_all()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

#===============================================================================
def retrieveMoneyVelocityMeasurements(
    start_date,
    end_date,
    period,
    test,
    log_level,
    cpu_cnt_manual,
    path_data_output,
):
    """
    Insert Desc:wription here
    """

    #--Retrieval of basic blockchain data for data frames-----------------------
    results = mp.get_data_for_df(
        start_date,
        end_date,
        period,
        test,
        log_level,
        cpu_cnt_manual,
        path_data_output,
    )

    if test>0:
        return

    elif test == -1:
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

        logger.info(prt)
        print("Exiting multiprocessing test")
        exit(0)

    #--retrieving data from all processes---------------------------------------
    Velo.get_results_of_processes(results)

    #--get csv of final pandas data frame---------------------------------------
    Velo.get_results_finalized()

    print ("Exiting program")
    return
#===============================================================================
def main():
# [ parse arguments ]-----------------------------------------------------------
    args             = parse_args()
    start_date       = args.start_date
    end_date         = args.end_date
    period           = int(args.period)
    log_level        = args.log_level
    path_log         = os.getcwd() + args.path_log + "velocity_data.log"
    path_data_input  = args.path_data_input
    path_data_output = args.path_data_output
    path_cluster     = args.path_cluster
    cpu_cnt_manual   = int(args.cpu_count)
    heur_input       = args.heur_input
    test             = int(args.test)
    date_format      = "%Y-%m-%d %H:%M:%S"
    time_windows     = args.windows_for_competing_msrs
    cnt_cls_only     = args.count_clustering_only


    #--check is path_data_output exists-----------------------------------------
    if not os.path.exists("{}_csv".format(path_data_output)):
        os.makedirs("{}_csv".format(path_data_output))
    if not os.path.exists("{}_ds".format(path_data_output)):
        os.makedirs("{}_ds".format(path_data_output))

    #--Logging setup------------------------------------------------------------
    logging_setup(
        logging,
        logger,
        path_log,
        log_level,
    )
    mp.logger   = logger
    Velo.logger = logger

    #-- initialize program:[Load main objects for using BlockSci ]--------------
    Velo.loadSession(
        path_data_input=path_data_input,
        path_data_output=path_data_output,
        path_cluster=path_cluster,
        logger=logger,
        heur_input=heur_input,
        test=test,
        date_format=date_format,
        start_date=start_date,
        end_date=end_date,
        windows_for_competing_msrs=time_windows,
        cnt_cls_only=cnt_cls_only,
    )

    #--Start test or normal application-----------------------------------------
    retrieveMoneyVelocityMeasurements(
        start_date=start_date,
        end_date=end_date,
        period=period,
        test=test,
        log_level=log_level,
        cpu_cnt_manual=cpu_cnt_manual,
        path_data_output=path_data_output,
    )

    exit(0)

if __name__ == "__main__":
    main()
