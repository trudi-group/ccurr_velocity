#!/usr/bin/env python3
# -------> Note: Please start from the directory "crypto_velocity/quant/impl/"
import operator
import blocksci
import os
import pandas as pd
import time
import logging
import argparse
import unittest
import random
import io
import sys
from datetime     import datetime
from datetime     import date
from datetime     import timedelta
from pandas       import read_csv
from math         import ceil
from math         import floor
from numpy        import concatenate
from numpy        import nditer
from numpy        import nan
from operator     import add
from numpy        import cumsum
from itertools    import compress
from colorstrings import colorStrings as cs

#===============================================================================
def logging_set_log_level_formatting(logging):
    """
    logging setup of log level formattings.
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

def logging_setup(
    logging,
    logger,
    path_log,
    log_level,
):
    out_hdlr = logging.StreamHandler(sys.stdout)
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
        filename=path_log,
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

    return
#===============================================================================
def parse_args (
):
    """
    Parse commandline arguments
    """
    parser = argparse.ArgumentParser(
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
        , '--heur_input'
        , action='store'
        , default="legacy_improved"
        , help="Sets Heuristic for change outputs"
    )
    parser.add_argument(
          '-p'
        , '--period'
        , action='store'
        , default=25
        , help="Set partitioning period in days"
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
        , '--windows_for_competing_msrs'
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
#===============================================================================
# def flatten(
#     lst
# ):
#     """
#     From:
#     https://stackoverflow.com/questions/44061355/flattening-list-of-lists
#     """
#     for el in lst:
#         if isinstance(el, list):
#             yield from el
#         elif isinstance(el, set):
#             yield from el
#         else:
#             yield el
#===============================================================================
# def ConcatElemToStr(
#     l1,
#     l2,
#     join
# ):
#     """
#     Concatenating the strings in lists of lists seperated by a "join"-symbol,
#     e.g. "-"
#     """
#     concat = [
#         m+join+n for m,
#         n in zip(
#             list(map(str, l1)),
#             list(map(str, l2))
#         )
#     ]
#     return concat
#===============================================================================
# class TestMethods(unittest.TestCase):
#     def test_date_range(self):
#         start_dt      = date.today().replace(day=1, month=1).toordinal()
#         end_dt        = date.today().toordinal()
#         start_dt_rand = date.fromordinal(random.randint(start_dt, end_dt))
#         end_dt_rand   = start_dt_rand
#         period_dt     = start_dt_rand
#
#         while True:
#             end_dt_rand = date.fromordinal(random.randint(start_dt, end_dt))
#
#             if start_dt_rand < end_dt_rand:
#                 start_date = str(start_dt_rand).replace("-", "/")
#                 end_date   = str(end_dt_rand).replace("-", "/")
#                 period_dt  = end_dt_rand - start_dt_rand
#                 break
#             if start_dt_rand > end_dt_rand:
#                 start_date = str(end_dt_rand).replace("-", "/")
#                 end_date   = str(start_dt_rand).replace("-", "/")
#                 period_dt  = start_dt_rand - end_dt_rand
#                 break
#
#         period = period_dt.days + 1
#
#         print(start_date)
#         print(end_date)
#         print(period)
#===============================================================================
# if __name__ == "__main__":
#     unittest.main()
