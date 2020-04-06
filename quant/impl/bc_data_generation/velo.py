import blocksci as bs
import logging
import csv
import numpy as np
import pandas as pd
import hashlib
import operator
import time
import threading
import os
import sys
import logging
import io
import random
import unittest
import dateparser
import multiprocessing
import threading

from multiprocessing        import Process, Queue, JoinableQueue, Pool, Lock, Value
from dateutil.relativedelta import relativedelta
from colorstrings           import colorStrings as cs
from datetime               import date
from datetime               import datetime
from datetime               import timedelta
from math                   import floor, ceil
from numpy                  import concatenate
from numpy                  import nditer
from numpy                  import nan
from operator               import add
from numpy                  import cumsum
from itertools              import compress
from itertools              import chain
from more_itertools         import sort_together

lock = Lock()

cluster_max_size_global = Value('i', 0)
cluster_max_id_global   = Value('i', 0)

def cls_worker(cls_range):
    cluster_max_size_local = 0
    cluster_max_id_local   = 0
    # skip assumingly max cluster, since its computation lasts to long
    arg_id, begin, end = cls_range

    for cluster_i in range(begin, end):
        if cluster_i == 32:
            continue;
        cluster_size = Velo.cluster_range[cluster_i].size()
        cluster_id   = Velo.cluster_range[cluster_i].index

        if cluster_i % 1000000 == 0:
            Velo.logger.info("{}[{}  clustering   {}]{}  {}".format(
                cs.RES,
                cs.PRGnBA,
                cs.RES,
                cs.PRGnBA,

                "{:3}: [{:9}/{:9}]".format(
                    arg_id,
                    cluster_i,
                    end,
                ),
            ))

        if cluster_size > cluster_max_size_local:
            cluster_max_size_local = cluster_size
            cluster_max_id_local   = cluster_i
            if Velo.cluster_range[cluster_i].size() > 999:
                Velo.logger.info("{}[{}  clustering   {}]{}  {}".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,

                    "{:3}: [{:9}/{:9}] index/size = {:9}/{:6}{}".format(
                        arg_id,
                        cluster_i,
                        end,
                        cluster_max_id_local,
                        cluster_max_size_local,
                        "---new found",
                    ),
                ))

    Velo.logger.info("{}[{}  clustering   {}]{}  {}".format(
        cs.RES,
        cs.PRGnBA,
        cs.RES,
        cs.PRGnBA,

        "{:3}: [{:9}/{:9}] index/size = {:9}/{:6}{}".format(
            arg_id,
            cluster_i,
            end,
            cluster_max_id_local,
            cluster_max_size_local,
            "===finished",
        ),
    ))

    with lock:
        if cluster_max_size_local > cluster_max_size_global.value:
            cluster_max_size_global.value = cluster_max_size_local
            cluster_max_id_global.value   = cluster_max_id_local

    return

class Velo:
    """
    Money-velocity calculation.
    This class provides calculation functionality to measure
    the money velocity on the bitcoin blockchain.
    """
    # date of bitcoin genesis
    start_date_gen    = "01/03/2009"
    # class attribute representing blocksci chain object used by all instances
    chain                 = None
    cluster_mgr           = None
    cluster_cnt           = None
    cluster_range         = None
    cluster_max_id        = None
    block_times           = None
    heur_select           = None
    heur_input            = None
    start                 = None
    end                   = None
    index_block_day       = []
    index_txes_day        = []
    index_day_block       = []
    txes_valout_agg_daily = []
    txes_number_daily     = []
    sub_proc_dates        = []
    # data structures conveyed by subprocess queues/used by data frame functions
    csupply_agg_cache = None
    queue_dict        = {}
    df_dict           = {}
    # remaining class attributes
    log_level         = logging.INFO
    path_data_output  = None
    logger            = None
    test_level        = 0
    process_cnt       = 0

    #0: [ CLASSLEVEL | SessionSetup & precaching of global data struct ]========


    def loadSession(
        path_data_input,
        path_cluster,
        logger,
        heur_input,
        test,
        date_format,
        start_date,
        end_date,
        windows_for_competing_msrs,
        cnt_cls_only,
     ):
        """
        Initialize session and with that, the main blockchain object used by
        each instance of the Velo class.
        """
        def coin_supply_renumeration(block_height):
            """
            supply calculation of BTC inspired by:
            [...] https://www.coindesk.com/making-sense-bitcoins-halving/
            """

            # the mining reward will be halved each 210000 blocks
            halving_interval = 210000
            #initial reward
            reward = 50*100000000

            if block_height < halving_interval:
                return(reward)

            halvings = floor(block_height / halving_interval)

            # if to much halvings (when using 64 bit integer since then,
            # right shifting (>>) will be undefined)
            if halvings >= 64:
                return(0)

            #using right shifts to devide by 2
            reward >>= halvings

            return(reward)

        def loadSession_heuristics():
            """
            compare
            https://citp.github.io/BlockSci/reference/heuristics/change.html
            *first* "ChangeHeuristic" objects need to be created
            *second* the objects can be combined
            combined objects form new "ChangeHeuristic" objects that can be
            called see *first*:
            """

            heur = []
            heur.append(bs.heuristics.change.address_reuse())
            heur.append(bs.heuristics.change.address_type())
            heur.append(bs.heuristics.change.client_change_address_behavior())
            heur.append(bs.heuristics.change.legacy())
            heur.append(bs.heuristics.change.locktime())
            heur.append(bs.heuristics.change.optimal_change())
            heur.append(bs.heuristics.change.peeling_chain())
            heur.append(bs.heuristics.change.power_of_ten_value())

            # give the objects in the list names
            heur_names = [
                "address_reuse",
                "address_type",
                "client_change_address_behavior",
                "legacy",
                "locktime",
                "optimal_change",
                "peeling_chain",
                "power_of_ten_value"
            ]
            heur = dict(zip(heur_names, heur))

            # see *second*
            heur_select = []
            heur_select.append(heur["legacy"])
            heur_select.append(
                # heur["address_type"].__or__(
                heur["legacy"].__or__(
                    heur["peeling_chain"]
                )
                # )
            )
            # heur_select.append(heur["address_reuse"].__or__(
            #     heur["address_type"].__or__(
            #         heur["client_change_address_behavior"].__or__(
            #             heur["legacy"].__or__(
            #                 heur["locktime"].__or__(
            #                     heur["optimal_change"].__or__(
            #                         heur["peeling_chain"].__or__(
            #                             heur["power_of_ten_value"]))))))))

            heur_ret = dict(zip(
                ["legacy", "legacy_improved"],
                heur_select
            ))

            Velo.heur_select = heur_ret
            return

        def get_coin_supply_agg(agg = True):
            """
            Precompute cumulated/aggregated coin supply for full chain
            """
            coin_supply_agg_str_a = "Calculating cumulative coin supply"
            coin_supply_agg_str_b = "for each Tx over full chain"
            Velo.logger.info("{}[{}coin_supply_agg{}]{}  {} {}".format(
                cs.RES,
                cs.PRGnBA,
                cs.RES,
                cs.PRGnBA,
                coin_supply_agg_str_a,
                coin_supply_agg_str_b,
            ))

            last_block = Velo.chain[-1]
            block_height_range_max = last_block.height

            coin_supply_agg = []
            for block_height in range(0,add(block_height_range_max,1)):
                coin_supply_agg.append(coin_supply_renumeration(block_height))

            if agg == True:
                coin_supply_agg = cumsum(coin_supply_agg)

            Velo.csupply_agg_cache = coin_supply_agg

            return

        def count_clustering(
            skip = False,
            overwrite = False
        ):
            Velo.cluster_max_size = 0
            Velo.cluster_max_id   = 0
            Velo.cluster_mgr      = bs.cluster.ClusterManager(
                path_cluster,
                Velo.chain,
            )

            # return assumingly largest cluster, when skip is on
            if True == skip:
                Velo.cluster_max_id   = 32
                # Velo.cluster_max_id   = 170740980
                # Velo.cluster_max_size = 133387
                Velo.logger.info("{}[{}  clustering   {}]{}  {}".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,
                    "Actually used max cluster: (with id/length {}/{})".format(
                        Velo.cluster_max_id,
                        Velo.cluster_max_size,
                    ),
                ))
                if cnt_cls_only != 0:
                    exit(0)

                return

            Velo.logger.info("{}[{}  clustering   {}]{}  Start".format(
                cs.RES,
                cs.PRGnBA,
                cs.RES,
                cs.PRGnBA,
            ))

            if overwrite == True:
                Velo.cluster_mgr = bs.cluster.ClusterManager.create_clustering(
                     path_cluster,
                     Velo.chain,
                     Velo.heur_select[Velo.heur_input],
                     overwrite,
                )

            #--get largest cluster
            Velo.cluster_range = Velo.cluster_mgr.clusters()
            Velo.cluster_cnt   = len(Velo.cluster_range)
            sub_proc_cls_range = ceil(
                Velo.cluster_cnt/(multiprocessing.cpu_count())
            )
            Velo.logger.info("{}[{}  clustering   {}]{}  Number of clusters per subprocess/in total: {}/{}".format(
                cs.RES,
                cs.PRGnBA,
                cs.RES,
                cs.PRGnBA,
                sub_proc_cls_range,
                Velo.cluster_cnt
            ))

            cls_args = []
            begin = 0
            end   = 0
            for cpu_i in range (0, multiprocessing.cpu_count()):
                begin = sub_proc_cls_range * cpu_i
                end   = sub_proc_cls_range * (cpu_i+1) - 1
                if end > Velo.cluster_cnt:
                    end = Velo.cluster_cnt -1
                cls_arg = (cpu_i, begin, end)
                cls_args.append(cls_arg)

            p = multiprocessing.Pool(multiprocessing.cpu_count())
            p.map(cls_worker, cls_args)

            Velo.cluster_max_id   = cluster_max_id_global.value
            Velo.cluster_max_size = cluster_max_size_global.value

            # Hardcoded result. Only change if parsing/clustering changes
            # Velo.cluster_max_id = 32

            Velo.logger.info("{}[{}  clustering   {}]{}  {}".format(
                cs.RES,
                cs.PRGnBA,
                cs.RES,
                cs.PRGnBA,
                "Finished (largest cluster id/size {}/{})".format(
                    Velo.cluster_max_id,
                    Velo.cluster_max_size,
                ),
            ))

            Velo.logger.info("{}[{}  clustering   {}]{}  End".format(
                cs.RES,
                cs.PRGnBA,
                cs.RES,
                cs.PRGnBA,
            ))

            if cnt_cls_only != 0:
                exit(0)

            return

        #--Setting globally used static variables-------------------------------
        Velo.logger       = logger
        Velo.heur_input   = heur_input
        Velo.test_level   = test
        Velo.date_format  = date_format
        Velo.start_date   = start_date
        Velo.end_date     = end_date
        Velo.time_windows = list(
            map(int, str(windows_for_competing_msrs).split(","))
        )
        time_windows_len  = len(Velo.time_windows)

        if test > 0:
            Velo.end_date = "01/01/2012"

        #--load chain object----------------------------------------------------
        Velo.chain = bs.Blockchain(path_data_input)

        #--load heurictics object-----------------------------------------------
        loadSession_heuristics()

        #--load clustering object-----------------------------------------------
        count_clustering(
            skip=True,
            overwrite=False
        )

        #--print basic data in debug mode---------------------------------------
        Velo.logger.debug(
            "date of first block = {}".format(Velo.chain[0].time)
        )
        Velo.logger.debug(
            "date of last block  = {}".format(Velo.chain[-1].time)
        )

        #--load index_block_day, index_txes_day & daily_outputs with time_window

        Velo.logger.info("{}[{}daily txesIndex{}]{}  Loading".format(
            cs.RES,
            cs.PRGnBA,
            cs.RES,
            cs.PRGnBA,
        ))

        Velo.block_times = pd.DataFrame(
            [block.time for block in Velo.chain],
            columns = ["date"],
        )
        Velo.block_times["height"] = Velo.block_times.index
        Velo.block_times.index = Velo.block_times["date"]
        del Velo.block_times["date"]

        cnt_blocks = Velo.block_times[
            Velo.block_times.index >= pd.to_datetime(Velo.end_date)
        ].iloc[0][0]

        cnt_days = (
            pd.to_datetime(Velo.end_date) - pd.to_datetime(Velo.start_date_gen)
        ).days

        block_height_min = Velo.block_times[
            Velo.block_times.index >= pd.to_datetime(Velo.start_date_gen)
        ].iloc[0][0]

        block_height_max = Velo.block_times[
            Velo.block_times.index >= pd.to_datetime(Velo.end_date)
        ].iloc[0][0]

        cnt_txes = 0
        for i_bh in range(block_height_min, block_height_max):
            cnt_txes += Velo.chain[i_bh].tx_count

        day_date      = pd.to_datetime(Velo.start_date_gen)
        day_date_next = day_date

        sub_proc_tx_num_max  = ceil(cnt_txes/(multiprocessing.cpu_count()))
        #sub_proc_tx_num_max  = 4000000
        sub_proc_tx_counter  = 0
        sub_proc_date_start  = day_date
        sub_proc_date_end    = day_date + timedelta(days=1)
        sub_proc_date_period = 1

        for day in range(cnt_days):
            # transform date variables------------------------------------------
            # day_date_net_prev = day_date_next
            day_date = day_date_next
            day_date_next += relativedelta(days=1)

            # initialize daily aggregated valouts per time_window---------------
            Velo.txes_valout_agg_daily.append([])
            txes_valout_agg_daily_sum = 0
            txes_number_per_day = 0

            # get minimum and maximum block_height according to actual day------
            block_height_min = Velo.block_times[
                Velo.block_times.index >= day_date
            ].iloc[0][0]

            block_height_max = Velo.block_times[
                Velo.block_times.index >= day_date_next
            ].iloc[0][0]

            # retrieve values per block in daily blockrange---------------------
            for i_bh in range(block_height_min, block_height_max):
                Velo.index_block_day.append(day)
                block = Velo.chain[i_bh]

                # block_tx_count = 0
                block_tx_count = block.tx_count
                txes_valout_agg_daily_sum += block.output_value

                for tx_i in range(block_tx_count):
                    Velo.index_txes_day.append(day)

                txes_number_per_day += block_tx_count

            Velo.index_day_block.append(block_height_min)
            Velo.txes_number_daily.append(txes_number_per_day)
            Velo.txes_valout_agg_daily[day].append(txes_valout_agg_daily_sum)

            # calculate data for sub processing periods-------------------------
            # Assumption: There is no day with cnt_txes > sub_proc_tx_num_max
            if day == 0:
                # txes number of first day, don't change dates
                sub_proc_tx_counter = txes_number_per_day

            else:
                # txes numbers of all other days
                tx_counter_next = sub_proc_tx_counter + txes_number_per_day

                if tx_counter_next < sub_proc_tx_num_max:
                    sub_proc_date_end    += timedelta(days = 1)
                    sub_proc_date_period += 1
                    sub_proc_tx_counter   = tx_counter_next

                else:
                    sub_proc_date = [
                        sub_proc_date_start,
                        sub_proc_date_end,
                        sub_proc_date_period,
                        sub_proc_tx_counter
                    ]
                    Velo.sub_proc_dates.append(sub_proc_date)
                    Velo.logger.debug("{}: {}".format(day, sub_proc_date))

                    sub_proc_date_start  = sub_proc_date_end
                    sub_proc_date_end    = sub_proc_date_start + timedelta(days=1)
                    sub_proc_date_period = 1
                    sub_proc_tx_counter  = txes_number_per_day

                if day == (cnt_days-1):
                    sub_proc_date = [
                        sub_proc_date_start,
                        sub_proc_date_end,
                        sub_proc_date_period,
                        sub_proc_tx_counter
                    ]
                    Velo.sub_proc_dates.append(sub_proc_date)
                    Velo.logger.debug(
                        "{}: {} (last)".format(day, sub_proc_date)
                    )

            # add sum of agg daily valouts of n days, n in Velo.time_windows----
            for t_w in range(1, time_windows_len):
                Velo.txes_valout_agg_daily[day].append(0)
                valout_agg_t_w_last = 0
                if day > 0:
                    valout_agg_t_w_last = Velo.txes_valout_agg_daily[day-1][t_w]

                # add the current daily calculations
                txes_valout_agg_for_t_w = (
                    valout_agg_t_w_last + txes_valout_agg_daily_sum
                )
                # substract the calculations right before the current window
                if day >= Velo.time_windows[t_w]:
                    txes_valout_agg_for_t_w -=  Velo.txes_valout_agg_daily[
                        day - Velo.time_windows[t_w]
                    ][0]

                Velo.txes_valout_agg_daily[day][t_w] = txes_valout_agg_for_t_w

        #--Call remaining subfunctions------------------------------------------


        get_coin_supply_agg()

        return

    #2: [ CLASSLEVEL | Retrieval & transform-functions for data frames ]========
    def get_df(
        type             = "",
        queue            = None,
        df               = None,
        df_type          = None,
        export_filename  = "",
        index_label      = "",
        path_data_output = "",
    ):
        """
        Wrapper function for get_df_* functions.
        """
        def get_df_to_csv(
            df,
            df_type,
            export_filename,
            index_label,
        ):
            """
            Export df as csv.
            """
            df.to_csv(
                export_filename,
                sep=",",
                header=True,
                date_format=Velo.date_format,
                index_label=index_label,
            )

            Velo.logger.info(
                "{}{}[{}   wrote csv   {}{}]  dfFrameType -- {}".format(
                    cs.RES,
                    cs.WHI,
                    cs.PRGnBI,
                    cs.RES,
                    cs.WHI,
                    df_type
                )
            )

            return

        def get_df_txes():
            """
            Builds a pandas data frame from pre-computed data.

                     used:             |       freed:
              Velo.txes_csupply_agg    |  Velo.txes_csupply_agg
              Velo.issues_in_tx_chouts |  Velo.issues_in_tx_chouts
              Velo.index_txes          |  Velo.index_txes
              Velo.txes_block_time     |  ----------------------

              return Velo.df_txes
            """

            df_txes = pd.DataFrame(
                {
                    #{}"index_txes"           : Velo.queue_dict["index_txes"],
                    "block_day_index"      : Velo.queue_dict["index_txes_day"],
                    "block_time"           : Velo.queue_dict["txes_block_time"],
                    "tx_vol"               : Velo.queue_dict["txes_valout"],
                    "tx_number"            : Velo.queue_dict["txes_number"],
                    "tx_fees"              : Velo.queue_dict["txes_fees"],
                    "tx_vol_issues_chouts" : Velo.queue_dict["issues_in_tx_chouts"],
                    "m_total"              : Velo.queue_dict["txes_csupply_agg"],
                }
                ,index=Velo.queue_dict["index_txes"]
            )

            #here, we don't need a queue since this functions runs in the main
            #process
            Velo.df_dict["txes"] = df_txes

            msg_str = "1/4    df_txes build"
            Velo.logger.info("{}{}[{}build dataframe{}{}]  {}".format(
                cs.RES,
                cs.WHI,
                cs.PRGnBI,
                cs.RES,
                cs.WHI,
                msg_str,
            ))

            return

        def get_df_m_circ(queue):
            """
            Get circulating supply for each day
            return: pandas DataFrame
                   used           |   freed (directly after return, not here)
              Velo.m_circ_wh_bill | Velo.m_circ_wh_bill
              Velo.m_circ_mc_lifo | Velo.m_circ_mc_lifo
              Velo.m_circ_mc_fifo | Velo.m_circ_mc_fifo
              Velo.sdd_daily      | -------------------------
              Velo.dormancy_daily | -------------------------
            """
            def df_m_circ(
                df_type,
                m_circ_def = True,
                non_def_type = "",
            ):
                if m_circ_def == True:
                    df_m_circ = pd.DataFrame(
                        {
                            df_type : Velo.queue_dict[df_type],
                        },
                        index=Velo.queue_dict["index_day"]
                    )

                else:
                    df_m_circ = pd.DataFrame(
                        Velo.queue_dict[df_type],
                        columns = [
                            "_{}_{}".format(
                                non_def_type,
                                w,
                            ) for w in Velo.time_windows
                        ],
                        index=Velo.queue_dict["index_day"]
                    )

                Velo.logger.info(
                    "{}{}[{}build dataframe{}{}]  2/4    {}{}{}".format(
                        cs.RES,
                        cs.WHI,
                        cs.PRGnBI,
                        cs.RES,
                        cs.WHI,
                        "df_dict[\"",
                        df_type,
                        "\"]",
                    )
                )

                # Also free here since since function is run in subprocess with
                # separate, copied memory
                del Velo.queue_dict[df_type]

                return df_m_circ

            def get_comp_meas_from_summands(
                sdd_summands,
                dormancy_summands,
                min_frac = 1,
            ):
                """
                Function using the results form get_comp_meas_summands to
                aggregate them after their results have been joined after the
                threading. Here lastly the measures dormancy and
                satoshi days destroyed (ssd) are created.
                """

                def cumsum_with_window_reverse(
                    l,
                    window,
                    min_frac = min_frac,
                ):
                    """
                    Sub-function that calculates the aggregate sum over the past
                    window size. As our lists start with the earliest date, the
                    lists are reversed and after the accumulation re-reversed.
                    This makes sense, as we want the lookback window to look
                    backwards and not in the future, so that we get missing values
                    in the earliest dates.
                    """
                    # reverse the list, as we need to start with the latest date
                    l.reverse()
                    # set minimum periods necessary for cumsum
                    min_periods = int(window*min_frac)
                    # convert list to pandas df
                    df = pd.DataFrame(l)
                    # calculate cumsum with lookback window
                    df = df.rolling(
                        window = window,
                        min_periods = min_periods,
                    ).sum().shift(-(window-1))
                    # convert pandas df back to list
                    l = list(chain(*df.values.tolist()))

                    # reverse the list again, as we have to put back the list...
                    # starting with the earliest date
                    l.reverse()
                    return l

                time_windows     = Velo.time_windows
                time_windows_len = len(time_windows)

                # (C1 & C2:)
                dormancy_summands = list(zip(*dormancy_summands))
                sdd_daily      = []
                dormancy_daily = []
                for i in range(time_windows_len):
                    # (C1):
                    sdd_daily.append(
                        cumsum_with_window_reverse(
                            l = list(sdd_summands),
                            window = time_windows[i],
                        )
                    )
                    # (C2):
                    dormancy_daily.append(
                        cumsum_with_window_reverse(
                            l = list(dormancy_summands[i]),
                            window = time_windows[i],
                        )
                    )

                Velo.queue_dict["sdd_daily"]      = list(zip(*sdd_daily))
                Velo.queue_dict["dormancy_daily"] = list(zip(*dormancy_daily))

                return

            get_comp_meas_from_summands(
                sdd_summands = Velo.queue_dict["summands_dsls_daily"],
                dormancy_summands = Velo.queue_dict["summands_dsls_daily_wghtd"],
                min_frac = 1,
            )

            #--free used data structures----------------------------------------
            del Velo.queue_dict["summands_dsls_daily"]
            del Velo.queue_dict["summands_dsls_daily_wghtd"]

            #--handle m_circ df_types-------------------------------------------
            m_circ_dict = {}
            m_circ_dict["m_circ_wh_bill"] = df_m_circ(
                "m_circ_wh_bill",
                False,
                "m_circ_wh_bill",
            )
            m_circ_dict["m_circ_mc_lifo"] = df_m_circ(
                "m_circ_mc_lifo",
                False,
                "m_circ_mc_lifo",
            )
            m_circ_dict["m_circ_mc_fifo"] = df_m_circ(
                "m_circ_mc_fifo",
                False,
                "m_circ_mc_fifo",
            )
            m_circ_dict["dormancy_daily"] = df_m_circ(
                "dormancy_daily",
                False,
                "dormancy",
            )
            m_circ_dict["sdd_daily"]      = df_m_circ(
                "sdd_daily",
                False,
                "sdd",
            )

            queue.put(m_circ_dict)

            return

        def get_df_agg_by_sum(queue):
            """
            Get aggregated time series data from given tx-wise data

                   used            |     freed (directly after return!)
              Velo.txes_block_time | Velo.txes_block_time
              Velo.df_txes         | Velo.df_txes
              Velo.index_day       | Velo.index_day

            return Velo.df_txes_agg_by_sum
            """
            ddf_txes = Velo.df_dict["txes"]
            ddf_txes["block_time"] = ddf_txes["block_time"].dt.date

            df_txes_agg_by_sum = ddf_txes.groupby(
                ['block_day_index']
            ).agg({
                'block_time'           : 'first',
                'tx_vol'               : 'sum',
                'tx_number'            : 'sum',
                'tx_fees'              : 'sum',
                'tx_vol_issues_chouts' : 'sum',
                'm_total'              : 'first',
            })

            # Translating block_day_index to date to have the same index as
            # in the dataframes to merge in.
            index_new = [Velo.queue_dict["index_day"][i]
            for i in df_txes_agg_by_sum.index]
            df_txes_agg_by_sum.index = index_new

            queue.put(df_txes_agg_by_sum)

            grouped_str = "3/4    df_txes_agg_by_sum grouped by daily tx index"
            Velo.logger.info("{}{}[{}build dataframe{}{}]  {}".format(
                cs.RES,
                cs.WHI,
                cs.PRGnBI,
                cs.RES,
                cs.WHI,
                grouped_str,
            ))

            return

        def get_df_merge():
            """
            Add coin in circulation measures by merging data frames.

                used:                        |   freed (right here)
              Velo.df_dict["m_circ_wh_bill"] | Velo.df_dict["m_circ_wh_bill"]
              Velo.df_dict["m_circ_mc_lifo"] | Velo.df_dict["m_circ_mc_lifo"]
              Velo.df_dict["m_circ_mc_fifo"] | Velo.df_dict["m_circ_mc_fifo"]

            return Velo.df_dict["txes_agg_by_sum"]
            """
            def df_merge(
                df_type,
                df_merge_targ = "txes_agg_by_sum",
            ):
                """
                """
                Velo.df_dict[df_merge_targ] = Velo.df_dict[df_merge_targ].merge(
                    Velo.df_dict[df_type],
                    how='outer',
                    left_index=True,
                    right_index=True
                )

                del Velo.df_dict[df_type]

                Velo.logger.info(
                    "{}{}[{}build dataframe{}{}]  4/4    {}{}{}{}{}".format(
                    cs.RES,
                    cs.WHI,
                    cs.PRGnBI,
                    cs.RES,
                    cs.WHI,
                    "df_dict[\"",
                    df_merge_targ,
                    "\"] merged with df_dict[\"",
                    df_type,
                    "\"]",
                    )
                )

                return

            #--handle m_circ df_types-------------------------------------------
            df_merge("m_circ_wh_bill")
            df_merge("m_circ_mc_lifo")
            df_merge("m_circ_mc_fifo")
            df_merge("dormancy_daily")
            df_merge("sdd_daily")

            return

        if type == "csv":
            get_df_to_csv(
                df,
                df_type,
                export_filename,
                index_label,
            )
            return

        if type == "txes":
            get_df_txes()
            return

        if type == "mc":
            get_df_m_circ(queue)
            return

        if type == "agg":
            get_df_agg_by_sum(queue)
            return

        if type == "merge":
            get_df_merge()
            return

        return

    #   [ CLASSLEVEL | Get combined results of all subprocesses ]===============
    def get_results_of_processes(results_from_processes):
        """
        Get the multiprocessed results and set them to globally used
        class variables.
        """
        del Velo.index_block_day

        Velo.queue_dict["index_txes_day"] = Velo.index_txes_day

        del Velo.index_txes_day

        Velo.queue_dict.update(results_from_processes)

        return

    #--PUBLIC INSTANCE-LEVEL METHODS--##########################################
    #0: [ INSTALEVEL | Initialize instances ]===================================
    def __init__ (
        self,
        process_id,
        process_name,
        queue,
        date_id,
    ):
        self.process_id   = process_id
        self.process_name = process_name
        self.__queue      = queue
        self.wait         = True

        # next day to include date_period_end. Otherwise, it won't be regarded
        # due to the blocksci chainrange being computed as the daily difference.
        s_p_d = Velo.sub_proc_dates

        date_period_start        = s_p_d[date_id][0]
        date_period_end          = s_p_d[date_id][1] - timedelta(days=1)
        date_period              = s_p_d[date_id][2]
        self.__date_id           = date_id
        self.__date_period_start = date_period_start
        self.__date_period_end   = date_period_end
        self.__date_period       = date_period
        self.__start_date        = date_period_start.strftime("%m/%d/%y")
        self.__end_date          = date_period_end.strftime("%m/%d/%y")
        self.__end_date_next     = date_period_end + timedelta(days=1)

        # instance-wise interfunctional temporary helper stuctures
        self.__txes_daily  = None
        self.__txes_chouts = None
        self.__txes_number = s_p_d[date_id][3]

        # data structures conveyed by subprocess queues
        self.__queue_dict = {}

    #1: [ INSTALEVEL | Main Script - Standard info, M & TP and TP adjustments ]=
    def run(self):
        """
        Run the thread
        """
        def in_max_cluster(out):
            """
            This function checks whether a given output (out) belongs to the
            biggest cluster of the applied change heuristic.
            """

            out_addr   = out.address
            out_cls    = Velo.cluster_mgr.cluster_with_address(out_addr)
            out_cls_id = out_cls.index

            if Velo.cluster_max_id == out_cls_id:
                return True

            return False

        def get_basic_tx_data():
            """
            Get for all transactions between given start and end date
            (as private variables):
            - transaction index
            - transaction block time
            - output values of transactions
            - aggregated coin supply from cached values
              (Velo.csupply_agg_cache)
            - transaction changeouts (chouts): value of all txes to change
              address, aka. change outputs for tx. (heuristics are applied)
            - transaction peeling: value of all transactions that where spend
              very soon
            """
            def retrieve_per_tx_daily(
                i_day,
                tx,
            ):
                """
                """
                try:
                    txes_daily[i_day].append(tx)
                except IndexError as error:
                    Velo.logger.error(
                        "{}{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}".format(
                            "{}[{}{}/{:03}{}]".format(
                                cs.RES,
                                cs.WHI,
                                self.process_name,
                                Velo.process_cnt-1,
                                cs.RES,
                            ),
                            cs.WHI,
                            "        i_day             = {}".format(i_day),
                            "        day_diff_to_start = {}".format(day_diff_to_start),
                            "        day_index         = {}".format(day_index),
                            "        date_period_start = {}".format(self.__date_period_start),
                            "        block_time        = {}".format(tx.block_time),
                            "        tx.hash           = {}".format(tx.hash),
                            "        is coinbase?      = {}".format(tx.is_coinbase),
                            error,
                        )
                    )
                    exit(-1)

                return

            def retrieve_per_tx_dummy(tx):
                if tx.block_height >= len(Velo.index_block_day):
                    return

                txes_block_heights.append(tx.block_height)
                index_txes.append(tx.index)
                txes_valout.append(tx.output_value)
                txes_block_time.append( str(tx.block_time) )

                return

            def retrieve_per_tx(tx):
                """
                Does the basic retrieval of data, but only for one tx.
                """
                if tx.block_height >= len(Velo.index_block_day):
                    return

                txes_block_heights.append(tx.block_height)
                index_txes.append(tx.index)
                txes_valout.append(tx.output_value)
                txes_block_time.append( str(tx.block_time) )

                val_chouts = 0
                for out in Velo.heur_select[Velo.heur_input].change(tx):
                    if False == in_max_cluster(out):
                        val_chouts += int(out.value)
                        # out_addr   = out.address
                        # out_cls    = Velo.cluster_mgr.cluster_with_address(out_addr)
                        # out_cls_id = out_cls.index
                        # Velo.logger.info("out_cls_id = {:9} added".format(out_cls_id))


                txes_chouts.append(val_chouts)

                # Calculate transaction numbers---------------------------------
                txes_number.append(1)

                # Caclulate fees------------------------------------------------
                txes_fees.append(tx.fee)

                # Calculate fees & aggr. input valuesof dust transactions-------
                dustfees   = 0
                dustinpval = 0

                if tx.output_value <= tx.fee:
                    dustfees   = tx.fee
                    dustinpval = tx.input_value

                txes_dustfees.append(dustfees)
                txes_dustinpval.append(dustinpval)

                return

            def retrieve_per_block(
                i_day,
                block,
            ):
                """
                Blockwise wrapper for retrieve_per_tx
                """
                for tx in block:
                    retrieve_per_tx(tx)

                    retrieve_per_tx_daily(i_day, tx)

                return

            #--print process status message-------------------------------------
            Velo.logger.info(
                "{}{}  {} [{}--{}, {:04d}], {}".format(
                    "{}[{}{}/{:03}{}]{}".format(
                        cs.RES,
                        cs.PRGnBA,
                        self.process_name,
                        Velo.process_cnt-1,
                        cs.RES,
                        cs.RES,
                    ),
                    cs.WHI,
                    "Loading basic tx data of period",
                    self.__start_date,
                    self.__end_date,
                    self.__date_period,
                    "txes_number = {:07d}".format(self.__txes_number),
                )
            )

            #--initialize data structures---------------------------------------
            txes_block_heights = []
            index_txes         = []
            txes_valout        = []
            txes_block_time    = []
            txes_chouts        = []
            txes_number        = []
            txes_fees          = []
            txes_dustfees      = []
            txes_dustinpval    = []
            txes_daily         = []
            index_day          = []
            txes_csupply_agg   = []

            # retrieve txes and values per block in process period--------------
            txes_num          = self.__txes_number
            day_date          = self.__date_period_start
            day_date_next     = day_date
            day_diff_to_start = (
                pd.to_datetime(day_date) -
                pd.to_datetime(Velo.start_date_gen)
            ).days

            for i_day in range(self.__date_period):
                # print day if transaction count in block fits condition--------

                print_still_alive = False

                if self.__date_period <= 25:
                    if txes_num <= 125000:
                        if (i_day % 36) == 0:
                            print_still_alive = True
                    elif txes_num <= 250000:
                        if (i_day % 24) == 0:
                            print_still_alive = True
                    elif txes_num <= 500000:
                        if (i_day % 18) == 0:
                            print_still_alive = True
                    elif txes_num <= 1000000:
                        if (i_day % 12) == 0:
                            print_still_alive = True
                    elif txes_num <= 2000000:
                        if (i_day % 6) == 0:
                            print_still_alive = True
                    elif txes_num <= 4000000:
                        if (i_day % 4) == 0:
                            print_still_alive = True
                    else:
                        if (i_day % 2) == 0:
                            print_still_alive = True

                elif self.__date_period <= 50:
                    if txes_num <= 125000:
                        if (i_day % 72) == 0:
                            print_still_alive = True
                    elif txes_num <= 250000:
                        if (i_day % 48) == 0:
                            print_still_alive = True
                    elif txes_num <= 500000:
                        if (i_day % 36) == 0:
                            print_still_alive = True
                    elif txes_num <= 1000000:
                        if (i_day % 24) == 0:
                            print_still_alive = True
                    elif txes_num <= 2000000:
                        if (i_day % 12) == 0:
                            print_still_alive = True
                    elif txes_num <= 4000000:
                        if (i_day % 8) == 0:
                            print_still_alive = True
                    else:
                        if (i_day % 4) == 0:
                            print_still_alive = True

                elif self.__date_period <= 100:
                    if txes_num <= 125000:
                        if (i_day % 128) == 0:
                            print_still_alive = True
                    elif txes_num <= 250000:
                        if (i_day % 96) == 0:
                            print_still_alive = True
                    elif txes_num <= 500000:
                        if (i_day % 72) == 0:
                            print_still_alive = True
                    elif txes_num <= 1000000:
                        if (i_day % 48) == 0:
                            print_still_alive = True
                    elif txes_num <= 2000000:
                        if (i_day % 24) == 0:
                            print_still_alive = True
                    elif txes_num <= 4000000:
                        if (i_day % 16) == 0:
                            print_still_alive = True
                    else:
                        if (i_day % 8) == 0:
                            print_still_alive = True

                elif self.__date_period <= 200:
                    if txes_num <= 125000:
                        if (i_day % 256) == 0:
                            print_still_alive = True
                    elif txes_num <= 250000:
                        if (i_day % 192) == 0:
                            print_still_alive = True
                    elif txes_num <= 500000:
                        if (i_day % 144) == 0:
                            print_still_alive = True
                    elif txes_num <= 1000000:
                        if (i_day % 96) == 0:
                            print_still_alive = True
                    elif txes_num <= 2000000:
                        if (i_day % 48) == 0:
                            print_still_alive = True
                    elif txes_num <= 4000000:
                        if (i_day % 32) == 0:
                            print_still_alive = True
                    else:
                        if (i_day % 16) == 0:
                            print_still_alive = True

                elif self.__date_period <= 1000:
                    if txes_num <= 3_500_000:
                        if (i_day % 160) == 0:
                            print_still_alive = True
                    else:
                        if (i_day % 80) == 0:
                            print_still_alive = True
                else:
                    if txes_num <= 4_000_000:
                        if (i_day % 240) == 0:
                            print_still_alive = True
                    else:
                        if (i_day % 120) == 0:
                            print_still_alive = True

                if i_day != 0 and print_still_alive == True:
                    colorChoice = cs.WHI
                    if self.__date_period >= 100:
                        colorChoice = cs.CYB

                    Velo.logger.info(
                        "{}{}  [day_{:04d}/{:04d}]  {}".format(
                            "{}[{}{}/{:03}{}]".format(
                                cs.RES,
                                colorChoice,
                                self.process_name,
                                Velo.process_cnt-1,
                                cs.RES,
                            ),
                            cs.WHI,
                            i_day,
                            self.__date_period,
                            "get_basic_tx_data()",
                        )
                    )

                # initialize daily used data structures-------------------------
                date     = self.__date_period_start + timedelta(i_day)
                date_str = date.strftime("%y/%m/%d")
                index_day.append(date_str)
                txes_daily.append([])

                # transform date variables--------------------------------------
                # day_date_net_prev = day_date_next
                day_date = day_date_next
                day_date_next += relativedelta(days=1)

                # get minimum and maximum block_height according to actual day--
                block_height_min = Velo.block_times[
                    Velo.block_times.index >= day_date
                ].iloc[0][0]

                block_height_max = Velo.block_times[
                    Velo.block_times.index >= day_date_next
                ].iloc[0][0]

                # retrieve daily txes and values per block in daily blockrange--
                for i_bh in range(block_height_min, block_height_max):
                    block = Velo.chain[i_bh]
                    retrieve_per_block(i_day, block)

            #--get list of aggregated coin supply per given block height--------
            txes_csupply_agg = list(
                Velo.csupply_agg_cache[i]
                for i in txes_block_heights
            )

            #--append results to queue dictionary-------------------------------
            self.__queue_dict["txes_number"]      = txes_number
            self.__queue_dict["txes_fees"]        = txes_fees
            self.__queue_dict["txes_dustfees"]    = txes_dustfees
            self.__queue_dict["txes_dustinpval"]  = txes_dustinpval
            self.__queue_dict["index_day"]        = index_day
            self.__queue_dict["txes_valout"]      = txes_valout
            self.__queue_dict["txes_csupply_agg"] = txes_csupply_agg
            self.__queue_dict["txes_block_time"]  = pd.to_datetime(
                txes_block_time
            )
            self.__queue_dict["index_txes"]       = index_txes

            #--used by following instance level functions-----------------------
            self.__txes_chouts = txes_chouts
            self.__txes_daily  = txes_daily

            #--test and normal returns------------------------------------------
            if Velo.test_level > 0:
                s_txes_chouts = str(self.__txes_chouts)
                self.__queue_dict["txes_chouts"] = s_txes_chouts

            if 2 <= Velo.test_level and Velo.test_level <=9:
                self.__queue.put([self.process_id, self.__queue_dict])
                return True

            return False

        def get_m_circ(type = ""):
            """
            "Whole bill"-approach:

            Get coin supply in circulation for a given period,
            based on Tx inputs created in earlier periods and new
            minted coins.

            Here: Get circulating supply for each day
            => extract the transactions for each day that satisfy certain
            characteristics. We need tx inputs that where processed and
            generated before the given period. Additionally we need newly
            minted coins that where transacted during the given period.

            "Moved coin"-approach:

            Get coin supply in circulation for a given period, based on Tx
            inputs created in earlier periods and new minted coins.
            Inputs converting to change outputs are substracted based on
            either "FIFO" or "LIFO". It is assumed, that inputs are first
            filling up outputs controlled by third party public keys,
            rather than change outputs.

            Here: Get circulating supply for each day
            """
            def m_circ(type):
                """
                Wrapper for handle_tx(wb_bill|mc_xifo), x \in {l, f}.
                """

                def inp_spend_before_bh_first_or_coinbase(
                    inp,
                    bh_first,
                ):
                    """
                    This function represents the condition for the handle_tx_mc
                    functions to decide, whether to sum an input or not.
                    """

                    # check if the tx that spent this input is
                    # older than bh_first
                    if inp.spent_tx.block_height < bh_first:
                        return True

                    # check if the tx that spent this input is a coinbase tx
                    if inp.spent_tx.is_coinbase:
                        return True

                    return False

                def handle_tx_mc_xifo(
                    tx,
                    bh_first,
                    sortswitch,
                ):
                    """
                    Moved-Coin-Approach
                    We will do:
                    1) For the TX: get the sum of Changeouts,
                       sum of all self-churning outputs
                    2) For the TX: the amount of Satoshi *actually sent*,
                       amount of money send to others/sum of self-churning
                       outputs
                    3) For the TX: the age-sorted list of inputs
                    4) For each input, that is smaller than the accumulated sum
                       of BTC sent to third party, extract it's value and to sum
                       it up later. If only a part of the respective input is
                       needed, it will be corrected. The complete input object
                       is stored as well for later use in filtering out recycled
                       TXOs.
                    *) Only those inputs are summed up that either where
                       generated before the respective first_block_height/period
                       or where spent by a coinbase transaction
                    **) Only as much as val_outs_sent_to_others will be summed
                        up.
                    """
                    m_circ_mc       = 0
                    val_inps_summed = 0
                    val_outs_break  = 0
                    inps            = tx.inputs
                    inps_age        = inps.age
                    outs            = tx.outputs
                    val_outs        = tx.output_value

                    if len(inps) == 0 or tx.input_value == 0 or tx.output_value == 0:
                        return 0

                    else:
                        # 1)
                        val_chouts = 0
                        for out in Velo.heur_select[Velo.heur_input].change(tx):
                            if False == in_max_cluster(out):
                                val_chouts += int(out.value)
                                # out_addr   = out.address
                                # out_cls    = Velo.cluster_mgr.cluster_with_address(out_addr)
                                # out_cls_id = out_cls.index
                                # Velo.logger.info("out_cls_id = {:9} added".format(out_cls_id))

                        val_outs_sent_to_others = ( val_outs - val_chouts )

                        # 2)
                        if val_outs_sent_to_others < 0:
                            raise ValueError(
                                "val_outs_sent_to_others must not be less than 0!"
                            )
                        elif val_outs_sent_to_others == 0:
                            return 0

                        # 3)
                        inps_sorted = sort_together(
                            [
                                inps_age,
                                inps
                            ],
                            reverse = sortswitch
                        )[1]
                        # 4/)
                        for inp_i in inps_sorted:
                            val_inp_i = inp_i.value
                            val_outs_break += val_inp_i

                            # *)
                            if inp_spend_before_bh_first_or_coinbase(
                                inp_i,
                                bh_first,
                            ) == True:
                                m_circ_mc += val_inp_i
                                # **)
                                if val_outs_break >= val_outs_sent_to_others:
                                    if m_circ_mc >= val_outs_sent_to_others:
                                        m_circ_mc = val_outs_sent_to_others
                                    break

                    if m_circ_mc < 0:
                        Velo.logger.error(
                            "{}{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}".format(
                                "{}[{}{}/{:03}{}]".format(
                                    cs.RES,
                                    cs.WHI,
                                    self.process_name,
                                    Velo.process_cnt-1,
                                    cs.RES,
                                ),
                                cs.WHI,
                                "        block_time              = {}".format(tx.block_time),
                                "        tx.hash                 = {}".format(tx.hash),
                                "        is coinbase?            = {}".format(tx.is_coinbase),
                                "        val_outs                = {}".format(val_outs),
                                "        val_chouts              = {}".format(val_chouts),
                                "        val_outs_sent_to_others = {}".format(val_outs_sent_to_others),
                                "        inps_sorted             = {}".format(inps_sorted),
                                "        val_inps_all            = {}".format(tx.input_value),
                                "        val_inps_summed         = {}".format(val_inps_summed),
                                "        m_circ_m                = {}".format(m_circ_mc),
                            )
                        )
                        raise ValueError(
                            "m_circ_m must not be less than 0!"
                            )

                    return m_circ_mc

                def handle_tx_wh_bill(
                    tx,
                    bh_first,
                ):
                    """
                    complete agg value of inputs
                    """
                    m_circ_mc = 0
                    inps      = tx.inputs

                    for inp_i in inps:
                        if inp_spend_before_bh_first_or_coinbase(
                            inp_i,
                            bh_first,
                        ) == True:
                            m_circ_mc += inp_i.value

                    return m_circ_mc

                Velo.logger.info(
                    "{}[{}{}/{:03}{}]{}  {} {}".format(
                        cs.RES,
                        cs.PRGnBC,
                        self.process_name,
                        Velo.process_cnt-1,
                        cs.RES,
                        cs.WHI,
                        "get daily coin supply type",
                        type,
                    )
                )

                # check type specific characteristics---------------------------
                if type == "mc_lifo":
                    sortswitch = False
                elif type == "mc_fifo":
                    sortswitch = True
                elif type == "wh_bill":
                    pass
                else:
                    raise ValueError('Wrong input in LIFO/FIFO choice.')

                # initialize data structures------------------------------------
                start_date_gen   = pd.to_datetime(Velo.start_date_gen)
                time_windows     = Velo.time_windows
                time_windows_len = len(time_windows)
                m_circ           = [[] for i in range(time_windows_len)]

                # retrieve data for each daychunk of txes-----------------------
                for daychunk in self.__txes_daily:
                    # if no transactions happened, append 0
                    if daychunk == []:
                        for i in range(time_windows_len):
                            m_circ[i].append(0)
                        continue

                    # initialize data structures per daychunk-------------------
                    m_circ_per_day  = [0 for i in range(time_windows_len)]

                    # initialize first block heights/day index of txes----------
                    first_block_height = []
                    # last_block_height  = daychunk[-1].block_height

                    first_block_height.append(daychunk[0].block_height)
                    day_index = Velo.index_block_day[first_block_height[0]]

                    for i in range(1, time_windows_len):
                        i_day = int(day_index - time_windows[i])

                        if i_day < 0:
                            i_day = 0

                        first_block_height.append(Velo.index_day_block[i_day])

                    # txes in daychunk------------------------------------------
                    # Loop over daychunks of tx data
                    # Note: Implicit assumption (made explicit): Inputs fill in
                    # "truly" transacted first, changeoutputs are filled last.
                    for tx in daychunk:
                        # Here, dust transaction shouldn't be included
                        # fee: output-input
                        # fee = output => input = 0
                        # fee > output => input < 0
                        if tx.output_value <= tx.fee:
                            continue

                        for i in range(time_windows_len):
                            if type == "mc_lifo" or type == "mc_fifo":
                                m_circ_per_day[i] += handle_tx_mc_xifo(
                                    tx,
                                    first_block_height[i],
                                    sortswitch,
                                )

                            elif type == "wh_bill":
                                m_circ_per_day[i] += handle_tx_wh_bill(
                                    tx,
                                    first_block_height[i],
                                )

                            else:
                                m_circ_per_day[i] += handle_tx_wh_bill(
                                    tx,
                                    first_block_height[i],
                                )

                    # append final values per day-------------------------------
                    for i in range(time_windows_len):
                        m_circ[i].append(m_circ_per_day[i])

                # put results into __queue_dict---------------------------------
                m_circ_results = list(zip(*m_circ))
                self.__queue_dict["m_circ_{}".format(type)] = m_circ_results

                # hande test_level cases----------------------------------------
                if 10 <= Velo.test_level and Velo.test_level <= 12:
                    self.__queue.put([self.process_id, self.__queue_dict])
                    return True

                return False

            if type == "":
                if m_circ("wh_bill") == True: return True

                #Call both lifo and fifo as they belong to the same concat test
                m_circ("mc_lifo")
                if m_circ("mc_fifo") == True: return True
                return False

            else:
                return m_circ(type)

            return False

        def get_issues_in_tx_chouts():
            """
            """
            Velo.logger.info(
                "{}[{}{}/{:03}{}]{}  get issues in changeouts".format(
                    cs.RES,
                    cs.PRGnBD,
                    self.process_name,
                    Velo.process_cnt-1,
                    cs.RES,
                    cs.WHI,
                )
            )

            self.__queue_dict["issues_in_tx_chouts"] = self.__txes_chouts

            del self.__txes_chouts

            if Velo.test_level == 14:
                self.__queue.put([self.process_id, self.__queue_dict])
                return True

            return False

        def get_comp_meas_summands():
            """
            We use this weighted average to calculate (1) the summands necessary
            to calculate SDD and (2) the (with the time window transaction
            volume) weighted summands for dormancy.
            Both is calculated first on a tx-wise level (A) and later
            summed up (B). Using the daily sums, finally, using a cum
            sum over a time window function, the two measures can be
            calculated (C).

            Overview: Weighted average used to calculate summands for ... on ...
            (1): ... SDD
            (2): ... (weighed summands) Dormancy
            (A): ... tx-wise/txly level
            (B): ... daychunk level, summed up
            (C): see get_comp_meas_from_summands() in get_df_m_circ()/get_df()

            Note: dsls - Time/Days Since Last Spent
            For non-weighted summands, no 2dim list is necessary, as there is
            no weighting with the tx value over the time window. It is helpful
            to look at the derivation of the equations in our paper.

            *) Here, dust transaction shouldn't be included

               If all inputs are zero, then the fee and the outputs are
               all zero. Thus, we ignore this transaction since only
               count "time since last spend", which does not occure here.
               Eventually, the weight of this transaction is zero, which
               means that we would not include it in our computation
               anyway
            """
            def get_dsls_per_tx(tx):
                """
                dsls_per_input := "days since the input has been spent last"
                """
                tx_time     = tx.block_time
                val_inp_tx  = tx.input_value
                dsls_per_tx = 0

                for input_i in tx.inputs:
                    # gather time variables to calculate the "dsls per input"
                    tx_time_of_input_origin = input_i.spent_tx.block_time

                    # calculate dsls
                    dsls_per_input        = tx_time - tx_time_of_input_origin
                    dsls_per_input_in_sec = dsls_per_input.total_seconds()

                    # form a list of the dsls information over all inputs
                    if not val_inp_tx == 0:
                        dsls_per_tx += (
                            dsls_per_input_in_sec * input_i.value/val_inp_tx
                        )

                return dsls_per_tx

            Velo.logger.info(
                "{}[{}{}/{:03}{}]{}  get competing measures".format(
                    cs.RES,
                    cs.PRGnBD,
                    self.process_name,
                    Velo.process_cnt-1,
                    cs.RES,
                    cs.WHI,
                )
            )

            time_windows              = Velo.time_windows
            time_windows_len          = len(time_windows)
            summands_dsls_daily       = []
            summands_dsls_daily_wghtd = [[] for i in range(time_windows_len)]
            sec_per_day               = 86400 # 24*60*60

            for daychunk in self.__txes_daily:
                # if no transactions happened, append 0
                if daychunk == []:
                    # (B1):
                    summands_dsls_daily.append(0.0)

                    # (B2):
                    for i in range(time_windows_len):
                        summands_dsls_daily_wghtd[i].append(0.0)

                    continue

                # initialize data structures per daychunk-----------------------
                summands_dsls_per_day       = 0.0
                summands_dsls_per_day_wghtd = [
                    0.0 for i in range(time_windows_len)
                ]

                # initialize first block heights/day index of txes--------------
                first_block_height = []
                last_block_height  = daychunk[-1].block_height

                first_block_height.append(daychunk[0].block_height)
                day_index = Velo.index_block_day[first_block_height[0]]

                for i in range(1, time_windows_len):
                    i_day = int(day_index - time_windows[i])

                    if i_day < 0:
                        i_day = 0

                    first_block_height.append(Velo.index_day_block[i_day])

                # txes in daychunk----------------------------------------------
                for tx in daychunk:
                    # *)
                    if tx.output_value <= tx.fee:
                        continue

                    val_inp_tx = tx.input_value

                    txes_valout_agg_per_day_tw = Velo.txes_valout_agg_daily[
                        day_index
                    ]

                    # use the dsls info per tx to calc the weighted average
                    dsls_per_tx           = get_dsls_per_tx(tx)
                    dsls_per_tx_wghtd_avg = dsls_per_tx/sec_per_day

                    # (A1): (b_i \cdot delta t_i in dormancy paper)
                    summand_dsls_per_day   = dsls_per_tx_wghtd_avg * val_inp_tx
                    summands_dsls_per_day += summand_dsls_per_day

                    # (A2):
                    for t_w in range(time_windows_len):
                        summand_dsls_per_tx_wghtd = 0
                        if not txes_valout_agg_per_day_tw[t_w] == 0:
                            summand_dsls_per_tx_wghtd = (
                                summand_dsls_per_day
                                /txes_valout_agg_per_day_tw[t_w]
                            )
                        summands_dsls_per_day_wghtd[t_w] += (
                            summand_dsls_per_tx_wghtd
                        )

                # (B1):
                summands_dsls_daily.append(summands_dsls_per_day)

                # (B2):
                for i in range(time_windows_len):
                    summands_dsls_daily_wghtd[i].append(
                        summands_dsls_per_day_wghtd[i]
                    )

            # output needs to be in tuples for correct rebuild after threading
            # put results into __queue_dict-------------------------------------
            self.__queue_dict["summands_dsls_daily"]       = summands_dsls_daily
            self.__queue_dict["summands_dsls_daily_wghtd"] = list(
                zip(*summands_dsls_daily_wghtd)
            )

            return False

        # 1.1: [ Main Script - Standard info, M & TP ]--------------------------
        # Setup instance-wise part of chain
        if get_basic_tx_data() == True: return

        if get_m_circ() == True: return

        if get_comp_meas_summands() == True: return

        # 1.2: [ Main Script - TP adjustments ]---------------------------------
        if get_issues_in_tx_chouts() == True: return

        #put all necessary data to parent process through multiprocess queue
        Velo.logger.debug("{}[{}{}/{:03}{}]{}  Sending results".format(
            cs.RES,
            cs.PRGnBE,
            self.process_name,
            Velo.process_cnt-1,
            cs.RES,
            cs.WHI,
        ))
        self.__queue.put([self.process_id, self.__queue_dict])
        Velo.logger.debug("{}[{}{}/{:03}{}]{}  terminating".format(
            cs.RES,
            cs.PRGnBE,
            self.process_name,
            Velo.process_cnt-1,
            cs.RES,
            cs.WHI,
        ))

        del Velo.chain

        return

if __name__ == "__main__":
    print("{}Use this file with script.py!{}".format(cs.RED,cs.RES))
    exit(0)
