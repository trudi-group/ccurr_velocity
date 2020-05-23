from logging             import INFO
from pandas              import DataFrame, to_datetime
from blocksci            import Blockchain
from blocksci.cluster    import ClusterManager
from blocksci.heuristics import change
from multiprocessing     import Pool, Lock, Value, cpu_count
from colorstrings        import ColorStrings as cs
from datetime            import date, datetime, timedelta
from math                import floor, ceil
from numpy               import cumsum
from iter<tools           import chain as it_chain
from more_itertools      import sort_together

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
    This class provides functionality to:
    - retrieve basic blockchain data, e.g., transaction volume
    - calculate transaction volume selfchurn via clustering of addresses
    - sophisticated measurements for money supply in circulation
    - eventually, measurements for the velocity of money in utxo-based crypto-
      currencies
    """

    #--class attribute representing blocksci chain object used by all instances-
    chain             = None              # the mainly blockcain object---------
    cluster_mgr       = None              # manager for clustering addresses----
    cluster_cnt       = None              # number of address clusters----------
    cluster_range     = None              # ------------------------------------
    cluster_max_id    = None              # id of cluster with max address count
    cluster_overwrite = False             # toggle overwriting of cluster cache-
    cluster_skip      = True              # skip recounting of cluster addresses
    block_times       = None              # ------------------------------------
    heur_select       = None              # selected change address heuristics--
    start             = None              # ------------------------------------
    end               = None              # ------------------------------------

    #--chosen columnnames for final dataframe-----------------------------------
    results_raw_types_basic        = None
    results_raw_types_m_circ       = None
    results_raw_types_m_circ_tw    = []
    results_raw_types_comp_meas    = None
    results_raw_types_comp_meas_tw = []

    #--lookup functions/mappings-(as lists)-------------------------------------
    f_index_day_of_block_height = []                 # f_index-day(block height)
    f_block_height_of_id_day    = []                 # f_block-height(day-id)---
    f_tx_vol_agg_of_id_day      = []                 # f_tx-vol-agg(day-id)-----
    f_dates_of_id_sub_proc      = []                 # f_dates(subprocess-id)---
    f_m_total_of_block_height   = None               # f_m-total(block height)--

    #--remaining class attributes-----------------------------------------------
    args              = None                   # commandline arguments----------
    date_format       = "%Y-%m-%d %H:%M:%S"    # date formatting information----
    start_date_gen    = "01/03/2009"           # date of bitcoin genesis--------
    path_data_output  = None                   # path for data output-----------
    log_level         = INFO                   # default logging level----------
    logger            = None                   # logging object-----------------
    test_level        = 0                      # default basic test level-------
    process_cnt       = 0                      # count of sub procs for printing
    cnt_days          = 0                      # count of days in range to be---
                                               # analyzed-----------------------
    block_height_max  = 0                      # maximum block height regarding-
                                               # given end date for analysis----
    tx_vol_agg        = []                     # daily aggr. tx volume----------

    #==[ CLASSLEVEL | SessionSetup & precaching of global data struct ]=========
    def setup(
        logger,
        args,
     ):
        """
        Initialize session and with that, the main blockchain object used by
        each instance of the Velo class.
        """
        def setup_chain_and_attributes(args):
            """
            This function handles the necessary commandline arguments for Velo
            and sets up static variables on class level.
            """
            #--setup of static variables on class level-------------------------
            Velo.args             = args
            Velo.test_level       = int(args.test_level)
            Velo.time_windows     = list(
                map(int, str(args.time_window).split(","))
            )
            Velo.cnt_cls_only     = args.count_clustering_only
            Velo.path_data_output = args.path_data_output
            Velo.chain            = Blockchain(args.path_data_input)

            Velo.start_date       = args.start_date
            if Velo.test_level > 0:
                Velo.end_date = "01/01/2012"
            else:
                Velo.end_date = args.end_date

            return

        def setup_logging(logger):
            """
            Setup logging and print basic info.
            """
            Velo.logger    = logger
            Velo.log_level = args.log_level

            #--print basic data in debug mode-----------------------------------
            Velo.logger.debug(
                "date(first block) = {}".format(Velo.chain[0].time)
            )
            Velo.logger.debug(
                "date(last block)  = {}".format(Velo.chain[-1].time)
            )

            return

        def setup_final_data_columns_choice():
            """
            This functions sets up the data choice for the final results.
            """
            # print status message----------------------------------------------
            Velo.logger.info(
                "{}[{}     SETUP     {}]{}  "
                "choice of desired data".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,
                )
            )

            # basic data from blockchain----------------------------------------
            Velo.results_raw_types_basic = [
                "index_day",
                "tx_count",
                "tx_fees",
                "tx_vol",
                "tx_vol_self_churn",
                "m_total",
            ]

            # money supply in effective circulation-----------------------------
            Velo.results_raw_types_m_circ = [
                "m_circ_wh_bill",
                "m_circ_mc_lifo",
                "m_circ_mc_fifo",
            ]

            for type in Velo.results_raw_types_m_circ:
                for t_w in Velo.time_windows:
                    Velo.results_raw_types_m_circ_tw.append(
                        "{}_{}".format(
                            type,
                            t_w,
                        )
                    )

            # compeating measurements for comparision---------------------------
            Velo.results_raw_types_comp_meas_tw.append("sdd")

            Velo.results_raw_types_comp_meas = ["dormancy"]

            for type in Velo.results_raw_types_comp_meas:
                for t_w in Velo.time_windows:
                    Velo.results_raw_types_comp_meas_tw.append(
                        "{}_{}".format(
                            type,
                            t_w,
                        )
                    )

            return

        def setup_heuristics():
            """
            compare
            https://citp.github.io/BlockSci/reference/heuristics/change.html
            Returns heuristics dictionary of selected heuristics
            """
            # print status message----------------------------------------------
            Velo.logger.info(
                "{}[{}     SETUP     {}]{}  "
                "change address heuristics".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,
                )
            )

            # setup heuristic lookup dictionary ids with heuristic names--------
            heur_names = [
                "address_reuse",
                "address_type",
                "client_change_address_behavior",
                "legacy",
                "legacy_improved",
                "peeling_chain",
                "locktime",
                "optimal_change",
                "power_of_ten_value"
            ]

            # setup heuristic lookup dictionary items---------------------------
            heur_items = []
            heur_items.append(change.address_reuse())
            heur_items.append(change.address_type())
            heur_items.append(change.client_change_address_behavior())
            heur_items.append(change.legacy())
            heur_items.append(change.legacy().__or__(change.peeling_chain()))
            heur_items.append(change.peeling_chain())
            heur_items.append(change.locktime())
            heur_items.append(change.optimal_change())
            heur_items.append(change.power_of_ten_value())

            # setup actual heuristic lookup dictionary--------------------------
            heur = dict(zip(heur_names, heur_items))
            Velo.heur_select = heur[Velo.args.heur_choice]

            return

        def setup_m_total_of_block_height():
            """
            Precompute aggregated total money supply
            aka. cumulated coin supply for whole chain.
            """
            def coin_supply_renumeration(block_height):
                """
                supply calculation of BTC inspired by:
                [https://www.coindesk.com/making-sense-bitcoins-halving/]
                """

                # the mining reward will be halved each 210000 blocks-----------
                halving_interval = 210000

                #initial reward
                reward = 50*100000000

                if block_height < halving_interval:
                    return(reward)

                halvings = floor(block_height / halving_interval)

                if halvings >= 64:
                    # (right shifting on 64 bit integer is be undefined then)
                    return(0)

                #using right shifts to devide by 2------------------------------
                reward >>= halvings

                return(reward)

            # print status message----------------------------------------------
            Velo.logger.info(
                "{}[{}     SETUP     {}]{}  "
                "calculating cumulative coin supply".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,
                )
            )

            # get basic values--------------------------------------------------
            last_block = Velo.chain[-1]
            block_height_range_max = last_block.height

            # compute coin supply per block height------------------------------
            coin_supply_per_block_height = []
            block_height_range_max += 1

            for block_height in range(0, block_height_range_max):
                coin_supply_per_block_height.append(
                    coin_supply_renumeration(block_height)
                )

            # compute cumulated/aggregated coin supply per block height---------
            Velo.f_m_total_of_block_height = cumsum(
                coin_supply_per_block_height
            )

            return

        def setup_clustering_count():
            """
            Count addresses per cluster and retrieve id and size of biggest
            cluster.
            """
            #--print status message---------------------------------------------
            Velo.logger.info(
                "{}[{}     SETUP     {}]{}  "
                "clustering: get id of maximum cluster".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,
                )
            )

            #-------------------------------------------------------------------
            path_cluster          = Velo.args.path_cluster
            Velo.cluster_max_size = 0
            Velo.cluster_max_id   = 0

            #--print status message---------------------------------------------
            Velo.logger.info(
                "{}[{}     SETUP     {}]{}  "
                "clustering: get id of maximum cluster".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,
                )
            )

            #-------------------------------------------------------------------
            path_cluster          = Velo.args.path_cluster
            Velo.cluster_max_size = 0
            Velo.cluster_max_id   = 0

            # load blocksci clustering manager----------------------------------
            Velo.cluster_mgr = ClusterManager(
                path_cluster,
                Velo.chain,
            )

            # return assumingly largest cluster (id = 32), when skip is on------
            if True == Velo.cluster_skip:
                Velo.cluster_max_id = 32
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

                if Velo.cnt_cls_only != 0:
                    exit(0)

                return

            # renew clustering cache, if desired--------------------------------
            if Velo.cluster_overwrite == True:
                Velo.cluster_mgr = ClusterManager.create_clustering(
                     path_cluster,
                     Velo.chain,
                     Velo.heur_select,
                     Velo.cluster_overwrite,
                )

            #--get largest cluster via subproccesing----------------------------
            Velo.cluster_range = Velo.cluster_mgr.clusters()
            Velo.cluster_cnt   = len(Velo.cluster_range)
            sub_proc_cls_range = ceil(Velo.cluster_cnt/cpu_count())

            Velo.logger.info(
                "{}[{}  clustering   {}]"
                "{}  Number of clusters per subprocess/in total: {}/{}".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,
                    sub_proc_cls_range,
                    Velo.cluster_cnt
                )
            )

            # setup cluster address counting subprocesses-----------------------
            cls_args = []
            begin    = 0
            end      = 0
            for cpu_i in range (0, cpu_count()):
                begin = sub_proc_cls_range * cpu_i
                end   = sub_proc_cls_range * (cpu_i+1) - 1

                if end > Velo.cluster_cnt:
                    end = Velo.cluster_cnt -1

                cls_arg = (cpu_i, begin, end)
                cls_args.append(cls_arg)

            # start subproccesing-----------------------------------------------
            p = Pool(cpu_count())
            p.map(cls_worker, cls_args)

            # retrieve results--------------------------------------------------
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

            # check whether we only want to count cluster addresses-------------
            if Velo.cnt_cls_only != 0:
                exit(0)

            return

        def setup_subprocessing_chunks():
            """
            Setup transactions chunks for multiprocessing.
            """
            def setup_subprocessing_chunks_per_day(
                day,
                sub_proc_tx_cnt_max,
                sub_proc_txes_counter,
                sub_proc_date_start,
                sub_proc_date_end,
                sub_proc_date_period,
                cnt_txes_per_day,
                sub_proc_printed,
            ):
                """
                Setup transactions chunks for multiprocessing per day.
                """
                # Assumption: There is no day with cnt_txes > sub_proc_tx_cnt_max

                # txes numbers of all other days
                txes_counter_next = sub_proc_txes_counter + cnt_txes_per_day

                #if txes_counter_next < sub_proc_tx_cnt_max or sub_proc_printed == cpu_count()-1:
                sub_proc_date_end    += timedelta(days = 1)
                sub_proc_date_period += 1
                sub_proc_txes_counter = txes_counter_next

                if txes_counter_next < sub_proc_tx_cnt_max:
                    #sub_proc_date_end    += timedelta(days = 1)
                    #sub_proc_date_period += 1
                    #sub_proc_txes_counter = txes_counter_next
                    pass

                else:
                    sub_proc_date = [
                        sub_proc_date_start,
                        sub_proc_date_end,
                        sub_proc_date_period,
                        sub_proc_txes_counter
                    ]
                    Velo.f_dates_of_id_sub_proc.append(sub_proc_date)
                    Velo.logger.debug(
                            "{:2}[{:4}]: ({})--({}), {:5}, {:10}".format(
                            sub_proc_printed,
                            day,
                            sub_proc_date_start,
                            sub_proc_date_end,
                            sub_proc_date_period,
                            sub_proc_txes_counter,
                        )
                    )
                    sub_proc_printed     += 1
                    sub_proc_date_start   = sub_proc_date_end
                    sub_proc_date_end     = sub_proc_date_start
                    sub_proc_date_period  = 0
                    sub_proc_txes_counter = 0
                    #sub_proc_txes_counter = cnt_txes_per_day

                # treat last day seperately-------------------------------------
                if day == (Velo.cnt_days-1):
                    sub_proc_date = [
                        sub_proc_date_start,
                        sub_proc_date_end,
                        sub_proc_date_period,
                        sub_proc_txes_counter
                    ]
                    Velo.f_dates_of_id_sub_proc.append(sub_proc_date)
                    Velo.logger.debug(
                        "{:2}[{:4}]: ({})--({}), {:5}, {:10} (last)".format(
                            sub_proc_printed,
                            day,
                            sub_proc_date_start,
                            sub_proc_date_end,
                            sub_proc_date_period,
                            sub_proc_txes_counter,
                        )
                    )


                return (
                    sub_proc_printed,
                    sub_proc_txes_counter,
                    sub_proc_date_start,
                    sub_proc_date_end,
                    sub_proc_date_period
                )

            #--print status message---------------------------------------------
            Velo.logger.info(
                "{}[{}     SETUP     {}]{}  "
                "subprocessing chunks of blockchain".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,
                )
            )

            #--get block times and day counts-----------------------------------
            Velo.block_times = DataFrame(
                [block.time for block in Velo.chain],
                columns = ["date"],
            )
            Velo.block_times["height"] = Velo.block_times.index
            Velo.block_times.index     = Velo.block_times["date"]
            del Velo.block_times["date"]

            Velo.cnt_days = (
                to_datetime(Velo.end_date) - to_datetime(Velo.start_date_gen)
            ).days
            Velo.logger.debug("cnt_days = {}".format(Velo.cnt_days))

            #--get minimum and maximum block_height according to start/end_date-
            block_height_min = Velo.block_times[
                Velo.block_times.index >= to_datetime(Velo.start_date_gen)
            ].iloc[0][0]

            block_height_max = Velo.block_times[
                Velo.block_times.index >= to_datetime(Velo.end_date)
            ].iloc[0][0]

            Velo.block_height_max = block_height_max

            #--get transcation counts between start/end_date blocks-------------
            cnt_txes = 0
            for i_bh in range(block_height_min, block_height_max):
                cnt_txes += Velo.chain[i_bh].tx_count

            Velo.logger.debug("cnt_txes = {}".format(cnt_txes))

            #-initialie data for subprocessing----------------------------------
            day_date              = to_datetime(Velo.start_date_gen)
            day_date_next         = day_date
            sub_proc_tx_cnt_max   = ceil(cnt_txes/cpu_count())
            sub_proc_txes_counter = 0
            sub_proc_date_start   = day_date
            sub_proc_date_end     = day_date + timedelta(days=1)
            sub_proc_date_period  = 1
            sub_proc_printed      = 0

            for day in range(Velo.cnt_days):
                # update for-loop date variables--------------------------------
                day_date       = day_date_next
                day_date_next += timedelta(days=1)

                # initialize for-scope variables--------------------------------
                cnt_txes_per_day = 0

                # get minimum and maximum block height according to actual day--
                block_height_min = Velo.block_times[
                    Velo.block_times.index >= day_date
                ].iloc[0][0]

                block_height_max = Velo.block_times[
                    Velo.block_times.index >= day_date_next
                ].iloc[0][0]

                # retrieve values per block in daily blockrange-----------------
                for i_bh in range(block_height_min, block_height_max):
                    cnt_txes_per_day += Velo.chain[i_bh].tx_count

                    Velo.f_index_day_of_block_height.append(day)

                Velo.f_block_height_of_id_day.append(block_height_min)

                # calculate data for sub processing periods---------------------
                if day == 0:
                    # txes number of first day, don't change dates
                    sub_proc_txes_counter = cnt_txes_per_day

                else:
                    ret = setup_subprocessing_chunks_per_day(
                        day,
                        sub_proc_tx_cnt_max,
                        sub_proc_txes_counter,
                        sub_proc_date_start,
                        sub_proc_date_end,
                        sub_proc_date_period,
                        cnt_txes_per_day,
                        sub_proc_printed,
                    )

                    sub_proc_printed      = ret[0]
                    sub_proc_txes_counter = ret[1]
                    sub_proc_date_start   = ret[2]
                    sub_proc_date_end     = ret[3]
                    sub_proc_date_period  = ret[4]

            return

        def setup_tx_vol_agg_time_windowed():
            """
            Compute aggregates for given times in Velo.time_windows.
            """
            def tx_vol_agg_time_windowed_per_day(tx_vol_agg_nxt_day):
                """
                Compute daily aggregates for given times in Velo.time_windows.
                """
                for t_w in range(1, len(Velo.time_windows)):
                    tx_vol_agg_last = 0

                    if day > 0:
                        tx_vol_agg_last = Velo.f_tx_vol_agg_of_id_day[day-1][t_w]

                    #-add the current daily calculations---------------------------
                    tx_vol_agg_t_w = tx_vol_agg_last + tx_vol_agg_nxt_day

                    #-substract the calculations right before the current window---
                    if day >= Velo.time_windows[t_w]:
                        tx_vol_agg_t_w -=  Velo.f_tx_vol_agg_of_id_day[
                            day - Velo.time_windows[t_w]
                        ][0]

                    Velo.f_tx_vol_agg_of_id_day[day].append(tx_vol_agg_t_w)

                return

            #--print status message---------------------------------------------
            Velo.logger.info(
                "{}[{}     SETUP     {}]{}  "
                "tx_vol_agg_time_windowed".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,
                )
            )
            day_date      = to_datetime(Velo.start_date_gen)
            day_date_next = day_date

            for day in range(Velo.cnt_days):
                # update for-loop date variables--------------------------------
                day_date       = day_date_next
                day_date_next += timedelta(days=1)

                # initialize for-scope variables: daily agg. tx_vol for---------
                # given time_windows--------------------------------------------
                tx_vol_agg_nxt_day = 0
                Velo.f_tx_vol_agg_of_id_day.append([])

                # get minimum and maximum block height according to actual day--
                block_height_min = Velo.block_times[
                    Velo.block_times.index >= day_date
                ].iloc[0][0]

                block_height_max = Velo.block_times[
                    Velo.block_times.index >= day_date_next
                ].iloc[0][0]

                # retrieve values per block in daily blockrange-----------------
                for i_bh in range(block_height_min, block_height_max):
                    tx_vol_agg_nxt_day += Velo.chain[i_bh].output_value

                Velo.f_tx_vol_agg_of_id_day[day].append(tx_vol_agg_nxt_day)

                # aggregate txes volume for given time windows------------------
                tx_vol_agg_time_windowed_per_day(tx_vol_agg_nxt_day)

            Velo.logger.info(
                "{}[{}     SETUP END {}]{}  "
                "tx_vol_agg_time_windowed".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,
                )
            )
            return

        #--setup of static variables on class level-----------------------------
        setup_chain_and_attributes(args)

        #--setup logging--------------------------------------------------------
        setup_logging(logger)

        #--setup names of resulting data----------------------------------------
        setup_final_data_columns_choice()

        #--setup heurictics object----------------------------------------------
        setup_heuristics()

        #--setup clustering object----------------------------------------------
        setup_clustering_count()

        #--setup total amount of money supply-----------------------------------
        setup_m_total_of_block_height()

        #--setup data for subprocessing-----------------------------------------
        setup_subprocessing_chunks()

        #--setup aggregated transaction volume regarding given time_windows-----
        #  setup_tx_vol_agg_time_windowed()

        return

    #==[ CLASSLEVEL | finalize results and get final data frames and csv ]======
    def get_results_finalized(
        results_raw,
        index_label = "",
    ):
        """
        Builds a pandas data frame and csv from pre-computed data.
        """

        def tx_vol_agg_time_windowed(results_raw):
            """
            Compute aggregates for given times in Velo.time_windows.
            """
            def tx_vol_agg_time_windowed_per_day(tx_vol_agg_nxt_day):
                """
                Compute daily aggregates for given times in Velo.time_windows.
                """
                for t_w in range(1, len(Velo.time_windows)):
                    tx_vol_agg_last = 0

                    if day > 0:
                        tx_vol_agg_last = Velo.tx_vol_agg[day-1][t_w]

                    #-add the current daily calculations---------------------------
                    tx_vol_agg_t_w = tx_vol_agg_last + tx_vol_agg_nxt_day

                    #-substract the calculations right before the current window---
                    if day >= Velo.time_windows[t_w]:
                        tx_vol_agg_t_w -=  Velo.tx_vol_agg[
                            day - Velo.time_windows[t_w]
                        ][0]

                    Velo.tx_vol_agg[day].append(tx_vol_agg_t_w)

                return

            #--print status message---------------------------------------------
            Velo.logger.info(
                "{}[{}   Calculate   {}]{}  "
                "tx_vol_agg_time_windowed".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,
                )
            )
            #-------------------------------------------------------------------
            tx_vol = results_raw["tx_vol"]

            for day in range(Velo.cnt_days):
                tx_vol_day = tx_vol[day]

                Velo.tx_vol_agg.append([])
                Velo.tx_vol_agg[-1].append(tx_vol_day)

                # aggregate txes volume for given time windows------------------
                tx_vol_agg_time_windowed_per_day(tx_vol_day)

            Velo.logger.info(
                "{}[{}     SETUP END {}]{}  "
                "tx_vol_agg_time_windowed".format(
                    cs.RES,
                    cs.PRGnBA,
                    cs.RES,
                    cs.PRGnBA,
                )
            )
            return

        def get_comp_meas_finalized(
            results_raw,
            min_frac = 1,
        ):
            """
            Function using the results form get_comp_meas to
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

                #--reverse the list, as we need to start with the latest date---
                l.reverse()

                #--set minimum periods necessary for cumsum---------------------
                min_periods = int(window*min_frac)

                #--convert list to pandas df------------------------------------
                df = DataFrame(l)

                #--calculate cumsum with lookback window------------------------
                df = df.rolling(
                    window = window,
                    min_periods = min_periods,
                ).sum().shift(-(window-1))

                #--convert pandas df back to list-------------------------------
                l = list(it_chain(*df.values.tolist()))

                #--reverse the list to be starting with the earliest date-------
                l.reverse()

                return l

            time_windows     = Velo.time_windows
            time_windows_len = len(time_windows)

            # finalize dormancy per time window---------------------------------
            for day_i in range(len(Velo.tx_vol_agg)):
                # initialize transaction volume per time window up to this day--
                tx_vol_per_day = Velo.tx_vol_agg[day_i]

                for t_w in range(time_windows_len):
                    dormancy_tw_str = "dormancy_raw_{}".format(
                        time_windows[t_w]
                    )

                    if tx_vol_per_day[t_w] == 0:
                        results_raw[dormancy_tw_str][day_i] = 0
                        continue

                    results_raw[dormancy_tw_str][day_i] /= tx_vol_per_day[t_w]

            #--C1---------------------------------------------------------------
            results_raw["sdd"] = cumsum_with_window_reverse(
                l = list(results_raw["sdd_raw"]),
                window = 1,
            )

            #-- C2--------------------------------------------------------------
            for i in range(time_windows_len):
                dormancy_tw = "dormancy_{}".format(time_windows[i])
                results_raw[dormancy_tw] = cumsum_with_window_reverse(
                    l = list(results_raw["dormancy_raw_{}".format(
                        time_windows[i]
                    )]),
                    window = time_windows[i],
                )

            return

        #--Start of get_results_finalized()-------------------------------------
        Velo.logger.info("{}{}[{}build&write csv{}{}]".format(
            cs.RES,
            cs.WHI,
            cs.PRGnBI,
            cs.RES,
            cs.WHI,
        ))

        #--prepare windows m_total for dormancy calculation---------------------
        tx_vol_agg_time_windowed(results_raw)

        #--prepare measures to be compared with velocity------------------------
        get_comp_meas_finalized(
            results_raw=results_raw,
            min_frac = 1,
        )

        #--create first part of final pandas data frame-------------------------
        results_raw_basic = {
            k: results_raw[k]
            for k in Velo.results_raw_types_basic
        }
        df_final = DataFrame.from_dict(results_raw_basic)
        df_final = df_final.set_index("index_day")

        #--handle m_circ df_types and merge to final data frame-----------------
        for m_circ_type in Velo.results_raw_types_m_circ_tw:
            df_final[m_circ_type] = results_raw[m_circ_type]

        #--handle measurements from literature and merge to finale data frame---
        for comp_meas_type in Velo.results_raw_types_comp_meas_tw:
            df_final[comp_meas_type] = results_raw[comp_meas_type]

        #--print status message-------------------------------------------------
        Velo.logger.info("{}{}[{}built dataframe{}{}]  {}".format(
            cs.RES,
            cs.WHI,
            cs.PRGnBI,
            cs.RES,
            cs.WHI,
            "final dataframe"
        ))

        #--remove row from January 4th 2009 to January 8th 2009-----------------
        df_final = df_final.drop([
            '09/01/04',
            '09/01/05',
            '09/01/06',
            '09/01/07',
            '09/01/08',
        ])

        #--build final csv------------------------------------------------------
        now_date       = datetime.now()
        end_date_d     = datetime.strptime(Velo.end_date, "%m/%d/%Y").date()
        now_date_str   = now_date.strftime("%Y%m%d_%H%M")
        end_date_str   = end_date_d.strftime("%Y%m%d")
        path           = "{}_csv/".format(Velo.path_data_output)
        filename_dates = "{}{}_e_{}".format(path, now_date_str, end_date_str)
        filename       = "{}_{}.csv".format(filename_dates, "velo_daily")

        df_final.to_csv(
            filename,
            sep=",",
            header=True,
            date_format=Velo.date_format,
            index_label=index_label,
        )

        #--print status message-------------------------------------------------
        Velo.logger.info(
            "{}{}[{}   wrote csv   {}{}]".format(
                cs.RES,
                cs.WHI,
                cs.PRGnBI,
                cs.RES,
                cs.WHI,
            )
        )

        return

    #--PUBLIC INSTANCE-LEVEL METHODS--##########################################
    #==[ INSTALEVEL | Initialize instances ]====================================
    def __init__ (
        self,
        process_id,
        process_name,
        queue,
        queue_evnt,
        date_id,
    ):
        """
        Initialize subprocess.
        """
        self.stage_id     = 0
        self.process_id   = process_id
        self.process_name = process_name
        self.__queue      = queue
        self.__queue_evnt = queue_evnt

        # next day to include date_period_end. Otherwise, it won't be regarded
        # due to the blocksci chainrange being computed as the daily difference.
        s_p_d = Velo.f_dates_of_id_sub_proc

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

        # instance-wise interfunctional temporary helper stuctures--------------
        self.__txes_daily = None
        self.__txes_count = s_p_d[date_id][3]

        # data structures conveyed by subprocess queues-------------------------
        self.__queue_dict      = {}
        self.__queue_evnt_dict = {}

    #==[ INSTALEVEL | Retrieve of desired data ]================================
    def run(self):
        """
        Run the thread.
        """
        def print_liveliness_message(
            i_day,
            function_str,
        ):
            """
            This function checks whether a liveliness message regaring
            the number of txes and the respective day id should be
            printed.
            Note that this function is not very dynamic and totally
            uggly!
            """
            print_still_alive = False
            txes_num = self.__txes_count
            date_period = self.__date_period

            if date_period <= 25:
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

            elif date_period <= 50:
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

            elif date_period <= 100:
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

            elif date_period <= 200:
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

            elif date_period <= 1000:
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
                if date_period >= 100:
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
                        date_period,
                        function_str,

                    )
                )

            return

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
            This function retrieves per subprocessing chunk:
            - txes_daily:          list of daily grouped txes
            - index_day:           index list of day ids
            - txes_count:          list of counted transactions per day
            - txes_fees:           list of aggregated transaction fees per day
            - txes_dust_fees:      list of aggregated transaction dust fees
                                   per day
            - txes_dust_inpval:    list of aggregated transaction dust
                                   input values per day
            - txes_vol:            transaction volume per day
                                   (output values of transactions per day)
            - txes_vol_self_churn: transaction volume selfchurn per day
                                   (check our paper or blocksci paper)
            - m_total:             aggregated money supply per day
            """

            def retrieve_per_tx_daily(
                i_day,
                tx,
            ):
                """
                This function retrieves basic blockchain values and aggregates
                them into daily chunks.
                """
                try:
                    if tx.block_height >= Velo.block_height_max:
                        return

                    txes_daily[i_day].append(tx)
                    txes_count[i_day] += 1
                    txes_fees[i_day]  += tx.fee

                    if tx.output_value <= tx.fee:
                        txes_dust_fees[i_day]   += tx.fee
                        txes_dust_inpval[i_day] += tx.input_value

                    txes_vol[i_day] += tx.output_value

                    val_chouts = 0
                    for out in Velo.heur_select.change(tx):
                        if False == in_max_cluster(out):
                            val_chouts += int(out.value)
                    txes_vol_self_churn[i_day] += val_chouts

                except IndexError as error:
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
                            "        block_height      = {}".format(
                                tx.block_height
                            ),
                            "        block_height_max  = {}".format(
                                Velo.block_height_max
                            ),
                            "        i_day             = {}".format(i_day),
                            "        day_diff_to_start = {}".format(
                                day_diff_to_start
                            ),
                            "        day_date          = {}".format(day_date),
                            "        date_period_start = {}".format(
                                self.__date_period_start
                            ),
                            "        block_time        = {}".format(
                                tx.block_time
                            ),
                            "        tx.hash           = {}".format(tx.hash),
                            "        is coinbase?      = {}".format(
                                tx.is_coinbase
                            ),
                            error,
                        )
                    )
                    exit(-1)

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
                    "txes_count = {:07d}".format(self.__txes_count),
                )
            )

            #--initialize data structures---------------------------------------
            txes_daily          = []         # all transactions of one day------
            index_day           = []         # index list of day ids------------
            txes_count          = []         # daily count of transactions------
            txes_fees           = []         # daily agg. tx fees---------------
            txes_dust_fees      = []         # daily agg. tx dust fees----------
            txes_dust_inpval    = []         # daily agg. tx dust input values--
            txes_vol            = []         # daily transaction volume---------
            txes_vol_self_churn = []         # daily tx volume selfchurn--------
            m_total             = []         # total money supply up to this day

            # retrieve txes and values per daily grouped txes in process period-
            day_date          = self.__date_period_start
            day_date_next     = day_date
            day_diff_to_start = (
                to_datetime(day_date) -
                to_datetime(Velo.start_date_gen)
            ).days

            for i_day in range(self.__date_period):
                # print a liveliness message if criteria are matched------------
                print_liveliness_message(
                    i_day,
                    "get_basic_tx_data()"
                )

                # initialize daily used data structures-------------------------
                date     = self.__date_period_start + timedelta(i_day)
                date_str = date.strftime("%y/%m/%d")

                txes_daily         .append([])
                index_day          .append(date_str)
                txes_vol           .append(0)
                txes_count         .append(0)
                txes_fees          .append(0)
                txes_dust_fees     .append(0)
                txes_dust_inpval   .append(0)
                txes_vol_self_churn.append(0)

                # transform date variables--------------------------------------
                # day_date_net_prev = day_date_next
                day_date = day_date_next
                day_date_next += timedelta(days=1)

                # get minimum and maximum block_height according to actual day--
                block_height_min = Velo.block_times[
                    Velo.block_times.index >= day_date
                ].iloc[0][0]

                block_height_max = Velo.block_times[
                    Velo.block_times.index >= day_date_next
                ].iloc[0][0]

                # get list of aggregated coin supply per given block height-----
                m_total.append(Velo.f_m_total_of_block_height[block_height_min])

                # retrieve daily txes and values per block in daily blockrange--
                for i_bh in range(block_height_min, block_height_max):
                    block = Velo.chain[i_bh]
                    for tx in block:
                        retrieve_per_tx_daily(i_day, tx)

            #--used by subsequent instance level functions----------------------
            self.__txes_daily = txes_daily

            # append results to queue dictionary--------------------------------
            self.__queue_dict["index_day"]         = index_day
            self.__queue_dict["tx_count"]          = txes_count
            self.__queue_dict["tx_fees"]           = txes_fees
            self.__queue_dict["tx_dust_fees"]      = txes_dust_fees
            self.__queue_dict["tx_dust_inpval"]    = txes_dust_inpval
            self.__queue_dict["tx_vol"]            = txes_vol
            self.__queue_dict["tx_vol_self_churn"] = txes_vol_self_churn
            self.__queue_dict["m_total"]           = m_total

            # append results to queue dictionary--------------------------------
            self.__queue_dict["index_day"]         = index_day
            self.__queue_dict["tx_count"]          = txes_count
            self.__queue_dict["tx_fees"]           = txes_fees
            self.__queue_dict["tx_dust_fees"]      = txes_dust_fees
            self.__queue_dict["tx_dust_inpval"]    = txes_dust_inpval
            self.__queue_dict["tx_vol"]            = txes_vol
            self.__queue_dict["tx_vol_self_churn"] = txes_vol_self_churn
            self.__queue_dict["m_total"]           = m_total

            #--test and normal returns------------------------------------------
            if Velo.test_level > 0:
                s_txes_vol_self_churn = str(txes_vol_self_churn)
                self.__queue_dict["txes_vol_self_churn"] = s_txes_vol_self_churn

            if 2 <= Velo.test_level and Velo.test_level <= 9:
                self.__queue.put([self.process_id, self.__queue_dict])
                return True

            return False

        def get_measurements():
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
                        for out in Velo.heur_select.change(tx):
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
                start_date_gen   = to_datetime(Velo.start_date_gen)
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
                    m_circ_per_day = [0 for i in range(time_windows_len)]

                    # initialize first block heights/day index of txes----------
                    first_block_height = []
                    # last_block_height  = daychunk[-1].block_height

                    first_block_height.append(daychunk[0].block_height)
                    day_index = Velo.f_index_day_of_block_height[first_block_height[0]]

                    # initalize first block/height/day index for lookback window
                    for i in range(1, time_windows_len):
                        i_day = int(day_index - time_windows[i])

                        if i_day < 0:
                            i_day = 0

                        first_block_height.append(Velo.f_block_height_of_id_day[i_day])

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
                for i in range(time_windows_len):
                    self.__queue_dict[
                        "m_circ_{}_{}".format(
                            type,
                            time_windows[i],
                        )
                    ] = m_circ[i]

                # hande test_level cases----------------------------------------
                if 10 <= Velo.test_level and Velo.test_level <= 12:
                    self.__queue.put([self.process_id, self.__queue_dict])
                    return True

                return False

            if type == "":
                if m_circ("wh_bill") == True: return True

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

            "Whole bill"-approach:----------------------------------------------

            Get coin supply in circulation for a given period,
            based on Tx inputs created in earlier periods and new
            minted coins.

            Here: Get circulating supply for each day
            => extract the transactions for each day that satisfy certain
            characteristics. We need tx inputs that where processed and
            generated before the given period. Additionally we need newly
            minted coins that where transacted during the given period.

            "Moved coin"-approach:----------------------------------------------

            Get coin supply in circulation for a given period, based on Tx
            inputs created in earlier periods and new minted coins.
            Inputs converting to change outputs are substracted based on
            either "FIFO" or "LIFO". It is assumed, that inputs are first
            filling up outputs controlled by third party public keys,
            rather than change outputs.

            Here: Get circulating supply for each day

            *) Dust transaction shouldn't be included!
               If all inputs are zero, then the fee and the outputs are
               all zero. Thus, we ignore this transaction since only
               count "time since last spend", which does not occure here.
               Eventually, the weight of this transaction is zero, which
               means that we would not include it in our computation
               anyway.
               fee: output-input
               fee = output => input = 0
               fee > output => input < 0
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

            def handle_tx_sdd_raw(tx):
                """
                Compute and return satoshi days desdroyed (sdd) per tx.
                """
                tx_block_time          = tx.block_time
                sdd_per_tx             = 0
                secs_per_day           = 86400 # 24*60*60

                if tx.is_coinbase:
                    return 0

                for input_i in tx.inputs:
                    time_sls_input_i = (
                        tx_block_time - input_i.spent_tx.block_time
                    )

                    secs_sls_input_i = time_sls_input_i.total_seconds()
                    days_sls_input_i = secs_sls_input_i / secs_per_day

                    sdd_per_tx += days_sls_input_i * input_i.value

                return sdd_per_tx

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
                    for out in Velo.heur_select.change(tx):
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
                Whole bill approach: complete agg value of inputs
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

            # print starting message--------------------------------------------
            Velo.logger.info(
                "{}[{}{}/{:03}{}]{}  {}".format(
                    cs.RES,
                    cs.PRGnBC,
                    self.process_name,
                    Velo.process_cnt-1,
                    cs.RES,
                    cs.WHI,
                    "get measurement results",
                )
            )

            # initialize basic variables/data structures------------------------
            time_windows     = Velo.time_windows
            time_windows_len = len(time_windows)
            satoshi_dd_raw   = []
            dormancy_raw     = [[] for i in range(time_windows_len)]
            m_circ_mc_lifo   = [[] for i in range(time_windows_len)]
            m_circ_mc_fifo   = [[] for i in range(time_windows_len)]
            m_circ_wh_bill   = [[] for i in range(time_windows_len)]

            # get day_index of first block in self.__txes_daily-----------------
            first_block_height = self.__txes_daily[0][0].block_height
            day_index_first = Velo.f_index_day_of_block_height[
                first_block_height
            ]

            # retrieve data for each daychunk of txes---------------------------
            for daychunk in self.__txes_daily:
                # initialize daily values---------------------------------------
                satoshi_dd_raw.append(0)

                for t_w in range(time_windows_len):
                    dormancy_raw[t_w]  .append(0)
                    m_circ_mc_lifo[t_w].append(0)
                    m_circ_mc_fifo[t_w].append(0)
                    m_circ_wh_bill[t_w].append(0)

                # if no transactions happened, continue-------------------------
                if daychunk == []:
                    continue

                # initialize first block heights/day index of txes--------------
                first_block_height = daychunk[0].block_height
                day_index = Velo.f_index_day_of_block_height[first_block_height]

                # print some liveliness message---------------------------------
                day_diff_to_start = day_index - day_index_first

                print_liveliness_message(
                    day_diff_to_start,
                    "get_measurements()"
                )
                # retrieve tx-wise values for money in effective cirulation-----
                for tx in daychunk:
                    # Here, dust transaction shouldn't be included, see *)------
                    if tx.output_value <= tx.fee:
                        continue

                    # get unwindowed values-------------------------------------
                    # (A1) (b_i \cdot delta t_i in dormancy paper)--------------
                    satoshi_dd_per_tx     = handle_tx_sdd_raw(tx)
                    m_circ_mc_lifo_per_tx = handle_tx_mc_xifo(
                        tx,
                        first_block_height,
                        False,
                    )
                    m_circ_mc_fifo_per_tx = handle_tx_mc_xifo(
                        tx,
                        first_block_height,
                        True,
                    )
                    m_circ_wh_bill_per_tx = handle_tx_wh_bill(
                        tx,
                        first_block_height,
                    )
                    satoshi_dd_raw[-1]   += satoshi_dd_per_tx
                    # prepare data structures for windowed values---------------
                    for t_w in range(time_windows_len):
                        dormancy_raw[t_w][-1]   += satoshi_dd_per_tx
                        m_circ_mc_lifo[t_w][-1] += m_circ_mc_lifo_per_tx
                        m_circ_mc_fifo[t_w][-1] += m_circ_mc_fifo_per_tx
                        m_circ_wh_bill[t_w][-1] += m_circ_wh_bill_per_tx

            # put results into __queue_dict-------------------------------------
            self.__queue_dict["sdd_raw"] = satoshi_dd_raw

            for t_w_i in range(time_windows_len):
                t_w = time_windows[t_w_i]

                dormancy_raw_str   = "dormancy_raw_{}"  .format(t_w)
                m_circ_mc_lifo_str = "m_circ_mc_lifo_{}".format(t_w)
                m_circ_mc_fifo_str = "m_circ_mc_fifo_{}".format(t_w)
                m_circ_wh_bill_str = "m_circ_wh_bill_{}".format(t_w)

                self.__queue_dict[dormancy_raw_str]   = dormancy_raw[t_w_i]
                self.__queue_dict[m_circ_mc_lifo_str] = m_circ_mc_lifo[t_w_i]
                self.__queue_dict[m_circ_mc_fifo_str] = m_circ_mc_fifo[t_w_i]
                self.__queue_dict[m_circ_wh_bill_str] = m_circ_wh_bill[t_w_i]

            # hande test_level cases--------------------------------------------
            if 10 <= Velo.test_level and Velo.test_level <= 12:
                self.__queue.put([self.process_id, self.__queue_dict])
                return True

            return False

        # print starting message------------------------------------------------
        process_name_str = "{}[{}{}/{:03}{}]{}  {}".format(
            cs.RES,
            cs.PRGnBA,
            self.process_name,
            Velo.process_cnt-1,
            cs.RES,
            cs.WHI,
            "--stage_id = {}--".format(self.stage_id)
        )

        Velo.logger.info("{}{}  Starting".format(
            process_name_str,
            cs.WHI,
        ))
        #-----------------------------------------------------------------------
        if self.stage_id == 0:
            if get_basic_tx_data() == True: return

            self.__queue.put([
                self.stage_id,
                self.process_id,
                self.__queue_evnt_dict
            ])
            self.__queue_evnt_dict = {}

            if get_measurements() == True: return

            while True:
                msg_from_queue = self.__queue_evnt.get()
                msg_stage_id   = msg_from_queue[0]
                msg_process_id = msg_from_queue[1]
                self.__queue_evnt.task_done()

                if msg_stage_id == self.stage_id and msg_process_id == self.process_id:
                    break

            self.stage_id += 1

        if self.stage_id == 1:
            pass

        # put all necessary data to parent process through multiprocess queue---
        Velo.logger.debug(
            "{}[{}{}/{:03}{}]{}  {} Sending results".format(
                cs.RES,
                cs.PRGnBE,
                self.process_name,
                Velo.process_cnt-1,
                cs.RES,
                cs.WHI,
                "--stage_id = {}--".format(self.stage_id)
            )
        )

        self.__queue.put([self.stage_id, self.process_id, self.__queue_dict])

        Velo.logger.debug(
            "{}[{}{}/{:03}{}]{}  {} terminating".format(
                cs.RES,
                cs.PRGnBE,
                self.process_name,
                Velo.process_cnt-1,
                cs.RES,
                cs.WHI,
                "--stage_id = {}--".format(self.stage_id)
            )
        )

            while True:
                msg_from_queue = self.__queue_evnt.get()
                msg_stage_id   = msg_from_queue[0]
                msg_process_id = msg_from_queue[1]
                self.__queue_evnt.task_done()

                if msg_stage_id == self.stage_id and msg_process_id == self.process_id:
                    break

            self.stage_id += 1

        if self.stage_id == 1:
            if get_comp_meas() == True: return

        # put all necessary data to parent process through multiprocess queue---
        Velo.logger.debug(
            "{}[{}{}/{:03}{}]{}  {} Sending results".format(
                cs.RES,
                cs.PRGnBE,
                self.process_name,
                Velo.process_cnt-1,
                cs.RES,
                cs.WHI,
                "--stage_id = {}--".format(self.stage_id)
            )
        )

        self.__queue.put([self.stage_id, self.process_id, self.__queue_dict])

        Velo.logger.debug(
            "{}[{}{}/{:03}{}]{}  {} terminating".format(
                cs.RES,
                cs.PRGnBE,
                self.process_name,
                Velo.process_cnt-1,
                cs.RES,
                cs.WHI,
                "--stage_id = {}--".format(self.stage_id)
            )
        )

        return

if __name__ == "__main__":
    print("{}Use this file with script.py!{}".format(cs.RED,cs.RES))
    exit(0)
