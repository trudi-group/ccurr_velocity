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
from itertools           import chain as it_chain
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

    # class attribute representing blocksci chain object used by all instances--
    chain          = None                 # the mainly blockcain object
    cluster_mgr    = None                 # manager for clustering addresses
    cluster_cnt    = None                 # number of address clusters
    cluster_range  = None
    cluster_max_id = None                 # id of cluster with max address count
    block_times    = None
    heur_select    = None
    start          = None
    end            = None

    #lookup functions-----------------------------------------------------------
    f_index_day_of_block_height = []                 # f_index-day(block height)
    f_index_day_of_id_txes      = []                 # f_index-day(txes-id)
    f_block_height_of_id_day    = []                 # f_block-height(day-id)
    f_tx_vol_agg_of_id_day      = []                 # f_tx-vol-agg(day-id)
    f_tx_count_of_id_day        = []                 # f_tx-count(day-id)
    f_dates_of_id_sub_proc      = []                 # f_dates(subprocess-id)
    f_m_total_of_block_height   = None               # f_m-total(block height)

    #--remaining class attributes-----------------------------------------------
    date_format       = "%Y-%m-%d %H:%M:%S"    # date formatting information
    start_date_gen    = "01/03/2009"           # date of bitcoin genesis
    path_data_output  = None                   # path for data output
    log_level         = INFO                   # default logging level
    logger            = None                   # logging object
    test_level        = 0                      # default basic test level
    process_cnt       = 0                      # count of sub procs for printing

    #==[ CLASSLEVEL | SessionSetup & precaching of global data struct ]=========
    def setup(
        logger,
        args,
     ):
        """
        Initialize session and with that, the main blockchain object used by
        each instance of the Velo class.
        """
        def setup_heuristics(heur_choice):
            """
            compare
            https://citp.github.io/BlockSci/reference/heuristics/change.html
            Returns heuristics dictionary of selected heuristics
            """

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
            Velo.heur_select = heur[heur_choice]

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

            # print function starting message-----------------------------------
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

        def setup_clustering_count(
            path_cluster,
            skip = False,
            overwrite = False
        ):
            """
            count addresses per cluster and retrieve id and size of biggest
            cluster.
            """
            Velo.cluster_max_size = 0
            Velo.cluster_max_id   = 0

            # load blocksci clustering manager----------------------------------
            Velo.cluster_mgr = ClusterManager(
                path_cluster,
                Velo.chain,
            )

            # return assumingly largest cluster (id = 32), when skip is on------
            if True == skip:
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

            # print starting message--------------------------------------------
            Velo.logger.info("{}[{}  clustering   {}]{}  Start".format(
                cs.RES,
                cs.PRGnBA,
                cs.RES,
                cs.PRGnBA,
            ))

            # renew clustering cache, if desired--------------------------------
            if overwrite == True:
                Velo.cluster_mgr = ClusterManager.create_clustering(
                     path_cluster,
                     Velo.chain,
                     Velo.heur_select,
                     overwrite,
                )

            #--get largest cluster via subproccesing----------------------------
            Velo.cluster_range = Velo.cluster_mgr.clusters()
            Velo.cluster_cnt   = len(Velo.cluster_range)
            sub_proc_cls_range = ceil(Velo.cluster_cnt/(cpu_count()))

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

        #--setup of static variables on class level-----------------------------
        Velo.logger           = logger
        Velo.path_data_output = args.path_data_output
        Velo.test_level       = args.test
        Velo.start_date       = args.start_date
        Velo.end_date         = args.end_date
        Velo.time_windows     = list(map(int, str(args.time_window).split(",")))
        Velo.cnt_cls_only     = args.count_clustering_only

        #--setup of locally used variables--------------------------------------
        time_windows_len = len(Velo.time_windows)

        if Velo.test_level > 0:
            Velo.end_date = "01/01/2012"

        #--load chain object----------------------------------------------------
        Velo.chain = Blockchain(args.path_data_input)

        #--print basic data in debug mode---------------------------------------
        Velo.logger.debug(
            "date of first block = {}".format(Velo.chain[0].time)
        )
        Velo.logger.debug(
            "date of last block  = {}".format(Velo.chain[-1].time)
        )

        #--load heurictics object-----------------------------------------------
        setup_heuristics(args.heur_choice)

        #--load clustering object-----------------------------------------------
        setup_clustering_count(
            path_cluster=args.path_cluster,
            skip=True,
            overwrite=False
        )

        #--remaining subfunctions-----------------------------------------------
        setup_m_total_of_block_height()

        #--get block times, block count and day counts--------------------------
        Velo.logger.info("{}[{}daily txesIndex{}]{}  Loading".format(
            cs.RES,
            cs.PRGnBA,
            cs.RES,
            cs.PRGnBA,
        ))

        Velo.block_times = DataFrame(
            [block.time for block in Velo.chain],
            columns = ["date"],
        )
        Velo.block_times["height"] = Velo.block_times.index
        Velo.block_times.index     = Velo.block_times["date"]
        del Velo.block_times["date"]

        cnt_blocks = Velo.block_times[
            Velo.block_times.index >= to_datetime(Velo.end_date)
        ].iloc[0][0]

        cnt_days = (
            to_datetime(Velo.end_date) - to_datetime(Velo.start_date_gen)
        ).days

        #--get minimum and maximum block_height according to start/end_date-----
        block_height_min = Velo.block_times[
            Velo.block_times.index >= to_datetime(Velo.start_date_gen)
        ].iloc[0][0]

        block_height_max = Velo.block_times[
            Velo.block_times.index >= to_datetime(Velo.end_date)
        ].iloc[0][0]

        #--get trancation counts between start/end_date blocks------------------
        cnt_txes = 0
        for i_bh in range(block_height_min, block_height_max):
            cnt_txes += Velo.chain[i_bh].tx_count

        #--setup data for subprocessing-----------------------------------------
        day_date              = to_datetime(Velo.start_date_gen)
        day_date_next         = day_date
        sub_proc_tx_num_max   = ceil(cnt_txes/(cpu_count()))
        sub_proc_txes_counter = 0
        sub_proc_date_start   = day_date
        sub_proc_date_end     = day_date + timedelta(days=1)
        sub_proc_date_period  = 1

        for day in range(cnt_days):
            # transform date variables------------------------------------------
            day_date = day_date_next
            day_date_next += timedelta(days=1)

            # initialize daily aggregated valouts per time_window---------------
            Velo.f_tx_vol_agg_of_id_day.append([])
            f_tx_vol_agg_of_id_day_sum = 0
            txes_count_per_day         = 0

            # get minimum and maximum block_height according to actual day------
            block_height_min = Velo.block_times[
                Velo.block_times.index >= day_date
            ].iloc[0][0]

            block_height_max = Velo.block_times[
                Velo.block_times.index >= day_date_next
            ].iloc[0][0]

            # retrieve values per block in daily blockrange---------------------
            for i_bh in range(block_height_min, block_height_max):
                Velo.f_index_day_of_block_height.append(day)
                block = Velo.chain[i_bh]

                block_txes_count = block.tx_count
                f_tx_vol_agg_of_id_day_sum += block.output_value

                for tx_i in range(block_txes_count):
                    Velo.f_index_day_of_id_txes.append(day)

                txes_count_per_day += block_txes_count

            Velo.f_block_height_of_id_day.   append(block_height_min)
            Velo.f_tx_count_of_id_day       .append(txes_count_per_day)
            Velo.f_tx_vol_agg_of_id_day[day].append(f_tx_vol_agg_of_id_day_sum)

            # calculate data for sub processing periods-------------------------
            # Assumption: There is no day with cnt_txes > sub_proc_tx_num_max
            if day == 0:
                # txes number of first day, don't change dates
                sub_proc_txes_counter = txes_count_per_day

            else:
                # txes numbers of all other days
                txes_counter_next = sub_proc_txes_counter + txes_count_per_day

                if txes_counter_next < sub_proc_tx_num_max:
                    sub_proc_date_end    += timedelta(days = 1)
                    sub_proc_date_period += 1
                    sub_proc_txes_counter   = txes_counter_next

                else:
                    sub_proc_date = [
                        sub_proc_date_start,
                        sub_proc_date_end,
                        sub_proc_date_period,
                        sub_proc_txes_counter
                    ]
                    Velo.f_dates_of_id_sub_proc.append(sub_proc_date)
                    Velo.logger.debug("{}: {}".format(day, sub_proc_date))

                    sub_proc_date_start   = sub_proc_date_end
                    sub_proc_date_end     = sub_proc_date_start
                    sub_proc_date_end    += timedelta(days=1)
                    sub_proc_date_period  = 1
                    sub_proc_txes_counter = txes_count_per_day

                if day == (cnt_days-1):
                    sub_proc_date = [
                        sub_proc_date_start,
                        sub_proc_date_end,
                        sub_proc_date_period,
                        sub_proc_txes_counter
                    ]
                    Velo.f_dates_of_id_sub_proc.append(sub_proc_date)
                    Velo.logger.debug(
                        "{}: {} (last)".format(day, sub_proc_date)
                    )

            # add sum of agg daily valouts of n days, n in Velo.time_windows----
            for t_w in range(1, time_windows_len):
                Velo.f_tx_vol_agg_of_id_day[day].append(0)
                valout_agg_t_w_last = 0
                if day > 0:
                    valout_agg_t_w_last = Velo.f_tx_vol_agg_of_id_day[day-1][t_w]

                # add the current daily calculations
                txes_vol_agg_for_t_w = (
                    valout_agg_t_w_last + f_tx_vol_agg_of_id_day_sum
                )
                # substract the calculations right before the current window
                if day >= Velo.time_windows[t_w]:
                    txes_vol_agg_for_t_w -=  Velo.f_tx_vol_agg_of_id_day[
                        day - Velo.time_windows[t_w]
                    ][0]

                Velo.f_tx_vol_agg_of_id_day[day][t_w] = txes_vol_agg_for_t_w

        return

    #==[ CLASSLEVEL | finalize results and get final data frames and csv ]======
    def get_results_finalized(
        results_raw,
        index_label = "",
    ):
        """
        Builds a pandas data frame and csv from pre-computed data.
        """

        def get_comp_meas_from_summands(
            results_raw,
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

            sdd_summands      = results_raw["summands_dsls_daily"]
            dormancy_summands = results_raw["summands_dsls_daily_wghtd"]
            time_windows      = Velo.time_windows
            time_windows_len  = len(time_windows)

            #--(C1 & C2:)-------------------------------------------------------
            dormancy_summands = list(zip(*dormancy_summands))
            sdd_daily         = []
            dormancy_daily    = []
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

            results_raw["sdd"]      = list(zip(*sdd_daily))
            results_raw["dormancy"] = list(zip(*dormancy_daily))

            #--free used data structures----------------------------------------
            del results_raw["summands_dsls_daily"]
            del results_raw["summands_dsls_daily_wghtd"]

            return

        def df_prepare_time_window(
            df_type,
            df_data,
            df_index,
        ):
            """
            Prepare dataframe according to time window values.
            """
            df_time_window = DataFrame(
                df_data,
                columns = [
                    "{}_{}".format(
                        df_type,
                        w,
                    ) for w in Velo.time_windows
                ],
                index=df_index
            )

            return df_time_window

        def df_merge(
            df_merge_target,
            df_to_be_merged
        ):
            """
            Merge pandas dataframe together.
            """

            df_merge_target = df_merge_target.merge(
                df_to_be_merged,
                how='outer',
                left_index=True,
                right_index=True
            )

            Velo.logger.info("{}{}[{}built dataframe{}{}]  {}".format(
                cs.RES,
                cs.WHI,
                cs.PRGnBI,
                cs.RES,
                cs.WHI,
                df_to_be_merged.columns[0],
            ))

            return df_merge_target

        #--Start of get_results_finalized()-------------------------------------
        Velo.logger.info("{}{}[{}build&write csv{}{}]".format(
            cs.RES,
            cs.WHI,
            cs.PRGnBI,
            cs.RES,
            cs.WHI,
        ))

        #--prepare measures to be compared with velocity------------------------
        get_comp_meas_from_summands(
            results_raw=results_raw,
            min_frac = 1,
        )

        #--basic data-----------------------------------------------------------
        df_index = results_raw["index_day"]

        #--create first part of final pandas data frame-------------------------
        results_raw_basic_types = [
            "index_day",
            "tx_count",
            "tx_fees",
            "tx_vol",
            "tx_vol_self_churn",
            "m_total",
        ]

        results_raw_basic = {k: results_raw[k] for k in results_raw_basic_types}
        df_final = DataFrame.from_dict(results_raw_basic)
        df_final = df_final.set_index("index_day")

        #--handle m_circ df_types and merge to final data frame-----------------
        m_circ_types = [
            "m_circ_wh_bill",
            "m_circ_mc_lifo",
            "m_circ_mc_fifo",
        ]

        for m_circ_type in m_circ_types:
            df_m_circ = df_prepare_time_window(
                df_type=m_circ_type,
                df_data=results_raw[m_circ_type],
                df_index=df_index,
            )

            df_final = df_merge(
                df_final,
                df_m_circ,
            )

        #--handle measurements from literature and merge to finale data frame---
        comp_meas_types = [
            "dormancy",
            "sdd",
        ]

        for comp_meas_type in comp_meas_types:
            df_comp_meas = df_prepare_time_window(
                df_type=comp_meas_type,
                df_data=results_raw[comp_meas_type],
                df_index=df_index
            )

            df_final = df_merge(
                df_final,
                df_comp_meas,
            )

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
        date_id,
    ):
        self.process_id   = process_id
        self.process_name = process_name
        self.__queue      = queue

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
        self.__queue_dict = {}

    #==[ INSTALEVEL | Retrieve of desired data ]================================
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
            def print_liveliness_message(i_day):
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
                            "get_basic_tx_data()",
                        )
                    )

                return

            def retrieve_per_tx_daily(
                i_day,
                tx,
            ):
                """
                This function retrieves basic blockchain values and aggregates
                them into daily chunks.
                """
                try:
                    if tx.block_height >= len(Velo.f_index_day_of_block_height):
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
                                len(Velo.f_index_day_of_block_height)
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
            txes_daily          = []         # all transactions of one day
            index_day           = []         # index list of day ids
            txes_count          = []         # daily count of transactions
            txes_fees           = []         # daily agg. tx fees
            txes_dust_fees      = []         # daily agg. tx dust fees
            txes_dust_inpval    = []         # daily agg. tx dust input values
            txes_vol            = []         # daily transaction volume
            txes_vol_self_churn = []         # daily tx volume selfchurn
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
                print_liveliness_message(i_day)

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

            #--test and normal returns------------------------------------------
            if Velo.test_level > 0:
                s_txes_vol_self_churn = str(txes_vol_self_churn)
                self.__queue_dict["txes_vol_self_churn"] = s_txes_vol_self_churn

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
                day_index = Velo.f_index_day_of_block_height[first_block_height[0]]

                for i in range(1, time_windows_len):
                    i_day = int(day_index - time_windows[i])

                    if i_day < 0:
                        i_day = 0

                    first_block_height.append(
                        Velo.f_block_height_of_id_day[i_day]
                    )

                # txes in daychunk----------------------------------------------
                for tx in daychunk:
                    # *)
                    if tx.output_value <= tx.fee:
                        continue

                    val_inp_tx = tx.input_value

                    txes_vol_agg_per_day_tw = Velo.f_tx_vol_agg_of_id_day[
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
                        if not txes_vol_agg_per_day_tw[t_w] == 0:
                            summand_dsls_per_tx_wghtd = (
                                summand_dsls_per_day
                                /txes_vol_agg_per_day_tw[t_w]
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
