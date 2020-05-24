import logging
from numpy           import ndarray, concatenate
from time            import sleep
from random          import randint
from hashlib         import md5
from threading       import Thread
from multiprocessing import Process, JoinableQueue, cpu_count
from datetime        import date, datetime, timedelta
from pandas          import DatetimeIndex
from math            import ceil
from velo            import Velo
from colorstrings    import ColorStrings as cs

class Multiprocess:
    """
    This class constitutes a multiprocessing framework for the Veloclass. It is
    based on the python multiprocessing module. Its basic functionality is to
    partition the given blockchain data in terms of transacation counts with
    regard to the amount of available cpu cores and to join calculated results
    of each subprocess into a total result.
    """

    logger           = None
    log_level        = logging.INFO
    test_level       = 0
    path_data_output = None
    cpu_cnt          = 0
    stage_fin        = 1

    # class attribute for concatenation
    cat_nxt = []

    #subprocessing related
    queue_evnt             = []
    processes              = []
    process_instances      = []
    process_result         = []
    process_result_cat_fin = None
    process_last_started   = -1
    process_cnt            = 0 #to print max number of started subprocesses

    #==[ CLASSLEVEL ]===========================================================
    def setup(
        logger,
        args,
    ):
        #status message---------------------------------------------------------
        Multiprocess.logger    = logger
        Multiprocess.log_level = args.log_level
        Multiprocess.logger.debug(
            "{}[{}function       {}]   {}   {}".format(
                cs.RES,
                cs.CYA,
                cs.RES,
                "{}[Started        ]".format(cs.WHI),
                "Multiprocess.setup()",
            )
        )

        #-----------------------------------------------------------------------
        Multiprocess.test_level       = int(args.test_level)
        Multiprocess.cpu_cnt          = cpu_count()
        Multiprocess.path_data_output = args.path_data_output
        Multiprocess.stage_fin        = 2

        #--Set cpu count manually for debugging---------------------------------
        if int(args.cpu_count) >= 0:
            Multiprocess.cpu_cnt = int(args.cpu_count)

        #--Initialize stage_fin related data structures-------------------------
        for stage_id in range(Multiprocess.stage_fin):
            Multiprocess.cat_nxt.append(0)

        #status message---------------------------------------------------------
        Multiprocess.logger.debug(
            "{}[{}function       {}]   {}   {}".format(
                cs.RES,
                cs.CYA,
                cs.RES,
                "{}[Finished       ]".format(cs.WHI),
                "Multiprocess.setup()",
            )
         )
        return

    def processes_kill_all():
        """
        Kills all subprocesses.
        """
        def processes_kill(process_id):
            """
            Kills a subprocess by its process_id.
            """
            process_to_terminate = Multiprocess.processes[process_id]
            if process_to_terminate is not None:
                process_to_terminate.terminate()

            return

        for process_id in range(0,len(Multiprocess.processes)):
            processes_kill(process_id)

        return

    def test_concat():
        """
        This function provides means to test the correct functioning of the
        concatenation of the subprocess results.
        """
        def get_data_for_df_test(
            start_date,
            end_date,
            test,
        ):
            """
            """
            process_id            = 0
            process_instances_ret = []
            queue                 = JoinableQueue()
            date_format           = "%m/%d/%Y"

            for date in range(3):
                end_date_o = datetime.strptime(
                    end_date[date],
                    date_format,
                ).date()
                start_date_o = datetime.strptime(
                    start_date[date],
                    date_format,
                ).date()

                process_name = "process_{:03d}".format(process_id)

                process_inst = Velo(
                    process_id=process_id,
                    process_name=process_name,
                    queue=queue,
                    date_id=date,
                )

                process = Process(target=process_inst.run)

                process_id               += 1
                Multiprocess.process_cnt += 1
                Multiprocess.process_instances.append(process_inst)
                Multiprocess.processes        .append(process)

            for i in range(Multiprocess.process_cnt):
                Multiprocess.processes[i].start()

            for i in range(Multiprocess.process_cnt):
                msg_process_id = None
                msg_from_queue = ""
                while True:
                    msg_from_queue = queue.get()
                    msg_stage_id   = msg_from_queue[0]
                    msg_process_id = msg_from_queue[1]
                    process_instances_ret.append(msg_from_queue[2])
                    queue.task_done()
                    break

                Multiprocess.processes[msg_process_id].join()

            return process_instances_ret

        def ds_cmp(
            test_number,
            test_name,
            ds_a_a,
            ds_a_b,
            ds_b,
        ):
            """
            Takes two data structures ds_a_a, ds_a_b, merges them (ds_a) and
            compares ds_a with a third data structute ds_b via comparing their
            md5 hashes.
            """
            def ds_to_list(ds):
                """
                This function transforms a given data structure to a list.
                """
                l_ds = None
                if isinstance(ds, list):
                    l_ds = ds
                elif isinstance(ds, type(ndarray)):
                    Multiprocess.logger.debug(
                        "test #[{}]:  ds_a_a type = {}".format(
                            test_number,
                            str(type(ds_a_a))
                        )
                    )
                    l_ds = ds.tolist()
                else:
                    l_ds = ds.tolist()

                return l_ds

            def ds_export(
                ds,
                version,
                test_number,
            ):
                """
                This function can be applied in order to export a given data
                structure to a textfile.
                """
                path_data_output_l =  "{}_ds/ds_{}_{}.txt".format(
                    Multiprocess.path_data_output,
                    test_number,
                    version,
                )
                with open(path_data_output_l, 'w') as text_file:
                    text_file.write("{}".format(ds))

                return

            def ds_hash_cmp(
                s_ds_a,
                s_ds_b,
            ):
                """
                This function compute the md5 hashes of two given data structures
                in form of strings and compares them.
                """
                hash_a = md5()
                hash_b = md5()

                hash_a.update(str.encode(s_ds_a))
                hash_b.update(str.encode(s_ds_b))

                s_hash_a = hash_a.hexdigest()
                s_hash_b = hash_b.hexdigest()

                if s_hash_a == s_hash_b:
                    return True

                return False

            path_data_output = Multiprocess.path_data_output
            ds_a = None

            #derive ds_a from a_a and a_b
            ds_a_a_bak = ds_a_a

            if type(ds_a_a) == list:
                ds_a = ds_a_a + ds_a_b

            elif type(ds_a_a) == DatetimeIndex:
                ds_a = ds_a_a_bak.append(ds_a_b)

            elif type(ds_a_a) == str:
                ds_a = "{}, {}".format(ds_a_a[0:-1], ds_a_b[1:])

            else:
                ds_a = concatenate( [ ds_a_a, ds_a_b ])

            s_da_a_a = None
            s_da_a_b = None
            s_da_a   = None
            s_da_b   = None

            #transform to list
            if type(ds_a_a) != str:
                s_ds_a_a = str(ds_to_list(ds_a_a))
                s_ds_a_b = str(ds_to_list(ds_a_b))
                s_ds_a   = str(ds_to_list(ds_a))
                s_ds_b   = str(ds_to_list(ds_b))
            else:
                s_ds_a_a = ds_a_a
                s_ds_a_b = ds_a_b
                s_ds_a   = ds_a
                s_ds_b   = ds_b

            #put each tx in newline
            s_ds_a_a = s_ds_a_a.replace( "), Tx(", "),\nTx(")
            s_ds_a_b = s_ds_a_b.replace( "), Tx(", "),\nTx(")
            s_ds_a   = s_ds_a  .replace( "), Tx(", "),\nTx(")
            s_ds_b   = s_ds_b  .replace( "), Tx(", "),\nTx(")

            #export to file
            ds_export( s_ds_a_a, "_a_a", test_number)
            ds_export( s_ds_a_b, "_a_b", test_number)
            ds_export( s_ds_a,   "_a",   test_number)
            ds_export( s_ds_b,   "_b",   test_number)

            #hashing
            hash_equal = None
            if ds_hash_cmp(s_ds_a, s_ds_b) == True:
                hash_equal = "{}Y{}".format(cs.PRGnBH, cs.WHI)
            else:
                hash_equal = "{}N{}".format(cs.REE, cs.WHI)

            ret_str_pre = "{}[{}testing cat #{}{}]".format(
                cs.WHI,
                cs.PRGnBH,
                test_number,
                cs.WHI,
            )
            ret_str     = "{}  {}same: [{}] -- {}{}".format(
                ret_str_pre,
                cs.WHI,
                hash_equal,
                test_name,
                cs.RES,
            )

            Multiprocess.logger.info(ret_str)
            return

        path_data_output = Multiprocess.path_data_output
        Multiprocess.logger.info("Starting mode: Test[Concatenation]")
        #start_date_a_a = "01/01/2010"
        #end_date_a_a   = "02/01/2011"

        #start_date_a_b = "02/02/2011"
        #end_date_a_b   = "03/01/2012"

        #start_date_b   = "01/01/2010"
        #end_date_b     = "03/01/2012"

        start_date_a_a = "01/01/2010"
        end_date_a_a   = "02/01/2010"

        start_date_a_b = "02/02/2010"
        end_date_a_b   = "03/01/2010"

        start_date_b   = "01/01/2010"
        end_date_b     = "03/01/2010"

        ret = get_data_for_df_test(
            start_date=[start_date_a_a, start_date_a_b, start_date_b],
            end_date=[end_date_a_a, end_date_a_b, end_date_b],
            test=Multiprocess.test_level,
        )

        processes_test = []

        ret_cnt  = len(ret[0])
        ret_keys = list(ret[0].keys())
        for i in range(ret_cnt):
            test_number = "{:02d}".format(i+1)
            i_name = ret_keys[i]

            process = Process(target = ds_cmp, args=(
                test_number,
                i_name,
                ret[0][i_name],
                ret[1][i_name],
                ret[2][i_name],
            ))

            processes_test.append(process)

        for i in range(ret_cnt):
            processes_test[i].start()

        for i in range(ret_cnt):
            processes_test[i].join()

        print("Exiting Multiprocess test: concat")

        return

    def test_multiprocessing ():
        """
        Test the basic multiprocessing framework.
        """

        MultiprocessTest.setup(
            Multiprocess.logger,
            Multiprocess.process_cnt,
        )

        results_raw = Multiprocess.run_subprocessed()

        ress = results_raw["process_id"]
        last_e = -1
        prt = ""
        for e in ress:
            if e != last_e +1:
                Multiprocess.logger.warning(
                    "Out of order! (last_e, e) = ({:03}, {:03})".format(
                        last_e,
                        e,
                    )
                )
                break

            if e % 6 == 0 and e > 0:
                Multiprocess.logger.info(prt)
                prt = ""

            prt    += "Result of {:03} | ".format(ress[e])
            last_e += 1

        Multiprocess.logger.info(prt)
        print("Exiting Multiprocess test: subprocessing")

        return

    def run_subprocessed():
        """
        This function retrieves required data in a multiprocessed fashion.
        """
        def subprocess_manage(queue):
            """
            This functions works as a multiprocess pool supplement.
            """
            #status message-----------------------------------------------------
            Multiprocess.logger.debug(
                "{}[{}function       {}]   {}   {}".format(
                    cs.RES,
                    cs.CYA,
                    cs.RES,
                    "{}[Started        ]".format(cs.WHI),
                    "Multiprocess.run_subprocessed.subprocess_manage",
                )
            )

            #-------------------------------------------------------------------
            start_allowed = True
            processes_fin = Multiprocess.process_cnt
            stage_id_fin  = Multiprocess.stage_fin-1

            #--Start cpu_cnt/process_cnt subprocesses on first stage------------
            if Multiprocess.cpu_cnt > Multiprocess.process_cnt:
                Multiprocess.cpu_cnt = Multiprocess.process_cnt

            for i in range(Multiprocess.cpu_cnt):
                Multiprocess.processes[i].start()
                Multiprocess.process_last_started += 1

                if not Multiprocess.processes[i].is_alive():
                    Multiprocess.logger.error(
                        "{}[{}process_{:03}/{:03}{}]  {}  Not running".format(
                            cs.RES,
                            cs.PRGnBA,
                            i,
                            Multiprocess.process_cnt-1,
                            cs.RES,
                            "[stage_id =  0  ]"
                        )
                    )

            process_id = Multiprocess.cpu_cnt - 1
            Multiprocess.logger.debug(
                "{}[{}process_{:03}{}-{}{:03}{}]   {}   {} ".format(
                    cs.RES,
                    cs.PRGnBA,
                    0,
                    cs.RES,
                    cs.PRGnBA,
                    Multiprocess.cpu_cnt-1,
                    cs.RES,
                    "                 ",
                    "{}Started processes on first stage".format(cs.WHI),
                )
            )

            #--retrieve subprocess results--------------------------------------
            while processes_fin > 0:
                # print some status message-------------------------------------

                Multiprocess.logger.debug(
                    "{}[{}process_xxx/{:03}{}]   {}   {}".format(
                        cs.RES,
                        cs.PRGnBG,
                        Multiprocess.process_cnt-1,
                        cs.RES,
                        "                 ",
                        "{}retrieving results".format(cs.WHI),
                    )
                )

                # handle queue data---------------------------------------------
                msg_from_queue     = queue.get()
                msg_stage_id       = msg_from_queue[0]
                msg_process_id     = msg_from_queue[1]
                msg_result         = msg_from_queue[2]
                msg_process_id_str = "{}[{}process_{:03}/{:03}{}]   {}".format(
                    cs.RES,
                    cs.PRGnBG,
                    msg_process_id,
                    Multiprocess.process_cnt-1,
                    cs.RES,
                    "{}[stage_id = {:2}  ]".format(
                        cs.WHI,
                        msg_stage_id,
                    ),
                )
               
                Multiprocess.process_result[msg_process_id][
                    msg_stage_id
                ] = msg_result 

                Multiprocess.logger.info("{}   {}results retrieved".format(
                    msg_process_id_str,
                    cs.PRGnBG,
                ))
                queue.task_done()

                if msg_stage_id == 0:
                    Multiprocess.queue_evnt[msg_process_id].put([
                        msg_stage_id,
                        msg_process_id,
                    ])

                # mark process as finished if all its stages are finished-------
                elif msg_stage_id == stage_id_fin:
                    Multiprocess.processes[msg_process_id].join()
                    processes_fin -= 1

                    Multiprocess.logger.info("{}   {}terminated/joined".format(
                        msg_process_id_str,
                        cs.PRGnBF,
                    ))

            #status message-----------------------------------------------------
            Multiprocess.logger.debug(
                "{}[{}function       {}]   {}   {}".format(
                    cs.RES,
                    cs.CYA,
                    cs.RES,
                    "{}[Finished       ]".format(cs.WHI),
                    "Multiprocess.run_subprocessed.subprocess_manage",
                )
            )

            return

        def concatenate(stage_id):
            """
            This function serves as a framework to concatenate results
            from subprocesses in order correctly.
            """
            def ds_cat(
                ds_res,
                ds_nxt_id,
                ds_nxt,
                process_name,
                stage_id,
            ):
                """
                This function concatenates two given data structures.
                """
                if ds_nxt_id != Multiprocess.cat_nxt[stage_id]:
                    Multiprocess.logger.error(
                        "{}[{}{}/{:03}{}]   {}   {}".format(
                            cs.RES,
                            cs.PRGnBE,
                            process_name,
                            Multiprocess.process_cnt-1,
                            cs.RES,
                            "{}[stage_id = {:2}  ]".format(
                                cs.WHI,
                                stage_id,
                            ),
                            "ds_nxt_id != Multiprocess.cat_nxt",
                        )
                    )
                    return

                Multiprocess.cat_nxt[stage_id] += 1

                #initial setup
                if ds_nxt_id == 0:
                    Multiprocess.logger.info(
                        "{}[{}{}/{:03}{}]   {}   {}".format(
                            cs.RES,
                            cs.PRGnBH,
                            process_name,
                            Multiprocess.process_cnt-1,
                            cs.RES,
                            "{}[stage_id = {:2}  ]".format(
                                cs.WHI,
                                stage_id,
                            ),
                            "{}data appended".format(cs.PRGnBH),
                        )
                    )

                    return ds_nxt

                ds_new = {}

                for i, v in ds_res.items():
                    if type(ds_nxt[i]) == list:
                        ds_new[i] = ds_res[i] + ds_nxt[i]

                    elif type(ds_nxt[i]) == DatetimeIndex:
                        ds_new[i] = ds_res[i].append(ds_nxt[i])

                    else:
                        ds_new[i] = concatenate([
                            ds_res[i],
                            ds_nxt[i]
                        ])

                Multiprocess.logger.info(
                    "{}[{}{}/{:03}{}]   {}   {}".format(
                        cs.RES,
                        cs.PRGnBH,
                        process_name,
                        Multiprocess.process_cnt-1,
                        cs.RES,
                        "{}[stage_id = {:2}  ]".format(
                            cs.WHI,
                            stage_id,
                        ),
                        "{}data appended".format(cs.PRGnBH),
                    )
                )

                return ds_new

            #status message-----------------------------------------------------
            Multiprocess.logger.debug(
                "{}[{}function       {}]   {}   {}".format(
                    cs.RES,
                    cs.CYA,
                    cs.RES,
                    "{}[Started        ]".format(cs.WHI),
                    "Multiprocess.run_subprocessed.concatenate",
                )
            )

            #-------------------------------------------------------------------
            process_result_cat    = []
            time_to_wait_if_alive = 0.1
            time_to_wait_if_none  = 0.1
            while Multiprocess.cat_nxt[stage_id] < Multiprocess.process_cnt:
                cat_nxt = Multiprocess.cat_nxt[stage_id]
                process_name_nxt = Multiprocess.process_instances[
                    cat_nxt
                ].process_name
                process_name_nxt_str = (
                    "{}[{}{}/{:03}{}]   {}".format(
                        cs.RES,
                        cs.PRGnBE,
                        process_name_nxt,
                        Multiprocess.process_cnt-1,
                        cs.RES,
                        "{}[stage_id = {:2}  ]".format(
                            cs.WHI,
                            stage_id,
                        ),
                    )
                )
                #-process that would produce the next results to be concatenated
                #-...was not started yet => continue----------------------------
                if cat_nxt > Multiprocess.process_last_started:
                    continue

                #-...was started and is still running => continue---------------
                if Multiprocess.processes[cat_nxt].is_alive():
                    time_sleep = time_to_wait_if_alive + 2
                    sleep(time_sleep)
                    if time_sleep <= 20:
                        time_to_wait_if_alive *= 2
                    elif time_sleep <= 60:
                        time_to_wait_if_alive += 10

                    if time_to_wait_if_alive > 3.2:
                        Multiprocess.logger.info(
                            "{}   {}still running".format(
                                process_name_nxt_str,
                                cs.PRGnBE,
                            )
                        )
                    continue

                #-...finished, but did not produce a result => major error------
                time_to_wait_if_alive = 0.1
                if Multiprocess.process_result[cat_nxt][stage_id] is None:
                    sleep(time_to_wait_if_none)
                    time_to_wait_if_none *= 2

                    if time_to_wait_if_none > 3.2:
                        Multiprocess.logger.warning(
                            "{}   {}no results yet!".format(
                                process_name_nxt_str,
                            )
                        )
                    elif time_to_wait_if_none > 6.4:
                        Multiprocess.logger.error("{}   no results!".format(
                            process_name_nxt_str,
                        ))
                        Multiprocess.processes_kill_all()
                        exit(-1)

                    continue

                #-...is finished and results are ready for concatenating--------
                time_to_wait_if_none = 0.1
                if Multiprocess.test_level == -1:
                    sleep(0.2)

                if True:
                    process_result_cat = ds_cat(
                        process_result_cat,
                        cat_nxt,
                        Multiprocess.process_result[cat_nxt][stage_id],
                        process_name_nxt,
                        stage_id,
                    )

                    Multiprocess.process_result[cat_nxt][stage_id] = None

            #--give thread_subprocess_manage time to return---------------------
            sleep(2)

            if thread_subprocess_manage.is_alive():
                Multiprocess.logger.warning("Exiting concat to early!")
                sleep(2)
            
            # return final results----------------------------------------------
            if stage_id == Multiprocess.stage_fin-1:
                Multiprocess.process_result_cat_fin = process_result_cat

            #status message-----------------------------------------------------
            Multiprocess.logger.debug(
                "{}[{}function       {}]   {}   {}".format(
                    cs.RES,
                    cs.CYA,
                    cs.RES,
                    "{}[Finished       ]".format(cs.WHI),
                    "Multiprocess.run_subprocessed.concatenate",
                )
            )

            return

        #status message---------------------------------------------------------
        Multiprocess.logger.debug(
            "{}[{}function       {}]   {}   {}".format(
                cs.RES,
                cs.CYA,
                cs.RES,
                "{}[Started        ]".format(cs.WHI),
                "Multiprocess.run_subprocessed",
            )
        )

        #-----------------------------------------------------------------------
        process_id    = 0
        start_allowed = True
        cat_finished  = False
        queue         = JoinableQueue()
        start_date_o  = datetime.strptime(
            Velo.start_date,
            "%m/%d/%Y"
        ).date()
        end_date_o    = datetime.strptime(
            Velo.end_date,
            "%m/%d/%Y"
        ).date()

        s_p_d = Velo.f_dates_of_id_sub_proc
        Multiprocess.process_cnt = len(s_p_d)
        Velo.process_cnt         = Multiprocess.process_cnt

        if Multiprocess.process_cnt > Multiprocess.cpu_cnt:
            Velo.logger.error(
                "Too many processes ({}) for only {} cpu's!".format(
                    Multiprocess.process_cnt,
                    Multiprocess.cpu_cnt,
                )
            )
            exit(-1)

        # setup processes/instances---------------------------------------------
        for date in range(Multiprocess.process_cnt):
            date_period = s_p_d[date][2]
            if date_period <= 0:
                continue;

            queue_evnt   = JoinableQueue()
            process      = [] 
            process_name = "process_{:03d}".format(process_id)
            process_inst = None

            if Multiprocess.test_level == 0:
                process_inst = Velo(
                    process_id=process_id,
                    process_name=process_name,
                    queue=queue,
                    queue_evnt=queue_evnt,
                    date_id=date,
                )

            if Multiprocess.test_level == -1:
                process_inst = MultiprocessTest(
                    process_id=process_id,
                    process_name=process_name,
                    queue_evnt=queue_evnt,
                    queue=queue,
                    date_id=date,
                )

            process = Process(target=process_inst.run)

            Multiprocess.process_instances.append(process_inst)
            Multiprocess.processes        .append(process)
            Multiprocess.process_result   .append([])
            Multiprocess.queue_evnt       .append(queue_evnt)

            for stage_id in range(Multiprocess.stage_fin):
                Multiprocess.process_result[-1].append(None)

            process_id += 1

        thread_subprocess_manage = Thread(
            target = subprocess_manage,
            args   = (queue,),
        )

        thread_concatenate = []

        #for stage_id in range(Multiprocess.stage_fin):
        thread_concatenate.append(
            Thread(
                target = concatenate,
                args   = (
                    #stage_id,
                    1,
                ),
            )
        )

        thread_subprocess_manage.start()

        #--concatenate all consecutive results----------------------------------
        for thread in thread_concatenate:
            thread.start()

        for thread in thread_concatenate:
            thread.join()

        # getting final results-------------------------------------------------
        process_result_cat = Multiprocess.process_result_cat_fin

        thread_subprocess_manage.join()

        #status message---------------------------------------------------------
        Multiprocess.logger.debug(
            "{}[{}function       {}]   {}   {}".format(
                cs.RES,
                cs.CYA,
                cs.RES,
                "{}[Started        ]".format(cs.WHI),
                "Multiprocess.run_subprocessed",
            )
        )

        return process_result_cat

    def run():
        """
        """
        #status message---------------------------------------------------------
        Multiprocess.logger.debug(
            "{}[{}function       {}]   {}   {}".format(
                cs.RES,
                cs.CYA,
                cs.RES,
                "{}[Started        ]".format(cs.WHI),
                "Multiprocess.run",
            )
         )
        #-----------------------------------------------------------------------
        if Multiprocess.test_level > 0:
            Multiprocess.logger.info("Starting mode: Test[Concatening]")
            Multiprocess.test_concat()
            exit(0)

        elif Multiprocess.test_level == -1:
            Multiprocess.logger.info("Starting mode: Test[Multiprocess]")
            Multiprocess.test_multiprocessing()
            exit(0)

        results_raw = Multiprocess.run_subprocessed()

        #status message---------------------------------------------------------
        Multiprocess.logger.debug(
            "{}[{}function       {}]   {}   {}".format(
                cs.RES,
                cs.CYA,
                cs.RES,
                "{}[Started        ]".format(cs.WHI),
                "Multiprocess.run",
            )
         )

        return results_raw

class MultiprocessTest:
    """
    This function provides some dummy commands in order to check the
    correct functioning of the multiprocessing of the chain.
    """
    #--class attributes---------------------------------------------------------
    logger      = None
    process_cnt = 0

    def setup(
        logger,
        process_cnt,
    ):
        MultiprocessTest.logger      = logger
        MultiprocessTest.process_cnt = process_cnt

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

        # data structures conveyed by subprocess queues-------------------------
        self.__queue_dict = {}

    def run(self):
        """
        Run the testing subprocess.
        """
        # print starting message------------------------------------------------
        process_name_str = "{}[{}{}/{:03}{}]{}  {}".format(
            cs.RES,
            cs.PRGnBA,
            self.process_name,
            MultiprocessTest.process_cnt-1,
            cs.RES,
            cs.RES,
            "[stage_id = {:2}  ]".format(self.stage_id)
        )

        # print some working message--------------------------------------------
        MultiprocessTest.logger.info(
            "{}{}  Loading transactions from [{}--{}, {}, {:03d}]".format(
                process_name_str,
                cs.WHI,
                self.__start_date,
                self.__end_date,
                self.__end_date_next,
                self.__date_period,
            )
        )

        MultiprocessTest.logger.debug(
            "process_id = {}, stage_id = {}".format(
                self.process_id,
                self.stage_id,
            )
        )

        # emulate working time randomly-----------------------------------------
        sleep(randint(2,4))
        self.stage_id = 1
        self.stage_id = 2
        # create and send testing result from process_id------------------------
        self.__queue_dict = {}
        self.__queue_dict["stage_id"]   = [self.stage_id]
        self.__queue_dict["process_id"] = [self.process_id]
        self.stage_id = 3

        # create and send testing result from process_id------------------------
        self.__queue_dict = {}
        self.__queue_dict["stage_id"]   = [self.stage_id]
        self.__queue_dict["process_id"] = [self.process_id]

        Multiprocess.logger.debug("{}{}  Sending results".format(
            process_name_str,
            cs.WHI,
        ))

        self.__queue.put([self.stage_id, self.process_id, self.__queue_dict])

        # print termination message---------------------------------------------
        Multiprocess.logger.debug("{}{}  terminating".format(
            process_name_str,
            cs.WHI,
        ))

        return

if __name__ == "__main__":
    print("{}Use this file with script.py!{}".format(cs.RED,cs.RES))
    exit(0)
