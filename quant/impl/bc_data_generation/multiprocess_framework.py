import time
import random
import logging
import threading
import hashlib
import multiprocessing
import numpy as np
from multiprocessing import Process, Queue, JoinableQueue
from velo            import Velo
from datetime        import date
from datetime        import datetime
from datetime        import timedelta
from pandas          import DatetimeIndex
from math            import ceil
from colorstrings    import colorStrings as cs

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

    # class attribute for concatenation
    cat_nxt = 0

    #subprocessing related
    processes            = []
    process_last_started = -1
    process_cnt          = 0 #to print max number of started subprocesses


    #==[ CLASSLEVEL ]===========================================================
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

        for i in range(0,len(Multiprocess.processes)):
            processes_kill(i)
        return

    def get_data_for_df(
        start_date,
        end_date,
        period,
        test,
        log_level,
        cpu_cnt_manual,
        path_data_output = "",
    ):
        """
        This function retrieves required data in a multiprocessed fashion.
        """
        def date_range(
            start_date,
            end_date,
            period,
        ):
            """
            for further explanation, see
            https://stackoverflow.com/questions/1060279/
            iterating-through-a-range-of-dates-in-python
            """
            yield_date_start  = start_date
            yield_date_end    = start_date
            yield_date_period = 0

            date_range = int((end_date - start_date).days/period + 1)
            for n in range(date_range):
                #print ("n =", n)
                if n > 0:
                    yield_date_start = yield_date_end + timedelta(1)

                yield_date_end = start_date + timedelta((n+1)*period)
                if yield_date_end > end_date:
                    yield_date_end = end_date

                yield_date_period = (yield_date_end - yield_date_start).days + 1

                yield yield_date_start, yield_date_end, yield_date_period

        def concat_test(
            test,
            path_data_output,
        ):
            """
            This function provides means to test the correct functioning of the
            concatenation of the subprocess results.
            """
            def get_data_for_df_test(
                start_date,
                end_date,
                period,
                test,
            ):
                """
                """
                process_id         = 0
                process_cnt        = 0
                velo_instances     = []
                velo_instances_ret = []
                queue              = JoinableQueue()
                date_format        = "%m/%d/%Y"
                for i in range(3):
                    end_date_o = datetime.strptime(
                        end_date[i],
                        date_format,
                    ).date()
                    start_date_o = datetime.strptime(
                        start_date[i],
                        date_format,
                    ).date()

                    process_name = "process_{:03d}".format(process_id)
                    velo_inst = Velo(
                        process_id=process_id,
                        process_name=process_name,
                        queue=queue,
                        date_id=date,
                    )

                    process = Process(target=velo_inst.run)

                    process_id += 1
                    process_cnt += 1
                    velo_instances.append(velo_inst)
                    Multiprocess.processes.append(process)

                for i in range(process_cnt):
                    Multiprocess.processes[i].start()

                for i in range(process_cnt):
                    msg_process_id = None
                    msg_from_queue = ""
                    while True:
                        msg_from_queue = queue.get()
                        msg_process_id = msg_from_queue[0]
                        velo_instances_ret.append(msg_from_queue[1])
                        queue.task_done()
                        break

                    Multiprocess.processes[msg_process_id].join()

                return velo_instances_ret

            def ds_cmp(
                test_number,
                test_name,
                ds_a_a,
                ds_a_b,
                ds_b,
                path_data_output,
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
                    elif isinstance(ds, type(np.ndarray)):
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
                    path_data_output,
                ):
                    """
                    This function can be applied in order to export a given data
                    structure to a textfile.
                    """
                    path_data_output_l =  "{}_ds/ds_{}_{}.txt".format(
                        path_data_output,
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
                    hash_a = hashlib.md5()
                    hash_b = hashlib.md5()

                    hash_a.update(str.encode(s_ds_a))
                    hash_b.update(str.encode(s_ds_b))

                    s_hash_a = hash_a.hexdigest()
                    s_hash_b = hash_b.hexdigest()

                    if s_hash_a == s_hash_b:
                        return True

                    return False

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
                    ds_a = np.concatenate( [ ds_a_a, ds_a_b ])

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
                ds_export( s_ds_a_a, "_a_a", test_number, path_data_output)
                ds_export( s_ds_a_b, "_a_b", test_number, path_data_output)
                ds_export( s_ds_a,   "_a",   test_number, path_data_output)
                ds_export( s_ds_b,   "_b",   test_number, path_data_output)

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

                Multiprocess.logger.debug(ret_str)
                return

            Multiprocess.logger.info("Starting mode: Test[Concatenation]")
            #start_date_a_a = "01/01/2010"
            #end_date_a_a   = "02/01/2011"
            #period_a_a     = 397

            #start_date_a_b = "02/02/2011"
            #end_date_a_b   = "03/01/2012"
            #period_a_b     = 394

            #start_date_b   = "01/01/2010"
            #end_date_b     = "03/01/2012"

            start_date_a_a = "01/01/2010"
            end_date_a_a   = "02/01/2010"
            period_a_a     = 32

            start_date_a_b = "02/02/2010"
            end_date_a_b   = "03/01/2010"
            period_a_b     = 28

            start_date_b   = "01/01/2010"
            end_date_b     = "03/01/2010"

            ret = get_data_for_df_test(
                start_date=[start_date_a_a, start_date_a_b, start_date_b],
                end_date=[end_date_a_a, end_date_a_b, end_date_b],
                period=[period_a_a, period_a_b, period_a_a + period_a_b],
                test=test,
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
                    path_data_output,
                ))

                processes_test.append(process)

            for i in range(ret_cnt):
                processes_test[i].start()

            for i in range(ret_cnt):
                processes_test[i].join()

            return

        def multiprocess_test(
            process_id,
            process_name,
            queue,
            date_id,
        ):
            """
            This function provides some dummy commands in order to check the
            correct functioning of the multiprocessing of the chain.
            """
            s_p_d = Velo.sub_proc_dates
            date_period_start        = s_p_d[date_id][0]
            date_period_end          = s_p_d[date_id][1]
            date_period              = s_p_d[date_id][2]
            date_period_start        = date_period_start
            date_period_end          = date_period_end
            date_period_end_next_day = date_period_end + timedelta(days=1)
            date_period              = date_period
            start_date               = date_period_start.strftime("%m/%d/%Y")
            end_date                 = date_period_end.strftime("%m/%d/%Y")
            end_date_next            = date_period_end_next_day.strftime(
                "%m/%d/%Y"
            )

            ret = {}

            process_name_str = "{}[{}{}/{:03}{}]{}".format(
                cs.RES,
                cs.PRGnBA,
                process_name,
                Multiprocess.process_cnt-1,
                cs.RES,
                cs.RES,

            )
            Multiprocess.logger.info(
                "{}{}  Loading transactions from [{}--{}, {}, {:03d}]".format(
                    process_name_str,
                    cs.WHI,
                    start_date,
                    end_date,
                    end_date_next,
                    date_period,
                )
            )
            time.sleep(random.randint(2,4))

            ret["process_id"] = [process_id]

            Multiprocess.logger.debug("{}{}  Sending results".format(
                process_name_str,
                cs.WHI,
            ))

            queue.put([process_id, ret])
            queue.close()

            Multiprocess.logger.debug("{}{}  terminating".format(
                process_name_str,
                cs.WHI,
            ))
            exit(0)

        def subprocess_manage(
            process_cnt,
            processes,
            cpu_cnt,
            process_result,
            queue,
        ):
            """
            This functions works as a multiprocess pool sublementary.
            """
            process_id    = 0
            start_allowed = True
            processes_fin = process_cnt

            #Start first cpu_cnt subprocesses
            if cpu_cnt > process_cnt:
                cpu_cnt = process_cnt
            for i in range(cpu_cnt):
                processes[i].start()
                Multiprocess.process_last_started += 1

                if not processes[i].is_alive():
                    Multiprocess.logger.error(
                        "{}[{}process_{:03}/{:03}{}]  Not running".format(
                            cs.RES,
                            cs.PRGnBA,
                            i,
                            process_cnt-1,
                            cs.RES,
                        )
                    )
                else:
                    Multiprocess.logger.debug(
                        "{}[{}process_{:03}/{:03}{}]  Starting".format(
                            cs.RES,
                            cs.PRGnBA,
                            i,
                            process_cnt-1,
                            cs.RES,
                        )
                    )

            Multiprocess.logger.debug("{}[{}process_{:03}{}-{}{:03}{}]  Started".format(
                cs.RES,
                cs.PRGnBA,
                0,
                cs.RES,
                cs.PRGnBA,
                cpu_cnt-1,
                cs.RES,
            ))


            process_id_str = "{}[{}process_{:03}/{:03}{}]".format(
                cs.RES,
                cs.PRGnBG,
                process_id,
                process_cnt-1,
                cs.RES,
            )

            #start next subprocess if the last one finished and its results
            #were retrieved
            while processes_fin > 0:

                if process_id < cpu_cnt - 1:
                    process_id = cpu_cnt - 1
                    continue

                #retrieve result from queue
                while True:
                    process_xxx_str = "{}[{}process_xxx/{:03}{}]".format(
                        cs.RES,
                        cs.PRGnBG,
                        process_cnt-1,
                        cs.RES,
                    )
                    Multiprocess.logger.info("{}{}  retrieving results".format(
                        process_xxx_str,
                        cs.PRGnBG,
                    ))

                    msg_from_queue = queue.get()
                    msg_process_id = msg_from_queue[0]
                    msg_process_id_str = "{}[{}process_{:03}/{:03}{}]".format(
                        cs.RES,
                        cs.PRGnBG,
                        msg_process_id,
                        process_cnt-1,
                        cs.RES,
                    )

                    process_result[msg_process_id] = msg_from_queue[1]
                    Multiprocess.logger.info("{}{}  results retrieved".format(
                        msg_process_id_str,
                        cs.PRGnBG,
                    ))
                    queue.task_done()

                    #processes[msg_process_id].terminate()
                    processes[msg_process_id].join()
                    #processes[msg_process_id].terminate()
                    Multiprocess.logger.info("{}{}  terminated/joined".format(
                        msg_process_id_str,
                        cs.PRGnBF,
                    ))
                    break

                processes_fin -= 1

                if process_id < process_cnt-1:
                    process_id += 1
                    process_tmp = processes[process_id]

                    process_tmp.start()
                    Multiprocess.process_last_started += 1

                    if not process_tmp.is_alive():
                        Multiprocess.logger.error("{}  Not running".format(
                            process_id_str
                        ))
                    else:
                        Multiprocess.logger.debug("{}  Starting".format(
                            process_id_str
                        ))

            Multiprocess.logger.debug("Returning from subprocess_manage()")
            return

        def ds_cat(
            ds_res,
            ds_nxt_id,
            ds_nxt,
            process_name
        ):
            """
            This function concatenates two given data structures.
            """
            if ds_nxt_id != Multiprocess.cat_nxt:
                Multiprocess.logger.error(
                    "{}[{}{}/{:03}{}]{}  ds_nxt_id != Multiprocess.cat_nxt".format(
                        cs.RES,
                        cs.PRGnBE,
                        process_name,
                        process_cnt-1,
                        cs.RES,
                        cs.PRGnBE,
                    )
                )
                return

            Multiprocess.cat_nxt += 1

            #initial setup
            if ds_nxt_id == 0:
                Multiprocess.logger.info("{}[{}{}/{:03}{}]{}  data appended".format(
                    cs.RES,
                    cs.PRGnBH,
                    process_name,
                    Multiprocess.process_cnt-1,
                    cs.RES,
                    cs.PRGnBH,
                ))
                return ds_nxt

            ds_new = {}

            for i, v in ds_res.items():

                if type(ds_nxt[i]) == list:
                    ds_new[i] = ds_res[i] + ds_nxt[i]

                elif type(ds_nxt[i]) == DatetimeIndex:
                    ds_new[i] = ds_res[i].append(ds_nxt[i])

                else:
                    ds_new[i] = np.concatenate([
                        ds_res[i],
                        ds_nxt[i]
                    ])

            Multiprocess.logger.info("{}[{}{}/{:03}{}]{}  data appended".format(
                cs.RES,
                cs.PRGnBH,
                process_name,
                Multiprocess.process_cnt-1,
                cs.RES,
                cs.PRGnBH,
            ))

            return ds_new

        if test > 0:
            concat_test(
                test,
                path_data_output,
            )
            return False

        velo_instances     = []
        process_result     = []
        process_result_cat = []
        process_id         = 0
        process_cnt        = 0
        cpu_cnt            = multiprocessing.cpu_count()
        cpu_cnt_test       = 16
        start_allowed      = True
        cat_finished       = False
        queue              = JoinableQueue()
        start_date_o       = datetime.strptime(start_date, "%m/%d/%Y").date()
        end_date_o         = datetime.strptime(end_date, "%m/%d/%Y").date()

        #Set cpu count manually for debugging
        if cpu_cnt_manual >= 0:
            cpu_cnt      = cpu_cnt_manual
            cpu_cnt_test = cpu_cnt_manual

        if test == -1:
            Multiprocess.logger.info("Starting mode: Test[Multiprocess]")
            cpu_cnt = cpu_cnt_test
        else:
            Multiprocess.logger.info("Starting mode: Production")

        #check if period is so high that less than cpu_cnt cores would be used
        num_days = (end_date_o - start_date_o).days
        period_max = ceil(num_days/ cpu_cnt)
        if period > period_max:
            period = period_max

        Multiprocess.logger.debug(
            "Determine velocity based on blocksci with period = {} days".format(
                period
            )
        )

        # for date_period_start, date_period_end, date_period in date_range(
        #     start_date_o,
        #     end_date_o,
        #     period
        # ):
        s_p_d = Velo.sub_proc_dates
        for date in range(len(s_p_d)):
            date_period = s_p_d[date][2]
            if date_period <= 0:
                continue;

            process_name = "process_{:03d}".format(process_id)

            velo_inst = Velo(
                process_id=process_id,
                process_name=process_name,
                queue=queue,
                date_id=date,
            )

            process = None
            if   test ==  0:
                process = Process(target=velo_inst.run)

            elif test == -1:
                process = Process(target=multiprocess_test, args = (
                    process_id,
                    process_name,
                    queue,
                    date,
                ))

            process_id  += 1
            process_cnt += 1
            velo_instances.append(velo_inst)
            Multiprocess.processes.append(process)
            process_result.append(None)

        Multiprocess.process_cnt = process_cnt
        Velo.process_cnt         = process_cnt

        thread_subprocess_manage = threading.Thread(
            target = subprocess_manage,
            args = (
                process_cnt,
                Multiprocess.processes,
                cpu_cnt,
                process_result,
                queue,
            ),
        )
        thread_subprocess_manage.start()

        #concatenate all consecutive results

        time_to_wait_is_alive = 0.1
        time_to_wait_is_none  = 0.1
        while Multiprocess.cat_nxt < process_cnt:
            cat_nxt = Multiprocess.cat_nxt
            process_name_nxt     = velo_instances[cat_nxt].process_name
            process_name_nxt_str = "{}[{}{}/{:03}{}]".format(
                cs.RES,
                cs.PRGnBE,
                process_name_nxt,
                process_cnt-1,
                cs.RES,
            )
            #process that would produced the next results to be concatenated ...
            #...was not started yet => continue
            if cat_nxt > Multiprocess.process_last_started:
                continue

            #...was started and is still running => continue
            if Multiprocess.processes[cat_nxt].is_alive():
                time_sleep = time_to_wait_is_alive + 2
                time.sleep(time_sleep)
                if time_sleep <= 20:
                    time_to_wait_is_alive *= 2
                elif time_sleep <= 60:
                    time_to_wait_is_alive += 10

                if time_to_wait_is_alive > 3.2:
                    Multiprocess.logger.info("{}{}  still running".format(
                        process_name_nxt_str,
                        cs.PRGnBE,
                    ))
                continue
            time_to_wait_is_alive = 0.1

            #...finished, but did not produce a result => Thats an major error

            if process_result[cat_nxt] is None:
                time.sleep(time_to_wait_is_none)
                time_to_wait_is_none *= 2

                if time_to_wait_is_none > 3.2:
                    Multiprocess.logger.warning("{}  no results yet!".format(
                        process_name_nxt_str,
                    ))
                elif time_to_wait_is_none > 6.4:
                    Multiprocess.logger.critical("{}  no results!".format(
                        process_name_nxt_str,
                    ))
                    processes_kill_all()
                    exit(-1)
                continue
            time_to_wait_is_none = 0.1

            # concatenate
            if test == -1:
                time.sleep(0.2)
            process_result_cat = ds_cat(
                process_result_cat,
                cat_nxt,
                process_result[cat_nxt],
                process_name_nxt,
            )

            process_result[cat_nxt] = None
            velo_instances[cat_nxt] = None

        #give thread_subprocess_manage time to return
        time.sleep(2)

        if thread_subprocess_manage.is_alive():
            Multiprocess.logger.warning("Exiting concat while to early!")

        thread_subprocess_manage.join()

        return process_result_cat
