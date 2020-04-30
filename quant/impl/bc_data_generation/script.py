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
    Multiprocess.logger = logger

    setup_output_path(args.path_data_output)

    #--setup: main objects for using BlockSci-----------------------------------
    Velo.setup(
        logger=logger,
        args=args,
    )

    #--Retrieval of basic blockchain data, money supply and velocity measures---
    results_raw = Multiprocess.get_data_for_df(
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
        ress = results_raw["process_id"]
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

    #--get csv of final pandas data frame---------------------------------------
    Velo.get_results_finalized(
        results_raw=results_raw,
        index_label="date"
    )

    print("Exiting program")
    exit(0)

if __name__ == "__main__":
    main()
