import os
import sys
import time
import argparse
import pandas as pd
import psycopg
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore', category=UserWarning)

def generate_SI_data(args):
    """
    Generate tables to calculate suspicion of infection
    :return:
    """

    conn = psycopg.connect(dbname=args.dbname,
                           user=args.sqluser,
                           password=args.sqlpass,
                           host = args.host,
                           port = args.port)

    cursor = conn.cursor()
    cursor.execute("""
    DROP SCHEMA IF EXISTS mimiciii_si CASCADE;
    CREATE SCHEMA mimiciii_si;
    SET search_path = "mimiciii";
    """)
    start = time.time()
    print("creating abx_poe_list...")
    cursor.execute(open("./SQL-SI/abx_poe_list.sql", "r").read())
    print("... done. Time taken: {} sec".format(time.time() - start))
    start = time.time()
    print("creating abx_micro_poe...")
    cursor.execute(open("./SQL-SI/abx_micro_poe.sql", "r").read())
    print("... done. Time taken: {} sec".format(time.time() - start))
    start = time.time()
    print("creating SI...")
    cursor.execute(open("./SQL-SI/SI.sql", "r").read())
    print("... done. Time taken: {} sec".format(time.time() - start))
    conn.commit()
    cursor.close()
    conn.close()

def generate_SOFA_data(args):
    """
    Generate tables to calculate suspicion of infection
    :return:
    """

    conn = psycopg.connect(dbname=args.dbname,
                           user=args.sqluser,
                           password=args.sqlpass,
                           host=args.host,
                           port=args.port)

    cursor = conn.cursor()
    cursor.execute("""
    DROP SCHEMA IF EXISTS mimiciii_sofa CASCADE;
    CREATE SCHEMA mimiciii_sofa;
    SET search_path = "mimiciii";
    """)

    # SOFA CARDIO

    start = time.time()
    print("calculating cardio contribution to SOFA...")
    cursor.execute(open("./SQL-SOFA/cardiovascular/echo.sql", "r").read())
    cursor.execute(open("./SQL-SOFA/cardiovascular/vitalsperhour.sql", "r").read())
    cursor.execute(open("./SQL-SOFA/cardiovascular/cardio_SOFA.sql", "r").read())
    conn.commit()
    print("... done. Time taken: {} sec".format(time.time() - start))

    # SOFA Central Nervous System

    start = time.time()
    print("calculating central nervous system contribution to SOFA...")
    cursor.execute(open("./SQL-SOFA/central_nervous_system/gcsperhour.sql", "r").read())
    conn.commit()
    print("... done. Time taken: {} sec".format(time.time() - start))

    # SOFA Coagulation
    start = time.time()
    print("calculating coagulation contribution to SOFA...")
    cursor.execute(open("./SQL-SOFA/coagulation/labsperhour.sql", "r").read())
    conn.commit()
    print("... done. Time taken: {} sec".format(time.time() - start))

    # SOFA liver
    start = time.time()
    print("calculating liver contribution to SOFA...")
    cursor.execute(open("./SQL-SOFA/liver/labsperhour.sql", "r").read())
    conn.commit()
    print("... done. Time taken: {} sec".format(time.time() - start))

    # SOFA renal
    start = time.time()
    print("calculating renal contribution to SOFA...")
    cursor.execute(open("./SQL-SOFA/renal/labsperhour.sql", "r").read())
    cursor.execute(open("./SQL-SOFA/renal/uoperhour.sql", "r").read())
    cursor.execute(open("./SQL-SOFA/renal/runninguo24h.sql", "r").read())
    conn.commit()
    print("... done. Time taken: {} sec".format(time.time() - start))

    # SOFA respiration
    start = time.time()
    print("calculating respiration contribution to SOFA...")
    cursor.execute(open("./SQL-SOFA/respiration/ventsettings.sql", "r").read())
    cursor.execute(open("./SQL-SOFA/respiration/ventdurations.sql", "r").read())
    cursor.execute(open("./SQL-SOFA/respiration/bloodgasfirstday.sql", "r").read())
    cursor.execute(open("./SQL-SOFA/respiration/bloodgasfirstdayarterial.sql", "r").read())
    cursor.execute(open("./SQL-SOFA/respiration/resp_SOFA.sql", "r").read())
    conn.commit()
    print("... done. Time taken: {} sec".format(time.time() - start))

    # Combining scores
    start = time.time()
    print("combining scores...")
    cursor.execute(open("./SQL-SOFA/hourly_table.sql", "r").read())
    cursor.execute(open("./SQL-SOFA/SOFA.sql", "r").read())
    conn.commit()
    print("... done. Time taken: {} sec".format(time.time() - start))

    cursor.close()
    conn.close()

def generate_sepsis_labels(args):
    """
    Generate sepsis onset time for all hadm_id
    :return:
    """

    conn = psycopg.connect(dbname=args.dbname,
                           user=args.sqluser,
                           password=args.sqlpass,
                           host=args.host,
                           port=args.port)

    cursor = conn.cursor()
    cursor.execute("""
    DROP SCHEMA IF EXISTS mimiciii_sepsislabels CASCADE;
    CREATE SCHEMA mimiciii_sepsislabels;
    SET search_path = "mimiciii";
    """)


    # sepsis within SI
    start = time.time()
    print("calculating sofa within SI...")
    cursor.execute(open("./SOFA_within_SI.sql", "r").read())
    conn.commit()
    print("... done. Time taken: {} sec".format(time.time() - start))

    start = time.time()
    print("creating sepsis onset table...")

    df = pd.read_sql("SELECT * FROM mimiciii_sepsislabels.SOFA_within_SI", conn)

    # calculate the first derivative of each SOFA score part
    #       first look for the previous value

    df["h_in_SI_window"] = df.sort_values(['hadm_id', 'sepsis_time'],
                                          ascending=[True, True]).groupby('hadm_id').cumcount() - 1

    columns = ['sofa', 'sofaresp', 'sofacoag', 'sofaliv', 'sofacardio', 'sofagcs', 'sofaren']
    #       then calculate the delta

    ####put time calculation
    for col in columns:

        df[col + "_temp"] = df[col]
        df[col + "_temp"] = df[col + "_temp"].fillna(value=0)
        df[col + "_min"] = df.sort_values(by=['hadm_id', 'sepsis_time'],
                                          ascending=[True, True]).groupby('hadm_id').cummin()[col + "_temp"]
        df[col + "_delta"] = df[col + "_temp"] - df[col + "_min"]
        df = df.drop(columns=[col + "_temp"])

    df["sepsis_onset"] = 0
    df.loc[df.sofa_delta >= 2, "sepsis_onset"] = 1

    # rank occurrences of positive sepsis onset per hadm_id by timestamp
    df.loc[df.sepsis_onset == 1, "ranked_onsets"] = df[df.sepsis_onset == 1].groupby("hadm_id").cumcount() + 1
    # filter by first occurrence
    df = df[(df.sepsis_onset == 1) & (df.ranked_onsets == 1)]

    df = df.rename(columns={"sofa_delta": "delta_score"})
    # save to postgres

    #had to use sqlalchemy because i was unable to use pandas to sql with just psycopg
    engine = create_engine("postgresql+psycopg://{0}:{1}@{2}:{3}/{4}".format(args.sqluser,
                                                                             args.sqlpass,
                                                                             args.host,
                                                                             args.port,
                                                                             args.dbname))
    df.to_sql('sepsis_onset',
              engine,
              if_exists='replace',
              schema='mimiciii_sepsislabels',
              index = False)

    conn.commit()
    print("... done. Time taken: {} sec".format(time.time() - start))

    start = time.time()
    print("calculating sofa deltas and creating tables...")
    conn.execute(open("./sofa_delta.sql", "r").read())
    conn.commit()
    print("... done. Time taken: {} sec".format(time.time() - start))

    cursor.close()
    conn.close()



def parse_arg():
    parser = argparse.ArgumentParser()
    parser.add_argument("-u","--sqluser",
                        help="SQL user",
                        default="postgres")
    parser.add_argument("-pw", "--sqlpass",
                        help="SQL user password. If none insert ''",
                        default="postgres")
    parser.add_argument("-host", "--host",
                        help="SQL host",
                        default="localhost")
    parser.add_argument("-db", "--dbname",
                        help="SQL database name",
                        default="mimic")
    parser.add_argument("-p", "--port",
                        help="SQL port number",
                        default="5432")
    return parser.parse_args()

def main(args):

    generate_SI_data(args)
    generate_SOFA_data(args)
    generate_sepsis_labels(args)



if __name__ == '__main__':
    args = parse_arg()
    main(args)