#!/usr/bin/python3
import os
import psycopg2
import time
from configparser import ConfigParser
from datetime import date, timedelta
from itertools import starmap


DRYRUN = bool(False if os.getenv('DRYRUN') == 'false' else True) # empty or any value except 'false' mean True. You must be deliberately enable this option to work.
DEBUG = bool(os.getenv('DEBUG') or False) # any nonempty env

config = ConfigParser()
config.read('config.ini')
# read config as globals:
DBHOST = os.getenv('DBHOST') or config.get('main', 'DBHOST')
DBPORT = int(os.getenv('DBPORT') or config.get('main', 'DBPORT'))
DBUSER = os.getenv('DBUSER') or config.get('main', 'DBUSER')
DBPASS = os.getenv('DBPASS') or config.get('main', 'DBPASS')
BATCHSIZE = int(os.getenv('BATCHSIZE') or config.get('main', 'BATCHSIZE'))
DEFAULT_HORIZON = config.get('main', 'DEFAULT_HORIZON')

def generate_clean_query(tablename, pk, ts_field, ts_in_millisec=False, special_horizon=False):
    pk_cte_resultset = ', '.join(pk)
    pk_wherejoin_tab = ', '.join(f'{tablename}.{f}' for f in pk)
    pk_wherejoin_cte = ', '.join(f'cte.{f}' for f in pk)
    millisec_factor = '* 1000' if ts_in_millisec else ''
    if special_horizon:  # special_horizon mean existence special horizon table for conveyor_id
        query = f""" WITH cte AS (
            select {pk_cte_resultset}
                FROM  {tablename} 
                left join  horizon h on h.conveyor_id = {tablename}.conveyor_id
                WHERE 
                      (h.conveyor_id = {tablename}.conveyor_id  and {ts_field} < trunc(date_part('epoch', now() - h.horizon_days * interval '1 day')){millisec_factor})
                   OR (h.conveyor_id is null and {ts_field} <  trunc(date_part('epoch', now() - {DEFAULT_HORIZON} * interval '1 day')){millisec_factor})
                LIMIT  {BATCHSIZE}
            )
            DELETE FROM {tablename}
            USING  cte
            WHERE ( {pk_wherejoin_tab} ) = ( {pk_wherejoin_cte} );"""
    else:
        query = f"""WITH cte AS (
     SELECT {pk_cte_resultset}
      FROM {tablename}
     WHERE  {ts_field} <  trunc(date_part('epoch', now() - {DEFAULT_HORIZON} * interval '1 day')) {millisec_factor}
      LIMIT  {BATCHSIZE}
      FOR  UPDATE )
     DELETE FROM {tablename}
      USING  cte
      WHERE ( {pk_wherejoin_tab} ) = ( {pk_wherejoin_cte} );"""
    if DEBUG or DRYRUN:
        msg = "\n# DRY RUN MODE. " if DRYRUN else ""
        msg = msg + f"for {tablename} generated SQL:\n {query}"
        print(msg)
    if DRYRUN:
        time.sleep(0.2)
    return query


def clean_cp(dbname):
    conn = psycopg2.connect(
        host=DBHOST,
        port=DBPORT,
        database=dbname,  # for every db
        user=DBUSER,
        password=DBPASS)
    cur = conn.cursor()

    # horizon_list
    custom_horizon_list = starmap(lambda conv_id, days: (int(conv_id), int(days)), config.items('custom_horizon'))
    if DEBUG:
        print('ENABLED custom_horizon_list:')
        for conv_id, days in custom_horizon_list:
            custom_horizon_date = (date.today() - timedelta(days=60)).strftime('%F')
            print(f'{conv_id} = {days} days (before {custom_horizon_date})')
            
    
    if not DRYRUN:
        cur.execute(f'create temporary table horizon (conveyor_id integer, horizon_days integer);')
        for params in custom_horizon_list:
            cur.execute('insert into horizon values(%s, %s);', params)
            conn.commit()

    cp_tables = [
        # do not forget add coma in single-field pk tuple: ('id',)
        {'tablename': 'tasks_archive',   'pk': ('id',), 'ts_field': 'create_time', 'ts_in_millisec': False, 'special_horizon': True},
        {'tablename': 'stream_counters', 'pk': ('id',), 'ts_field': 'ts',          'ts_in_millisec': False, 'special_horizon': True},
        {'tablename': 'tasks_history',   'pk': ('id',), 'ts_field': 'create_time', 'ts_in_millisec': True,  'special_horizon': True}
    ]

    for job in cp_tables:
        tablename = job['tablename']
        pk = job['pk']
        ts_field = job['ts_field']
        ts_in_millisec = job.get('ts_in_millisec', False)  # in case millisec factor *1000
        special_horizon = job.get('special_horizon', False)
        query = generate_clean_query(tablename=tablename, pk=pk, ts_field=ts_field, ts_in_millisec=ts_in_millisec,
                                     special_horizon=special_horizon)
        total_rc = 0
        if not DRYRUN:
            while True:
                cur.execute(query)
                conn.commit()
                total_rc += cur.rowcount
                if cur.rowcount < BATCHSIZE:
                    # done 
                    print(f"{dbname} {tablename} deleted: {total_rc} rows", flush=True)
                    break
                elif DEBUG:
                    print(f"DEBUG {dbname} {tablename} deleted: {cur.rowcount} rows, total {total_rc}", flush=True)
        # db finished
    conn.close()


def general_clean_table(dbname, tablename, pk, ts_field, ts_in_millisec=False, special_horizon=False):
    # tables = [ {'dbname': 'conveyor_statistics','tablename':'conveyor_copy_rpc_logic_statistics', 'pk': ('from_conveyor_id','to_conveyor_id','ts','from_node_id'), 'ts_field': 'ts', 'ts_in_millisec': False, 'special_horizon': False},]
    conn = psycopg2.connect(
        host=DBHOST,
        port=DBPORT,
        database=dbname,
        user=DBUSER,
        password=DBPASS)
    cur = conn.cursor()
    query = generate_clean_query(tablename=tablename, pk=pk, ts_field=ts_field, ts_in_millisec=ts_in_millisec,
                                 special_horizon=special_horizon)
    total_rc = 0
    if not DRYRUN:
        while True:
            cur.execute(query)
            conn.commit()
            total_rc += cur.rowcount
            if cur.rowcount < BATCHSIZE:
                # done 
                print(f"{dbname} {tablename} deleted: {total_rc} rows", flush=True)
                break
            elif DEBUG:
                print(f"DEBUG {dbname} {tablename} deleted: {cur.rowcount} rows, total {total_rc}", flush=True)
        # db finished
    conn.close()


if __name__ == '__main__':
    cp_count = int(config.get('main', 'CP_COUNT'))
    for db in [f"cp{i}" for i in range(cp_count)]:
        clean_cp(db)

    tables = [
        {'dbname': 'conveyor_statistics', 'tablename': 'conveyor_copy_rpc_logic_statistics', 'pk': ('from_conveyor_id', 'to_conveyor_id', 'ts', 'from_node_id'), 'ts_field': 'ts', 'ts_in_millisec': False, 'special_horizon': False},
        {'dbname': 'conveyor_statistics', 'tablename': 'conveyor_logic_statistics',          'pk': ('conveyor_id', 'node_id', 'ts'),                             'ts_field': 'ts', 'ts_in_millisec': False, 'special_horizon': False},
        {'dbname': 'conveyor',            'tablename': 'conveyor_billing',                   'pk': ('conveyor_id', 'user_id', 'ts'),                             'ts_field': 'ts', 'ts_in_millisec': False, 'special_horizon': False},
        {'dbname': 'conveyor',            'tablename': 'cce_exec_time',                      'pk': ('conveyor_id', 'node_id', 'ts'),                             'ts_field': 'ts', 'ts_in_millisec': False, 'special_horizon': False},
        {'dbname': 'conveyor',            'tablename': 'conveyor_called_timers',             'pk': ('conveyor_id', 'node_id', 'ts'),                             'ts_field': 'ts', 'ts_in_millisec': False, 'special_horizon': False},
    ]
    for table in tables:
        general_clean_table(**table)
        
    if DRYRUN:
        print('='*80)
        print('Clean ran in DRYRUN-mode. You can disable dryrun mode via passing env variable DRYRUN="false"')

