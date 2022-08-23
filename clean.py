#!/usr/bin/python3
import os
import psycopg2

DBHOST = os.getenv('DBHOST') or '127.0.0.1'
DBPORT = os.getenv('DBPORT') or '5432'
DBUSER = os.getenv('DBUSER') or 'postgres'
DBPASS = os.getenv('DBPASS') or 'password'
default_horizon = 21
BATCHSIZE = int(os.getenv('BATCHSIZE') or '10000')

custom_horizon = """
# converyer_id, horizon -  keep horison in days.
# you can пока добавлять прямо сюда. пустьіе строки и строки с решеткой считаются за комментарий и не обрабатьіваются. 
# TODO - хранить на стороне, в бд например или во внешнем файле. и можно написать нормальньій парсер.
123,  21
456,  7
789,  14
"""


def clean_cp(dbname):
    conn = psycopg2.connect(
        host=DBHOST,
        port=int(DBPORT),
        database=dbname, # for every db
        user=DBUSER,
        password=DBPASS)
    cur = conn.cursor()

    # horizon_list
    cur.execute(f'create temporary table horizon (conveyor_id integer, horizon_days integer);')
    custom_horizon_list = [tuple(map(int,line.split(','))) for line in custom_horizon.splitlines() if line and '#' not in line ]
    for params in custom_horizon_list:
        cur.execute('insert into horizon values(%s, %s);', params)
    conn.commit()

    total_rc=0
    # tasks_archive
    while True:
        cur.execute(f"""
        WITH cte AS (
            select ta.id                                  -- your PK
                FROM  tasks_archive ta
                left join  horizon h on h.conveyor_id = ta.conveyor_id
                WHERE 
                      (h.conveyor_id = ta.conveyor_id  and  create_time <  trunc(date_part('epoch', now() - h.horizon_days * interval '1 day')))
                   OR (h.conveyor_id is null and create_time <  trunc(date_part('epoch', now() - {default_horizon} * interval '1 day')))
                LIMIT  {BATCHSIZE}
            )
            DELETE FROM tasks_archive
            USING  cte
            WHERE  tasks_archive.id = cte.id;
        """);
        conn.commit()
        total_rc += cur.rowcount
        if cur.rowcount < BATCHSIZE:
            # done 
            print (f"{dbname} tasks_archive deleted: {total_rc} rows", flush=True)
            break
        else:
            if DEBUG:
                print (f"DEBUG {dbname} tasks_archive deleted: {cur.rowcount} rows, total {total_rc}", flush=True)

            
    # stream_counters
    while True:
        cur.execute(f"""
        WITH cte AS (
            select sc.id                                  -- your PK
                FROM  stream_counters sc
                left join  horizon h on h.conveyor_id = sc.conveyor_id
                WHERE 
                      (h.conveyor_id = sc.conveyor_id  and  ts <  trunc(date_part('epoch', now() - h.horizon_days * interval '1 day')))
                   OR (h.conveyor_id is null and ts <  trunc(date_part('epoch', now() - {default_horizon} * interval '1 day')))
                LIMIT  {BATCHSIZE}
            )
            DELETE FROM stream_counters
            USING  cte
            WHERE  stream_counters.id = cte.id;
        """);
        conn.commit()
        total_rc += cur.rowcount
        if cur.rowcount < BATCHSIZE:
            # done 
            print (f"{dbname} stream_counters deleted: {total_rc} rows", flush=True)
            break
        else:
            if DEBUG:
                print (f"DEBUG {dbname} stream_counters deleted: {cur.rowcount} rows, total {total_rc}", flush=True)


    # tasks_history
    while True:
        cur.execute(f"""
        WITH cte AS (
            select th.id                                  -- your PK
                FROM  tasks_history th
                left join  horizon h on h.conveyor_id = th.conveyor_id
                WHERE 
                      (h.conveyor_id = th.conveyor_id  and  create_time <  trunc(date_part('epoch', now() - h.horizon_days * interval '1 day')) * 1000 )
                   OR (h.conveyor_id is null and create_time <  trunc(date_part('epoch', now() - {default_horizon} * interval '1 day')) * 1000 )
                LIMIT  {BATCHSIZE}
            )
            DELETE FROM tasks_history
            USING  cte
            WHERE  tasks_history.id = cte.id;
        """);
        conn.commit()
        total_rc += cur.rowcount
        if cur.rowcount < BATCHSIZE:
            # done 
            print (f"{dbname} tasks_history deleted: {total_rc} rows", flush=True)
            break
        else:
            if DEBUG:
                print (f"DEBUG {dbname} tasks_history deleted: {cur.rowcount} rows, total {total_rc}", flush=True)
    conn.close()


def clean_conveyor_statistics():
    dbname = 'conveyor_statistics'
    conn = psycopg2.connect(
        host=DBHOST,
        port=int(DBPORT),
        database=dbname,
        user=DBUSER,
        password=DBPASS)
    cur = conn.cursor()

    # conveyor_copy_rpc_logic_statistics
    total_rc = 0
    while True:
        cur.execute(f"""WITH cte AS (
         SELECT from_conveyor_id,to_conveyor_id,ts,from_node_id  -- your PK
         FROM conveyor_copy_rpc_logic_statistics
         WHERE  ts <  trunc(date_part('epoch', now() - {default_horizon} * interval '1 day'))
         LIMIT  {BATCHSIZE}
         FOR  UPDATE
         )
          DELETE FROM conveyor_copy_rpc_logic_statistics    
          USING  cte
          WHERE ( conveyor_copy_rpc_logic_statistics.from_conveyor_id,
                  conveyor_copy_rpc_logic_statistics.to_conveyor_id,
                  conveyor_copy_rpc_logic_statistics.ts,
                  conveyor_copy_rpc_logic_statistics.from_node_id )
                 = (cte.from_conveyor_id, cte.to_conveyor_id, cte.ts, cte.from_node_id);""");
        conn.commit()
        total_rc += cur.rowcount
        if cur.rowcount < BATCHSIZE:
            # done.
            print (f"{dbname} conveyor_copy_rpc_logic_statistics deleted: {total_rc} rows", flush=True)
            break
        else:
            if DEBUG:
                print (f"DEBUG {dbname} conveyor_copy_rpc_logic_statistics deleted: {cur.rowcount} rows, total {total_rc}", flush=True)

    # conveyor_logic_statistics
    total_rc = 0
    while True:
        cur.execute(f"""WITH cte AS (
           SELECT conveyor_id, node_id, ts     -- your PK
           FROM   conveyor_logic_statistics
           WHERE  ts <  trunc(date_part('epoch', now() - {default_horizon} * interval '1 day'))
           LIMIT  {BATCHSIZE}
           FOR    UPDATE
           )
        DELETE FROM conveyor_logic_statistics
        USING  cte
        WHERE ( conveyor_logic_statistics.conveyor_id,
                conveyor_logic_statistics.node_id,
                conveyor_logic_statistics.ts )
            = (cte.conveyor_id, cte.node_id, cte.ts);""");
        conn.commit()
        total_rc += cur.rowcount
        if cur.rowcount < BATCHSIZE:
            # done.
            print (f"{dbname} conveyor_logic_statistics deleted: {total_rc} rows", flush=True)
            break
        else:
            if DEBUG:
                print (f"DEBUG {dbname} conveyor_logic_statistics deleted: {cur.rowcount} rows, total {total_rc}", flush=True)



def clean_conveyor():
    dbname = 'conveyor'
    conn = psycopg2.connect(
        host=DBHOST,
        port=int(DBPORT),
        database=dbname,
        user=DBUSER,
        password=DBPASS)
    cur = conn.cursor()

    # conveyor_billing
    total_rc = 0
    while True:
        cur.execute(f"""WITH cte AS (
         SELECT conveyor_id,user_id,ts  -- your PK
         FROM conveyor_billing
         WHERE  ts <  trunc(date_part('epoch', now() - {default_horizon} * interval '1 day'))
         LIMIT  {BATCHSIZE}
         FOR  UPDATE
         )
          DELETE FROM conveyor_billing
          USING  cte
          WHERE ( conveyor_billing.conveyor_id,
                  conveyor_billing.user_id,
                  conveyor_billing.ts)
                 = (cte.conveyor_id, cte.user_id, cte.ts);""")
        conn.commit()
        total_rc += cur.rowcount
        if cur.rowcount < BATCHSIZE:
            # done.
            print (f"{dbname} conveyor_billing deleted: {total_rc} rows, total {total_rc}", flush=True)
            break
        else:
            if DEBUG:
                print (f"DEBUG {dbname} conveyor_billing deleted: {cur.rowcount} rows", flush=True)

    # cce_exec_time
    total_rc = 0
    while True:
        cur.execute(f"""WITH cte AS (
         SELECT conveyor_id,node_id,ts  -- your PK
         FROM cce_exec_time
         WHERE  ts <  trunc(date_part('epoch', now() - {default_horizon} * interval '1 day'))
         LIMIT  {BATCHSIZE}
         FOR  UPDATE
         )
          DELETE FROM cce_exec_time
          USING  cte
          WHERE ( cce_exec_time.conveyor_id,
                  cce_exec_time.node_id,
                  cce_exec_time.ts)
                 = (cte.conveyor_id, cte.node_id, cte.ts);""");
        conn.commit()
        total_rc += cur.rowcount
        if cur.rowcount < BATCHSIZE:
            # done.
            print (f"{dbname} cce_exec_time deleted: {total_rc} rows", flush=True)
            break
        else:
            if DEBUG:
                print (f"DEBUG {dbname} cce_exec_time deleted: {cur.rowcount} rows, total {total_rc}", flush=True)

    # conveyor_called_timers
    total_rc = 0
    while True:
        cur.execute(f"""WITH cte AS (
         SELECT conveyor_id,node_id,ts  -- your PK
         FROM conveyor_called_timers
         WHERE  ts <  trunc(date_part('epoch', now() - {default_horizon} * interval '1 day'))
         LIMIT  {BATCHSIZE}
         FOR  UPDATE
         )
          DELETE FROM conveyor_called_timers
          USING  cte
          WHERE ( conveyor_called_timers.conveyor_id,
                  conveyor_called_timers.node_id,
                  conveyor_called_timers.ts)
                 = (cte.conveyor_id, cte.node_id, cte.ts);""");
        conn.commit()
        total_rc += cur.rowcount
        if cur.rowcount < BATCHSIZE:
            # done.
            print (f"{dbname} conveyor_called_timers deleted: {total_rc} rows", flush=True)
            break
        else:
            if DEBUG:
                print (f"DEBUG {dbname} conveyor_called_timers deleted: {cur.rowcount} rows, total {total_rc}", flush=True)


if __name__ == '__main__':
    for db in [f"cp{i}" for i in range(10)]:
        clean_cp(db)
    clean_conveyor_statistics()
    clean_conveyor()
