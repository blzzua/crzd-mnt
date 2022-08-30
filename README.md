
# crzd-mnt
## setup and running
requires the python3-psycopg2 library.
setting via editing the clean.py file, or pass the ENV for connect db: DBHOST DBPORT DBUSER DBPASS

Please after configuration, disable DRYRUN mode via set DRYRUN='false' value. 

verbose possible:
`DEBUG=1`  - any nonempty value = True, empty or  absence of a variable - will be perceived as False.
`BATCHSIZE` - batch size

runing:

    /usr/bin/python3 clean.py

can be run via docker(Dockerfile)

    docker build  . -t crzdmnt
    docker run --env-file=.env crzdmnt

### cleaning databases cp0..cp9:

 - tasks_archive 
 - stream_counters 
 - tasks_history

 DEFAULT_HORIZON = 21 days.

It is possible to configure individual keep horizons for certain converyer_id:.

change it in text string: 

    CUSTOM_HORIZON = """
    # converyer_id, horizon -  keep horison in days. you can add  custom keep horizon per converyer_id. empty strings and strings with # - will be excluded.
    # TODO - to store this part configuration aside of script (external file, db and make normal parser)
    123,  21
    456,  7
    789,  14
    """
## other databases:

    tables = [
        {'dbname': 'conveyor_statistics', 'tablename': 'conveyor_copy_rpc_logic_statistics', 'pk': ('from_conveyor_id', 'to_conveyor_id', 'ts', 'from_node_id'), 'ts_field': 'ts', 'ts_in_millisec': False, 'special_horizon': False},
        {'dbname': 'conveyor_statistics', 'tablename': 'conveyor_logic_statistics',          'pk': ('conveyor_id', 'node_id', 'ts'),                             'ts_field': 'ts', 'ts_in_millisec': False, 'special_horizon': False},
        {'dbname': 'conveyor',            'tablename': 'conveyor_billing',                   'pk': ('conveyor_id', 'user_id', 'ts'),                             'ts_field': 'ts', 'ts_in_millisec': False, 'special_horizon': False},
        {'dbname': 'conveyor',            'tablename': 'cce_exec_time',                      'pk': ('conveyor_id', 'node_id', 'ts'),                             'ts_field': 'ts', 'ts_in_millisec': False, 'special_horizon': False},
        {'dbname': 'conveyor',            'tablename': 'conveyor_called_timers',             'pk': ('conveyor_id', 'node_id', 'ts'),                             'ts_field': 'ts', 'ts_in_millisec': False, 'special_horizon': False},
    ]

 - `dbname` - database name
 - `tablename` - table name
 - `pk` - a tuple of fields that make up pk tables (required for batch deleting)
 - `ts_field` - the name of the field with datetime.
 - `ts_in_millisec` - Flase when datetime unixtimestamp in milliseconds, not seconds.
- `special_horizon` - False, used for horizon conveyor_id in cp-bases.
