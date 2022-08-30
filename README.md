
# crzd-mnt
## setup and running
requires the python3-psycopg2 library.

## configuration
setting via configuration file, or pass the ENV for connect db: 
 - `DBHOST`
 - `DBPORT` 
 - `DBUSER` 
 - `DBPASS`
also you can pass ENV:
 - `BATCHSIZE` - number of rows, deleted per transaction. (prevent wal-log flood). Default 10000
 - `CP_COUNT` - number of cp-databases. default 10. depends on your setup. # TODO make it autoconfigurable
 - `DEFAULT_HORIZON` - number of days to keep data for default purposes. Default 21 days.
 - `DEBUG` - any nonempty value = True, empty or  absence of a variable - will be perceived as False.
 - `DRYRUN` - can be enabled only over ENV variable with value 'false'. default value True (python) to prevent accidentaly removing data.

Please after configuration, **disable DRYRUN** mode via set `DRYRUN='false'` value. 

runing:

    /usr/bin/python3 clean.py

can be run via docker(Dockerfile)

    docker build  . -t crzdmnt
    docker run --env-file=.env crzdmnt

## cleaning databases cp0..cpN:

 - tasks_archive 
 - stream_counters 
 - tasks_history

### CUSTOM_HORIZON
It is possible to configure individual keep horizons for certain converyer_id in configuration file. Section custom_horizon. Lines in format 
converyer_id = horizon:

    [custom_horizon]
    6641 = 21
    6751 = 7
    9501 = 14
    
keep horison in days. you can add  custom keep horizon per converyer_id. empty strings and strings with # - will be excluded.

## cleanup other databases:
Objects are hardcoded to cleap.py using python list-of-dicts format:

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
