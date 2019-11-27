# CONFIG Syntax

**Nebula Graph** uses `gflags` for run-time configurations.

## gflag Parameters

There are four gflags related parameters, among which, `max_edge_returned_per_vertex` is used to control the max edges returned by a certain vertex, `rocksdb_db_options`, `rocksdb_column_family_options` and `rocksdb_block_based_table_options`
 are all in json format, and the key and value of them are in string format. For example, you can set as follows in the conf file of storage:

```text
    rocksdb_db_options = {"stats_dump_period_sec":"200", "enable_write_thread_adaptive_yield":"false", "write_thread_max_yield_usec":"600"}
    rocksdb_column_family_options = {"max_write_buffer_number":"4", "min_write_buffer_number_to_merge":"2", "max_write_buffer_number_to_maintain":"1"}
    rocksdb_block_based_table_options = {"block_restart_interval":"2"}
    "max_edge_returned_per_vertex":"INT_MAX"
```

**Nebula Graph** supports changing some rocksdb parameters in storage service as follows:

```text
    // rocksdb_column_family_options
    max_write_buffer_number
    level0_file_num_compaction_trigger
    level0_slowdown_writes_trigger
    level0_stop_writes_trigger
    target_file_size_base
    target_file_size_multiplier
    max_bytes_for_level_base
    max_bytes_for_level_multiplier
    ttl
    disable_auto_compactions

    // rocksdb_db_options
    max_total_wal_size
    delete_obsolete_files_period_micros
    max_background_jobs
    base_background_compactions
    max_background_compactions
    stats_dump_period_sec
    compaction_readahead_size
    writable_file_max_buffer_size
    bytes_per_sync
    wal_bytes_per_sync
    delayed_write_rate
    avoid_flush_during_shutdown
    max_open_files
```

For example

```ngql
nebula> UPDATE CONFIGS storage:rocksdb_column_family_options = \
        { disable_auto_compactions = false ,         level0_file_num_compaction_trigger = 10 }
```

## SHOW CONFIGS

```ngql
SHOW CONFIGS [graph|meta|storage]
```

For example

```ngql
nebula> SHOW CONFIGS meta
============================================================================================================================
| module | name                                        | type   | mode      | value                                        |
============================================================================================================================
| META   | v                                           | INT64  | IMMUTABLE | 4                                            |
----------------------------------------------------------------------------------------------------------------------------
| META   | help                                        | BOOL   | IMMUTABLE | False                                        |
----------------------------------------------------------------------------------------------------------------------------
| META   | port                                        | INT64  | IMMUTABLE | 45500                                        |
----------------------------------------------------------------------------------------------------------------------------
```

## Get CONFIGS

```ngql
GET CONFIGS [graph|meta|storage :] var
```

For example

```ngql
nebula> GET CONFIGS storage:load_data_interval_secs
=================================================================
| module  | name                      | type  | mode    | value |
=================================================================
| STORAGE | load_data_interval_secs   | INT64 | MUTABLE | 120   |
-----------------------------------------------------------------
```

```ngql
nebula> GET CONFIGS load_data_interval_secs
=================================================================
| module  | name                    | type  | mode      | value |
=================================================================
| GRAPH   | load_data_interval_secs | INT64 | MUTABLE   | 120   |
-----------------------------------------------------------------
| META    | load_data_interval_secs | INT64 | IMMUTABLE | 120   |
-----------------------------------------------------------------
| STORAGE | load_data_interval_secs | INT64 | MUTABLE   | 120   |
-----------------------------------------------------------------
Got 3 rows (Time spent: 1449/2339 us)
```

## Update CONFIGS

```ngql
UPDATE CONFIGS [graph|meta|storage :] var = value
```

> The updated CONFIGS will be stored into meta-service permanently.
> If the configuration's mode is `MUTABLE`, the change will take effects immediately. Otherwise, if the mode is `REBOOT`, the change will not work until server restart.
> Expression is supported in UPDATE CONFIGS.
For example

```ngql
nebula> UPDATE CONFIGS storage:load_data_interval_secs=1
Execution succeeded (Time spent: 1750/2484 us)
nebula> GET CONFIGS storage:load_data_interval_secs
===============================================================
| module  | name                    | type  | mode    | value |
===============================================================
| STORAGE | load_data_interval_secs | INT64 | MUTABLE | 1     |
---------------------------------------------------------------
Got 1 rows (Time spent: 1678/3420 us)
```
