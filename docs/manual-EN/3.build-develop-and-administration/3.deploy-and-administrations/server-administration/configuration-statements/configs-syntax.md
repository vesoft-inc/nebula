# CONFIG Syntax

Nebula use `gflags` for run-time configurations.

The related three gflags parameters are: `rocksdb_db_options`, `rocksdb_column_family_options` and `rocksdb_block_based_table_options`.

The three parameters are all in json formet, and the key and value of them are in string format. For example, you can set as follows in the conf file of storage:

```
    rocksdb_db_options = {"stats_dump_period_sec":"200", "enable_write_thread_adaptive_yield":"false", "write_thread_max_yield_usec":"600"}
    rocksdb_column_family_options = {"max_write_buffer_number":"4", "min_write_buffer_number_to_merge":"2", "max_write_buffer_number_to_maintain":"1"}
    rocksdb_block_based_table_options = {"block_restart_interval":"2"}
```

Nebula supports changing some rocksdb parameters in storage service as follows:

```
    snap_refresh_nanos
    disable_auto_compactions
    write_buffer_size
    compression
    level0_file_num_compaction_trigger
    max_bytes_for_level_base
    snap_refresh_nanos
    block_size
    block_restart_interval
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
`UPDATE VARIABLE storage:rocksdb_column_family_options = { disable_auto_compactions = false , level0_file_num_compaction_trigger = 10 } `

## SHOW CONFIGS

```sql
SHOW CONFIGS [graph|meta|storage]
```

For example

```sql
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

```sql
GET CONFIGS [graph|meta|storage :] var
```

For example

```sql
nebula> GET CONFIGS storage:load_data_interval_secs
=================================================================
| module  | name                      | type  | mode    | value |
=================================================================
| STORAGE | load_data_interval_secs   | INT64 | MUTABLE | 120   |
-----------------------------------------------------------------
```

```sql
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

```sql
UPDATE CONFIGS [graph|meta|storage :] var = value
```

> The updated CONFIGS will be stored into meta-service permanently.
> If the CONFIG's mode is `MUTABLE`, the change will take effects immediately. Otherwise, if the mode is `REBOOT`, the change will not work until server restart.

For example

```sql
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
