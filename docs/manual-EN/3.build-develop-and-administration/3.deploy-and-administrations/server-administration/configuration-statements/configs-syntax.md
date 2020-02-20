# CONFIG Syntax

## Introduction to Configuration

**Nebula Graph** gets configuration from meta by default. If you want to get configuration locally, please add the `local_config=true` option in the configuration files `metad.conf`, `storaged.conf`, `graphd.conf` (directory is `/home/user/nebula/build/install/etc`) respectively.

**Note:**

- Configuration precedence: meta > console > environment variable > configuration files.
- If set `local_config` to true, the configuration files take precedence.
- Restart the services after changing the configuration files to take effect.
- Configuration changes in console take effect in real time.

## gflag Parameters

**Nebula Graph** uses `gflags` for run-time configurations.

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
nebula> GET CONFIGS storage:local_ip
=======================================================
| module  | name     | type   | mode      | value     |
=======================================================
| STORAGE | local_ip | STRING | IMMUTABLE | 127.0.0.1 |
-------------------------------------------------------
```

```ngql
nebula> GET CONFIGS heartbeat_interval_secs
=================================================================
| module  | name                    | type  | mode      | value |
=================================================================
| GRAPH   | heartbeat_interval_secs | INT64 | MUTABLE | 10    |
-----------------------------------------------------------------
| STORAGE | heartbeat_interval_secs | INT64 | MUTABLE | 10    |
-----------------------------------------------------------------
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
nebula> UPDATE CONFIGS storage:heartbeat_interval_secs=1
nebula> GET CONFIGS storage:heartbeat_interval_secs
===============================================================
| module  | name                    | type  | mode    | value |
===============================================================
| STORAGE | heartbeat_interval_secs | INT64 | MUTABLE | 1     |
---------------------------------------------------------------
```
