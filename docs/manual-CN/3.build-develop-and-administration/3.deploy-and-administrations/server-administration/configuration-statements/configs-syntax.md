# CONFIG 语法

Nebula使用 `gflags` 进行运行时配置。

相关的 `gflags` 参数有三个，分别为 rocksdb_db_options，rocksdb_column_family_options，rocksdb_block_based_table_options。
三个参数均为 json 格式，其中每个参数 key 和 value 均为 string 格式。例如可以在 storage 的 conf 文件中做如下设置

```text
    rocksdb_db_options = {"stats_dump_period_sec":"200", "enable_write_thread_adaptive_yield":"false", "write_thread_max_yield_usec":"600"}
    rocksdb_column_family_options = {"max_write_buffer_number":"4", "min_write_buffer_number_to_merge":"2", "max_write_buffer_number_to_maintain":"1"}
    rocksdb_block_based_table_options = {"block_restart_interval":"2"}
```

另外目前支持动态修改 storage service 的部分 rocksdb 参数, 如下

```text
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

示例

```ngql
UPDATE CONFIGS storage:rocksdb_column_family_options =
{ disable_auto_compactions = false , level0_file_num_compaction_trigger = 10 }
```

## 显示变量

```ngql
SHOW CONFIG graph|meta|storage
```

例如

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

### 获取变量

```ngql
GET CONFIGS [graph|meta|storage :] var
```

例如

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

### 更新变量

```ngql
UPDATE CONFIGS [graph|meta|storage :] var = value
```

> 更新的变量将永久存储于 meta-service 中。
> 如果变量模式为 `MUTABLE`，更改会即时生效。如果模式为 `REBOOT`，更改在服务器重启后生效。

例如

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
