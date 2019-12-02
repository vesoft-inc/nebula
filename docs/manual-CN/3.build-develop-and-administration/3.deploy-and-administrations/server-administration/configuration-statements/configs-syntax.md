# CONFIG 语法

**Nebula Graph** 使用 `gflags` 进行运行时配置。

## gflags 参数

相关的 `gflags` 参数有四个，其中，`max_edge_returned_per_vertex` 用来控制一个点最多可以搜索返回的边数，`rocksdb_db_options`，`rocksdb_column_family_options`，`rocksdb_block_based_table_options`
三个参数均为 json 格式，其中每个参数 key 和 value 均为 string 格式。例如可以在 storage 的 conf 文件中做如下设置：

```text
    rocksdb_db_options = {"stats_dump_period_sec":"200", "enable_write_thread_adaptive_yield":"false", "write_thread_max_yield_usec":"600",
    "max_edge_returned_per_vertex":"INT_MAX"}
    rocksdb_column_family_options = {"max_write_buffer_number":"4", "min_write_buffer_number_to_merge":"2", "max_write_buffer_number_to_maintain":"1"}
    rocksdb_block_based_table_options = {"block_restart_interval":"2"}
```

另外目前支持动态修改 storage service 的部分 rocksdb 参数, 如下

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

示例

```ngql
nebula> UPDATE CONFIGS storage:rocksdb_column_family_options = \
        { disable_auto_compactions = false ,         level0_file_num_compaction_trigger = 10 }
```

### 显示变量

```ngql
SHOW CONFIGS [graph|meta|storage]
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
> 支持在 UPDATE CONFIGS 中使用算式。

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
