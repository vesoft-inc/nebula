# RocksDB Compaction & Flush

Nebula supports auto and manual (through HTTP request) RocksDB compaction or flush in storage service.

## Auto RocksDB Compaction

### Turn off auto Compaction

Input the following commands in console to **turn off** the auto compaction and check if the change works.

```ngql
nebula> UPDATE CONFIGS storage:rocksdb_column_family_options = { disable_auto_compactions = true }

nebula> GET CONFIGS storage:rocksdb_column_family_options
=========================================================================================================
| module  | name                          | type   | mode    | value                                    |
=========================================================================================================
| STORAGE | rocksdb_column_family_options | NESTED | MUTABLE | {
  "disable_auto_compactions": "true"
} |
---------------------------------------------------------------------------------------------------------
```

### Turn on auto Compaction

Input the following commands in console to **turn on** the auto compaction and check if the change works. The default value of auto compaction is false in nebula.

```ngql
nebula> UPDATE CONFIGS storage:rocksdb_column_family_options = { disable_auto_compactions = false }

nebula> GET CONFIGS storage:rocksdb_column_family_options
==========================================================================================================
| module  | name                          | type   | mode    | value                                     |
==========================================================================================================
| STORAGE | rocksdb_column_family_options | NESTED | MUTABLE | {
  "disable_auto_compactions": "false"
} |
----------------------------------------------------------------------------------------------------------
```

## Manually Trigger compaction and flush

Use the following command to trigger compaction and flush manually through HTTP request.

```bash
curl "${ws_ip}:${ws_http_port}/admin?space=${spaceName}&${op}"
```

- `ws_ip` is the HTTP service IP, which can be found in config file `storage.conf`
- `ws_port` is the storage HTTP port
- `op` is the associated admin operations, only `compact` and `flush` are supported currently

For example:

```bash
curl "http://127.0.0.1:50005/admin?space=test&op=compact"
curl "http://127.0.0.1:50005/admin?space=test&op=flush"
```
