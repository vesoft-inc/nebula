# RocksDB Compaction and Flush

**Nebula Graph** supports auto and manual (through HTTP request) RocksDB compaction or flush in storage service.

## Auto RocksDB Compaction

### Turn off Auto Compaction

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

**Note:** You can turn off auto compaction before bulk writing data, but remember to turn it on again since long-term shutdown auto compaction affects the subsequent read performance.

### Turn on Auto Compaction

Input the following commands in console to **turn on** the auto compaction and check if the change works. The default value of auto compaction is false in **Nebula Graph**.

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

## Manually Trigger Compaction and Flush

Use the following command to trigger compaction and flush manually through HTTP request.

```bash
curl "${ws_ip}:${ws_http_port}/admin?space=${spaceName}&${op}"
```

- `ws_ip` is the HTTP service IP, which can be found in config file `etc/storage.conf`
- `ws_port` is the storage HTTP port
- `op` is the associated admin operations, only `compact` and `flush` are supported currently

For example:

```bash
curl "http://127.0.0.1:12000/admin?space=test&op=compact"
curl "http://127.0.0.1:12000/admin?space=test&op=flush"
```

> Notice that you should create space test before the above curl command.
