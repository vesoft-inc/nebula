# RocksDB Compaction 和 Flush

**Nebula Graph** 支持对 storage 中的 RocksDB 设置是否自动 compaction，支持通过 HTTP 请求，手动触发 storage 的 RocksDB compaction 或 flush。

## 自动 RocksDB Compaction

### 关闭自动 Compaction

在 console 输入如下命令**关闭**自动 compaction 并查看更改是否生效。

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

**注意：** 可以在大批量数据写入前关闭 auto compaction，但请记得批量写入后再将其打开，长期关闭 auto compaction 会影响后续的读性能。

### 打开自动 Compaction

在 console 输入如下命令**打开**自动 compaction 并查看更改是否生效。Nebula 默认 compaction 为打开。

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

## 手动触发 Compaction 和 Flush

手动触发 compaction 和 flush 需要通过 HTTP 请求，命令如下。

```bash
curl "${ws_ip}:${ws_http_port}/admin?space=${spaceName}&${op}"
```

- `ws_ip` 为 HTTP 服务的 IP，可以在 `etc/storage.conf` 配置文件中找到。
- `ws_http_port` 为 storage 的 HTTP 服务端口。
- `op` 为相关的 admin 操作，当前只支持 `compact` 和 `flush`。

例如：

```bash
curl "http://127.0.0.1:12000/admin?space=test&op=compact"
curl "http://127.0.0.1:12000/admin?space=test&op=flush"
```

> 注意：请先建立图空间 test，再运行上面的 curl 命令。
