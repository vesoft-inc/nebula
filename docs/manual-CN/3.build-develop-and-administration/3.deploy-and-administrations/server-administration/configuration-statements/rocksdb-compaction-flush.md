# RocksDB Compaction & Flush

Nebula 支持对 storage 中的 RocksDB 设置是否自动 compaction，支持通过 HTTP 请求，手动触发 storage 的 RocksDB compaction 或 flush。

## 自动 RocksDB Compaction

### 关闭自动 compaction

在 console 输入如下命令**关闭**自动 compaction。

```bash
UPDATE CONFIGS storage:rocksdb_column_family_options = { disable_auto_compactions = true }
```

### 打开自动 Compaction

在 console 输入如下命令**打开**自动 compaction。

```bash
UPDATE CONFIGS storage:rocksdb_column_family_options = { disable_auto_compactions = false }
```

## 手动触发 compaction 和 flush

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

> 注意：请先建立图空间test，再运行上面的curl命令。

