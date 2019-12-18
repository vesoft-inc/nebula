# Storage Metrics

## 介绍

目前，**Nebula Graph** 支持通过 HTTP 方式来获取 Storage Service 层的基本性能指标。

每一个性能指标都由三部分组成，分别为指标名、统计类型、时间范围。

| counter\_name | statistic\_type | time_range |
| ----  |  ----|-------|

下面将分别介绍这三部分。

### 指标名

每个指标名都由服务名加模块名构成，目前支持获取如下接口：

```text
获取点的属性 storage_vertex_props
获取边的属性 storage_edge_props
插入一个点 storage_add_vertex
插入一条边 storage_add_edge
删除一个点 storage_del_vertex
更新一个点的属性 storage_update_vertex
更新一条边的属性 storage_update_edge
读取一个键值对 storage_get_kv
写入一个键值对 storage_put_kv
仅限内部使用 storage_get_bound
```

每一个接口都有三个性能指标，分别为延迟(单位为 us)、成功的 QPS、发生错误的 QPS，后缀名如下：

```text
_latency
_qps
_error_qps
```

将接口名和相应指标连接在一起即可获得完整的指标名，例如 `storage_add_vertex_latency`、`storage_add_vertex_qps`、`storage_add_vertex_error_qps` 分别代表插入一个点的延迟、QPS 和发生错误的 QPS。

### 统计类型

目前支持的统计类型有 SUM、COUNT、AVG、RATE 和 P 分位数 (P99，P999， ... ，P999999)。其中：

- `_qps`、`_error_qps` 后缀的指标，支持 SUM、COUNT、AVG、RATE，但不支持 P 分位；
- `_latency` 后缀的指标，支持 SUM、COUNT、AVG、RATE，也支持 P 分位。

### 时间范围

时间范围目前只支持三种，分别为 60、600、3600，分别表示最近一分钟、最近十分钟和最近一小时。

## 通过 HTTP 接口获取相应的性能指标

根据上面的介绍，就可以写出一个完整的指标名称了，下面是一些示例：

```cpp
storage_add_vertex_latency.avg.60        // 最近一分钟插入一个点的平均延时
storage_get_bound_qps.rate.600        // 最近十分钟获取邻点的 QPS
storage_update_edge_error_qps.count.3600   // 最近一小时更新一条边发生错误的总计数量
```

假设本地启动了一个 nebula storage service，同时启动时设置的 `ws_http_port` 端口号为 12000。通过 HTTP 的 **GET** 接口发送，方法名为 **get_stats**，参数为 stats 加对应的指标名字。下面是通过 HTTP 接口获取指标的示例：

```bash
# 获取一个指标
curl -G "http://127.0.0.1:12000/get_stats?stats=storage_vertex_props_qps.rate.60"
# storage_vertex_props_qps.rate.60=2674

# 同时获取多个指标
curl -G "http://127.0.0.1:12000/get_stats?stats=storage_vertex_props_qps.rate.60,storage_vertex_props_latency.avg.60"
# storage_vertex_props_qps.rate.60=2638
# storage_vertex_props_latency.avg.60=812

# 同时获取多个指标并以 json 格式返回
curl -G "http://127.0.0.1:12000/get_stats?stats=storage_vertex_props_qps.rate.60,storage_vertex_props_latency.avg.60&returnjson"
# [{"value":2723,"name":"storage_vertex_props_qps.rate.60"},{"value":804,"name":"storage_vertex_props_latency.avg.60"}]

# 获取所有指标
curl -G "http://127.0.0.1:12000/get_stats?stats"
# 或
curl -G "http://127.0.0.1:12000/get_stats"
```
