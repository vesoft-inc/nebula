# Graph Metrics

## 介绍

目前，**Nebula Graph** 支持通过 HTTP 方式来获取 Graph Service 层的基本性能指标。

每一个性能指标都由三部分组成，分别为指标名、统计类型、时间范围。

| counter\_name | statistic\_type | time_range |
| ----  |  ----|-------|

### 指标名

每个指标名都由服务名加模块名构成，目前支持获取如下接口：

```cpp
通过 storageClient 发送的请求，需要同时向多个 storage 并发多条消息时，按一次统计  graph_storageClient
通过 metaClient 发送的请求
graph_graph_all 客户端向 graph 发送的请求，当一条请求包含多条语句时，按一条计算 graph_metaClient
插入点 graph_insertVertex
插入边 graph_insertEdge
删除点 graph_deleteVertex
删除边 graph_deleteEdge //未支持
更新点的属性 graph_updateVertex
更新边的属性 graph_updateEdge
执行 go 命令 graph_go
查找最小路径或者全路径 graph_findPath
获取点属性，不统计获取点的总数，只统计执行命令的数量 graph_fetchVertex
获取边属性，不统计边的总数，只统计执行命令的数量 graph_fetchEdge
```

每一个接口都有三个性能指标，分别为延迟(单位为 us)、成功的 QPS、发生错误的 QPS，后缀名如下：

```text
_latency
_qps
_error_qps
```

将接口名和相应指标连接在一起即可获得完整的指标名，例如 `graph_insertVertex_latency`、`graph_insertVertex_qps`、`graph_insertVertex_error_qps`、分别代表插入一个点的延迟、QPS 和发生错误的 QPS。

### 统计类型

目前支持的统计类型有 SUM、COUNT、AVG、RATE 和 P 分位数 (P99，P999， ... ，P999999)。其中：

- `_qps`、`_error_qps` 后缀的指标，支持 SUM、COUNT、AVG、RATE，但不支持 P 分位；
- `_latency` 后缀的指标，支持 SUM、COUNT、AVG、RATE，也支持 P 分位。

### 时间范围

时间范围目前只支持三种，分别为 60、600、3600，分别表示最近一分钟，最近十分钟和最近一小时。

## 通过 HTTP 接口获取相应的性能指标

根据上面的介绍，就可以写出一个完整的指标名称了，下面是一些示例：

```cpp
graph_insertVertex_latency.avg.60        // 最近一分钟插入点命令执行成功的平均延时
graph_updateEdge_error_qps.count.3600   // 最近一小时更新边命令失败的总计数量
```

假设本地启动了一个 nebula graph service，同时启动时设置的 `ws_http_port` 端口号为 13000。通过 HTTP 的 **GET** 接口发送，方法名为 **get_stats**，参数为 stats 加对应的指标名字。下面是通过 HTTP 接口获取指标的示例：

```bash
# 获取一个指标
curl -G "http://127.0.0.1:13000/get_stats?stats=graph_insertVertex_qps.rate.60"
# graph_insertVertex_qps.rate.60=3069

# 同时获取多个指标
curl -G "http://127.0.0.1:13000/get_stats?stats=graph_insertVertex_qps.rate.60, graph_deleteVertex_latency.avg.60"
# graph_insertVertex_qps.rate.60=3069
# graph_deleteVertex_latency.avg.60=837

# 同时获取多个指标并以 json 格式返回
curl -G "http://127.0.0.1:13000/get_stats?stats=graph_insertVertex_qps.rate.60, graph_deleteVertex_latency.avg.60&returnjson"
# [{"value":2373,"name":"graph_insertVertex_qps.rate.60"},{"value":760,"name":"graph_deleteVertex_latency.avg.60"}]

# 获取所有指标
curl -G "http://127.0.0.1:13000/get_stats?stats"
# 或
curl -G "http://127.0.0.1:13000/get_stats"
```
