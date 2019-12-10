# Meta Metrics

## 介绍

目前，**Nebula Graph** 支持通过 HTTP 方式来获取 Meta Service 层的基本性能指标。

每一个性能指标都由三部分组成，分别为指标名、统计类型、时间范围。

| counter\_name | statistic\_type | time_range |
| ----  |  ----|-------|

### 指标名

每个指标名都由服务名加模块名构成，meta 只统计心跳信息，目前支持获取如下接口：

```text
meta_heartbeat_qps
meta_heartbeat_error_qps
meta_heartbeat_latency
```

### 统计类型

目前支持的统计类型有 SUM、COUNT、AVG、RATE 和 P 分位数 (P99，P999， ... ，P999999)。其中：

- `_qps`、`_error_qps` 后缀的指标，支持 SUM、COUNT、AVG、RATE，但不支持 P 分位；
- `_latency` 后缀的指标，支持 SUM、COUNT、AVG、RATE，也支持 P 分位。

### 时间范围

时间范围目前只支持三种，分别为 60、600、3600，分别表示最近一分钟，最近十分钟和最近一小时。

## 通过 HTTP 接口获取相应的性能指标

下面是一些示例：

```cpp
meta_heartbeat_qps.avg.60         // 最近一分钟心跳的平均 QPS
meta_heartbeat_error_qps.count.60   // 最近一分钟心跳的平均错误总计数量
meta_heartbeat_latency.avg.60     // 最近一分钟心中的平均延时
```

假设本地启动了一个 nebula meta service，同时启动时设置的 `ws_http_port` 端口号为 11000。通过 HTTP 的 **GET** 接口发送，方法名为 **get_stats**，参数为 stats 加对应的指标名字。下面是通过 HTTP 接口获取指标的示例：

```bash
# 获取一个指标
curl -G "http://127.0.0.1:11000/get_stats?stats=meta_heartbeat_qps.avg.60"
# meta_heartbeat_qps.avg.60=580

# 同时获取多个指标
curl -G "http://127.0.0.1:11000/get_stats?stats=meta_heartbeat_qps.avg.60,meta_heartbeat_error_qps.avg.60"
# meta_heartbeat_qps.avg.60=537
# meta_heartbeat_error_qps.avg.60=579

# 同时获取多个指标并以 json 格式返回
curl -G "http://127.0.0.1:11000/get_stats?stats=meta_heartbeat_qps.avg.60,meta_heartbeat_error_qps.avg.60&returnjson"
# [{"value":533,"name":"meta_heartbeat_qps.avg.60"},{"value":574,"name":"meta_heartbeat_error_qps.avg.60"}]

# 获取所有指标
curl -G "http://127.0.0.1:11000/get_stats?stats"
# 或
curl -G "http://127.0.0.1:11000/get_stats"
```
