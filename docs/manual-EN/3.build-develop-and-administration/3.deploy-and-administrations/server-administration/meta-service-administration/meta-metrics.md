# Meta Metrics

## Introduction

Currently, **Nebula Graph** supports obtaining the basic performance metrics for the meta service via HTTP.

Each performance metric consists of three parts, namely `<counter_name>.<statistic_type>.<time_range>`.

### Counter Names

Each counter name is composed of the interface name and the counter name. Meta service only counts the heartbeat. Currently, the supported interfaces are:

```text
meta_heartbeat_qps
meta_heartbeat_error_qps
meta_heartbeat_latency
```

### Statistics Type

Currently supported types are SUM, COUNT, AVG, RATE and P quantiles (P99, P999, ..., P999999). Among which:

- Metrics have suffixes `_qps` and `_error_qps` support SUM, COUNT, AVG, RATE but don't support P quantiles.
- Metrics have suffixes `_latency` support SUM, COUNT, AVG, RATE, and P quantiles.

### Time Range

Currently, the supported time ranges are 60s, 600s, and 3600s, which correspond to the last minute, the last ten minutes, and the last hour till now.

## Obtain the Corresponding Metrics via HTTP Interface

Here are some examples:

```cpp
meta_heartbeat_qps.avg.60         // the average QPS of the heart beat in the last minute
meta_heartbeat_error_qps.count.60   // the total errors occurred of the heart beat in the last minute
meta_heartbeat_latency.avg.60     // the average latency of the heart beat in the last minute
```

Assume that a **Nebula Graph** meta service is started locally, and the `ws_http_port` port number is set to 11000 when starting. It is sent through the **GET** interface of HTTP. The method name is **get_stats**, and the parameter is stats plus the corresponding metrics name. Here's an example of getting metrics via the HTTP interface:

```bash
# obtain a metrics
curl -G "http://127.0.0.1:11000/get_stats?stats=meta_heartbeat_qps.avg.60"
# meta_heartbeat_qps.avg.60=580

# obtain multiple metrics at the same time
curl -G "http://127.0.0.1:11000/get_stats?stats=meta_heartbeat_qps.avg.60,meta_heartbeat_error_qps.avg.60"
# meta_heartbeat_qps.avg.60=537
# meta_heartbeat_error_qps.avg.60=579

# obtain multiple metrics at the same time and return in json format
curl -G "http://127.0.0.1:11000/get_stats?stats=meta_heartbeat_qps.avg.60,meta_heartbeat_error_qps.avg.60&returnjson"
# [{"value":533,"name":"meta_heartbeat_qps.avg.60"},{"value":574,"name":"meta_heartbeat_error_qps.avg.60"}]

# obtain all the metrics
curl -G "http://127.0.0.1:11000/get_stats?stats"
# or
curl -G "http://127.0.0.1:11000/get_stats"
```
