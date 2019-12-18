# Storage Metrics

## Introduction

Currently, **Nebula Graph** supports obtaining the basic performance metrics for the storage service via HTTP.

Each performance metric consists of three parts, namely `<counter_name>.<statistic_type>.<time_range>`, details are introduced as the follows.

### Counter Names

Each counter name is composed of the interface name and the counter name. Currently, the supported interfaces are:

```cpp
storage_vertex_props // obtain properties of a vertex
storage_edge_props // obtain properties of an edge
storage_add_vertex // insert a vertex
storage_add_edge // insert an edge
storage_del_vertex // delete a vertex
storage_update_vertex // update properties of a vertex
storage_update_edge // update properties of an edge
storage_get_kv // read kv pair
storage_put_kv // put kv pair
storage_get_bound // internal use only
```

Each interface has three metrics, namely latency (in the units of us), QPS and QPS with errors. The suffixes are as follows:

```text
_latency
_qps
_error_qps
```

The complete metric concatenates the interface name with the corresponding metric, such as `storage_add_vertex_latency`, `storage_add_vertex_qps` and `storage_add_vertex_error_qps`, representing the latency, QPS, and the QPS with errors of inserting a vertex, respectively.

### Statistics Type

Currently supported types are SUM, COUNT, AVG, RATE, and P quantiles (P99, P999, ..., P999999). Among which:

- Metrics have suffixes `_qps` and `_error_qps` support SUM, COUNT, AVG, RATE but don't support P quantiles.
- Metrics have suffixes `_latency` support SUM, COUNT, AVG, RATE, and P quantiles.

### Time Range

Currently, the supported time ranges are 60s, 600s, and 3600s, which correspond to the last minute, the last ten minutes, and the last hour till now.

## Obtain the Corresponding Metrics via HTTP Interface

According to the above introduction, you can make a complete metrics name, here are some examples:

```cpp
storage_add_vertex_latency.avg.60   // the average latency of inserting a vertex in the last minute
storage_get_bound_qps.rate.600  // obtain neighbor's QPS in the last ten minutes
storage_update_edge_error_qps.count.3600  // errors occurred in updating an edge in the last hour
```

Assume that a **Nebula Graph** storage service is started locally, and the `ws_http_port` port number is set to 12000 when starting. It is sent through the **GET** interface of HTTP. The method name is **get_stats**, and the parameter is stats plus the corresponding metrics name. Here's an example of getting metrics via the HTTP interface:

```bash
# obtain a metrics
curl -G "http://127.0.0.1:12000/get_stats?stats=storage_vertex_props_qps.rate.60"
# storage_vertex_props_qps.rate.60=2674

# obtain multiple metrics at the same time
curl -G "http://127.0.0.1:12000/get_stats?stats=storage_vertex_props_qps.rate.60,storage_vertex_props_latency.avg.60"
# storage_vertex_props_qps.rate.60=2638
# storage_vertex_props_latency.avg.60=812

# obtain multiple metrics at the same time and return in json format
curl -G "http://127.0.0.1:12000/get_stats?stats=storage_vertex_props_qps.rate.60,storage_vertex_props_latency.avg.60&returnjson"
# [{"value":2723,"name":"storage_vertex_props_qps.rate.60"},{"value":804,"name":"storage_vertex_props_latency.avg.60"}]

# obtain all the metrics
curl -G "http://127.0.0.1:12000/get_stats?stats"
# or
curl -G "http://127.0.0.1:12000/get_stats"
```
