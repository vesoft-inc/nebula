# Nebula Storage Metrics

## Introduction

Currently, Nebula supports obtaining some basic performance metrics for the storage service via HTTP.

Each performance metrics consists of three parts, namely `<counter_name>.<statistic_type>.<time_range>`, details are introduced as the follows.

### Counter Names

Each counter name is composed of the interface name and the counter name. Currently, the supported interfaces are:

```cpp
vertex_props // obtain properties of a vertex
edge_props // obtain properties of an edge
add_vertex // insert a vertex
add_edge // insert an edge
del_vertex // delete a vertex
update_vertex // update properties of a vertex
update_edge // update properties of an edge
get_kv // read kv pair
put_kv // put kv pair
get_bound // internal use only
```

Each interface has three metrics, namely latency (in the units of us), QPS and QPS with errors. The suffixes are as follows:

```
_latency
_qps
_error_qps
```

The complete metric concatenates the interface name with the corresponding metric, such as `add_vertex_latency`, `add_vertex_qps`, and `add_vertex_error_qps`, representing the latency of inserting a vertex, QPS, and the QPS with errors, respectively.

### Statistics Type

Currently supported types are SUM, COUNT, AVG, RATE, and P99, P999, the maximum supported range is P999999.

### Time Range

Currently supported time ranges are 60, 600, and 3600, which mean the last minute, the last ten minutes, and the last hour till now.

## Obtain the Corresponding Metrics via HTTP Interface

According to the above introduction, you can write a complete metrics name, here are some examples:

```cpp
add_vertex_latency.avg.60   // the average latency of inserting a vertex in the last minute
get_bound_qps.rate.600  // obtain neighbor's QPS in the last ten minutes
update_edge_error_qps.count.3600  // errors occurred in updating an edge in the last hour
```

Assume that a nebula storage service is started locally, and the `ws_http_port` port number is set to 50005 when starting. It is sent through the GET interface of HTTP. The method name is get_stats, and the parameter is stats plus the corresponding metrics name. Here's an example of getting metrics via the HTTP interface:

```shell
# obtain a metrics
curl -G "http://127.0.0.1:50005/get_stats?stats=vertex_props_qps.rate.60"
# vertex_props_qps.rate.60=2674

# obtain multiple metrics at the same time
curl -G "http://127.0.0.1:50005/get_stats?stats=vertex_props_qps.rate.60,vertex_props_latency.avg.60"
# vertex_props_qps.rate.60=2638
# vertex_props_latency.avg.60=812

# obtain multiple metrics at the same time and return in json format
curl -G "http://127.0.0.1:50005/get_stats?stats=vertex_props_qps.rate.60,vertex_props_latency.avg.60&returnjson"
# [{"value":2723,"name":"vertex_props_qps.rate.60"},{"value":804,"name":"vertex_props_latency.avg.60"}]

# obtain all the metrics
curl -G "http://127.0.0.1:50005/get_stats?stats"
# or
curl -G "http://127.0.0.1:50005/get_stats"
```
