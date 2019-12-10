# Graph Metrics

## Introduction

Currently, **Nebula Graph** supports obtaining the basic performance metric for the graph service via HTTP.

Each performance metrics consists of three parts, namely `<counter_name>.<statistic_type>.<time_range>`.

### Counter Names

Each counter name is composed of the interface name and the counter name. Currently, the supported interfaces are:

```cpp
graph_storageClient // Requests sent via storageClient, when sending requests to multiple storages concurrently, counted as one
graph_metaClient // Requests sent via metaClient
graph_graph_all // Requests sent by the client to the graph, when a request contains multiple queries, counted as one
graph_insertVertex // Insert a vertex
graph_insertEdge // Insert an edge
graph_deleteVertex // Delete a vertex
graph_deleteEdge // Delete an edge // Not supported yet
graph_updateVertex // Update properties of a vertex
graph_updateEdge // Update properties of an edge
graph_go // Execute the go command
graph_findPath // Find the shortest path or the full path
graph_fetchVertex // Fetch the vertex's properties. Only count the commands executed rather than the total number of fetched vertices.
graph_fetchEdge // Fetch the edge's properties. Only count the commands executed rather than the total number of fetched edges.
```

Each interface has three metrics, namely latency (in the units of us), QPS and QPS with errors. The suffixes are as follows:

```text
_latency
_qps
_error_qps
```

The complete metric concatenates the interface name with the corresponding metric, such as `graph_insertVertex_latency`, `graph_insertVertex_qps` and `graph_insertVertex_error_qps`, representing the latency of inserting a vertex, QPS and the QPS with errors, respectively.

### Statistics Type

Currently supported types are SUM, COUNT, AVG, RATE and P quantiles (P99, P999, ..., P999999). Among which:

- Metrics have suffixes `_qps` and `_error_qps` support SUM, COUNT, AVG, RATE but don't support P quantiles.
- Metrics have suffixes `_latency` support SUM, COUNT, AVG, RATE, and P quantiles.

### Time Range

Currently, the supported time ranges are 60s, 600s, and 3600s, which correspond to the last minute, the last ten minutes, and the last hour till now.

## Obtain the Corresponding Metrics via HTTP Interface

According to the above introduction, you can make a complete metrics name. Here are some examples:

```cpp
graph_insertVertex_latency.avg.60   // the average latency of successfully inserting a vertex in the last minute
graph_updateEdge_error_qps.count.3600  // total number of failures in updating an edge in the last hour
```

Assume that a graph service is started locally, and the `ws_http_port` port number is set to 13000 when starting. It is sent through the **GET** interface of HTTP. The method name is **get_stats**, and the parameter is stats plus the corresponding metrics name. Here's an example of getting metrics via the HTTP interface:

```bash
# obtain a metrics
curl -G "http://127.0.0.1:13000/get_stats?stats=graph_insertVertex_qps.rate.60"
# graph_insertVertex_qps.rate.60=3069

# obtain multiple metrics at the same time
curl -G "http://127.0.0.1:13000/get_stats?stats=graph_insertVertex_qps.rate.60, graph_deleteVertex_latency.avg.60"
# graph_insertVertex_qps.rate.60=3069
# graph_deleteVertex_latency.avg.60=837

# obtain multiple metrics at the same time and return in json format
curl -G "http://127.0.0.1:13000/get_stats?stats=graph_insertVertex_qps.rate.60, graph_deleteVertex_latency.avg.60&returnjson"
# [{"value":2373,"name":"graph_insertVertex_qps.rate.60"},{"value":760,"name":"graph_deleteVertex_latency.avg.60"}]

# obtain all the metrics
curl -G "http://127.0.0.1:13000/get_stats?stats"
# or
curl -G "http://127.0.0.1:13000/get_stats"
```
