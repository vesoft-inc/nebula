# Storage Performance Tool

`_build/storage_perf` is the performance tool to test the storage service.

You should create a graph space, tag and edge type before testing.

The default value is `test`, `test_tag` and `test_edge`.

***

## Configuration Reference

Property Name            | Default Value   | Description
------------------------ | --------------- | -----------
`threads`                | 2               | Total threads for perf.
`qps`                    | 1000            | Total qps for the perf tool.
`totalReqs`              | 10000           | Total requests during the perf test.
`io_threads`             | 10              | Client io threads.
`method`                 | "getNeighbors"  | method type being tested,such as getNeighbors, addVertices, addEdges, getVertices.
`meta_server_addrs`      | ""              | meta server address.
`min_vertex_id`          | 1               | The smallest vertex Id.
`max_vertex_id`          | 10000           | The biggest vertex Id.
`size`                   | 1000            | The data size per request.
`space_name`             | "test"          | Specify the space name.
`tag_name`               | "test_tag"      | Specify the tag name.
`edge_name`              | "test_edge"     | Specify the edge name.
`random_message`         | false           | Whether to write random message to storage service.

### Storage Integrity Tool

Integration test is based on `IntegrationTestBigLinkedList` of HBase.
