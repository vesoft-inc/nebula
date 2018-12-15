/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

namespace cpp nebula.storage
namespace java nebula.storage
namespace go nebula.storage

enum ErrorCode {
    SUCCEEDED = 0,

    // RPC Failure
    E_DISCONNECTED = -1,
    E_FAILED_TO_CONNECT = -2,
    E_RPC_FAILURE = -3,
} (cpp.enum_strict)


// These are all data types supported in the graph properties
enum SupportedType {
    // Simple types
    BOOL = 1,
    INT = 2,
    VID = 3,
    FLOAT = 4,
    DOUBLE = 5,
    STRING = 6,

    // Date time
    TIMESTAMP = 21,
    YEAR = 22,
    YEARMONTH = 23,
    DATE = 24,
    DATETIME = 25,

    // Graph specific
    PATH = 41,

    // Container types
//    LIST = 101,
//    SET = 102,
//    MAP = 103,      // The key type is always a STRING
//    STRUCT = 104,
} (cpp.enum_strict)


struct ValueType {
    1: SupportedType type;
    // vtype only exists when the type is a LIST, SET, or MAP
    2: optional ValueType value_type (cpp.ref = true);
    // When the type is STRUCT, schema defines the struct
    3: optional Schema schema (cpp.ref = true);
} (cpp.virtual)


struct ColumnDef {
    1: required string name,
    2: required ValueType type,
}


struct Schema {
    1: list<ColumnDef> columns,
}


enum PropOwner {
    SOURCE = 1,
    DEST = 2,
    EDGE = 3,
} (cpp.enum_strict)


struct PropDef {
    1: PropOwner owner,
    2: i32 tagId,       // Only valid when owner is SOURCE or DEST
    3: string name,     // Property name
}


enum StatType {
    SUM = 1,
    COUNT = 2,
    AVG = 3,
} (cpp.enum_strict)


struct PropStat {
    1: StatType stat,
    2: PropDef prop,
}


struct QueryResponse {
    1: required ErrorCode error_code,
    2: required i32 latency_in_ms,      // Query latency from storage service
    3: optional Schema schema,
    4: optional binary data,            // Encoded data
}


struct ExecResponse {
    1: required ErrorCode error_code,
    2: required i32 latency_in_ms,      // Execution latency
}


struct ExecResponseList {
    1: required list<ErrorCode> error_codes,
    2: required i32 latency_in_ms,              // Execution latency
}


struct VertexProps {
    1: i32 tagId,
    2: binary props,
}


struct Vertex {
    1: i64 id,
    2: list<VertexProps> props,
}


struct EdgeKey {
    1: i64 src,
    2: i64 dst,
    // When edgeType > 0, it's an out-edge, otherwise, it's an in-edge
    3: i32 edgeType,
    4: i64 ranking,
}


struct Edge {
    1: EdgeKey key,
    2: binary props,
}


struct GetNeighborsRequest {
    // shard_id => ids
    1: map<i32, list<i64>> ids,
    // When edgeType > 0, going along the out-edge, otherwise, along the in-edge
    2: i32 edgeType,
    3: binary filter,
    4: list<PropDef> returnColumnn,
}


struct NeighborsStatsRequest {
    // shard_id => ids
    1: map<i32, list<i64>> ids,
    // When edgeType > 0, going along the out-edge, otherwise, along the in-edge
    2: i32 edgeType,
    3: binary filter,
    4: list<PropStat> returnColumnn,
}


struct VertexPropRequest {
    1: map<i32, list<i64>> ids,
    2: list<PropDef> returnColumns,
}


struct EdgePropRequest {
    // shard_id => edges
    1: map<i32, list<EdgeKey>> edges
    5: list<PropDef> returnColumns,
}


service StorageService {
    QueryResponse getOutBound(1: GetNeighborsRequest req)
    QueryResponse getInBound(1: GetNeighborsRequest req)

    QueryResponse outBoundStats(1: NeighborsStatsRequest req)
    QueryResponse inBoundStats(1: NeighborsStatsRequest req)

    // When returnColumns is empty, return all properties
    QueryResponse getProps(1: VertexPropRequest req);
    QueryResponse getEdgeProps(1: EdgePropRequest req)

    ExecResponseList addVertices(1: list<Vertex> vertices);
    ExecResponse addEdges(1: list<Edge> edges);

    ExecResponseList updateVertices(1: list<Vertex> vertices);
    ExecResponse updateEdges(1: list<Edge> edges);
}

