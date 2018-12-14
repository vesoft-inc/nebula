/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

namespace cpp vesoft.storage
namespace java vesoft.storage
namespace go vesoft.storage

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


struct Edge {
    1: i64 src,
    2: i64 dst,
    3: i32 edgeType,
    4: i64 ranking,
    5: binary props,
}


service StorageService {
    QueryResponse getOutBound(1: list<i64> ids,
                              2: i32 edgeType,
                              3: binary filter
                              4: list<PropDef> returnColumns)
    QueryResponse getInBound(1: list<i64> ids,
                             2: i32 edgeType,
                             3: binary filter
                             4: list<PropDef> returnColumns)

    QueryResponse outBoundStats(1: list<i64> ids,
                                2: i32 edgeType,
                                3: binary filter
                                4: list<PropStat> returnColumns)
    QueryResponse inBoundStats(1: list<i64> ids,
                               2: i32 edgeType,
                               3: binary filter
                               4: list<PropStat> returnColumns)

    // When returnColumns is empty, return all properties
    QueryResponse getProps(1: list<i64> ids,
                           2: list<PropDef> returnColumns)
    QueryResponse getEdgeProps(1: i64 src,
                               2: i64 dst,
                               3: i32 edgeType,
                               4: i64 ranking,
                               5: list<PropDef> returnColumns)

    ExecResponseList addVertices(1: list<Vertex> vertices);
    ExecResponse addEdges(1: list<Edge> edges);

    ExecResponseList updateVertices(1: list<Vertex> vertices);
    ExecResponse updateEdges(1: list<Edge> edges);
}

