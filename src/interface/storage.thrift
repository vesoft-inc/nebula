/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

namespace cpp nebula.storage
namespace java nebula.storage
namespace go nebula.storage

cpp_include "base/ThriftTypes.h"

typedef i32 (cpp.type = "nebula::GraphSpaceID") GraphSpaceID
typedef i32 (cpp.type = "nebula::PartitionID") PartitionID
typedef i32 (cpp.type = "nebula::IPv4") IPv4
typedef i32 (cpp.type = "nebula::Port") Port

enum ErrorCode {
    SUCCEEDED = 0,

    // RPC Failure
    E_DISCONNECTED = -1,
    E_FAILED_TO_CONNECT = -2,
    E_RPC_FAILURE = -3,

    // storage failures
    E_LEADER_CHANGED = -11,
    E_KEY_HAS_EXISTS = -12,
    E_SPACE_NOT_FOUND = -13,
    E_PART_NOT_FOUND = -14,

    E_UNKNOWN = -100,
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
    2: i32 tag_id,       // Only valid when owner is SOURCE or DEST
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

struct HostAddr {
    1: IPv4  ip,
    2: Port  port,
}

struct ResultCode {
    1: required ErrorCode code,
    2: required PartitionID part_id,
    // Only valid when code is E_LEADER_CHANAGED.
    3: optional HostAddr  leader,
}

struct QueryResponse {
    1: required list<ResultCode> codes,
    2: required i32 latency_in_ms,      // Query latency from storage service
    3: optional Schema schema,
    4: optional binary data,            // Encoded data
}

struct ExecResponse {
    1: required list<ResultCode> codes,
    2: required i32 latency_in_ms,      // Execution latency
}

struct VertexProps {
    1: i32 tag_id,
    2: binary props,
}

struct Vertex {
    1: i64 id,
    2: list<VertexProps> props,
}

struct EdgeKey {
    1: i64 src,
    2: i64 dst,
    // When edge_type > 0, it's an out-edge, otherwise, it's an in-edge
    3: i32 edge_type,
    4: i64 ranking,
}

struct Edge {
    1: EdgeKey key,
    2: binary props,
}

struct GetNeighborsRequest {
    1: GraphSpaceID space_id,
    // partId => ids
    2: map<PartitionID, list<i64>> ids,
    // When edge_type > 0, going along the out-edge, otherwise, along the in-edge
    3: i32 edge_type,
    4: binary filter,
    5: list<PropDef> return_columns,
}

struct NeighborsStatsRequest {
    1: GraphSpaceID space_id,
    // partId => ids
    2: map<PartitionID, list<i64>> ids,
    // When edge_type > 0, going along the out-edge, otherwise, along the in-edge
    3: i32 edge_type,
    4: binary filter,
    5: list<PropStat> return_columns,
}

struct VertexPropRequest {
    1: GraphSpaceID space_id,
    2: map<PartitionID, list<i64>> ids,
    3: list<PropDef> return_columns,
}

struct EdgePropRequest {
    1: GraphSpaceID space_id,
    // partId => edges
    2: map<PartitionID, list<EdgeKey>> edges,
    3: list<PropDef> return_columns,
}

struct AddVerticesRequest {
    1: GraphSpaceID space_id,
    // partId => vertices
    2: map<PartitionID, list<Vertex>> vertices,
    // If true, it equals an upsert operation.
    3: bool overwritable,
}

struct AddEdgesRequest {
    1: GraphSpaceID space_id,
    // partId => edges
    2: map<PartitionID, list<Edge>> edges,
    // If true, it equals an upsert operation.
    3: bool overwritable,
}

service StorageService {
    QueryResponse getOutBound(1: GetNeighborsRequest req)
    QueryResponse getInBound(1: GetNeighborsRequest req)

    QueryResponse outBoundStats(1: NeighborsStatsRequest req)
    QueryResponse inBoundStats(1: NeighborsStatsRequest req)

    // When return_columns is empty, return all properties
    QueryResponse getProps(1: VertexPropRequest req);
    QueryResponse getEdgeProps(1: EdgePropRequest req)

    ExecResponse addVertices(1: AddVerticesRequest req);
    ExecResponse addEdges(1: AddEdgesRequest req);
}

