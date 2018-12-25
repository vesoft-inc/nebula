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

    // meta failures
    E_EDGE_PROP_NOT_FOUND = -21,
    E_TAG_PROP_NOT_FOUND = -22,
    E_IMPROPER_DATA_TYPE = -23,

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
    3: string name,      // Property name
    4: StatType stat,    // calc stats when setted.
}

enum StatType {
    SUM = 1,
    COUNT = 2,
    AVG = 3,
} (cpp.enum_strict)

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

struct VertexResponse {
    1: i64 vertex_id,
    2: binary vertex_data, // decode according to vertex_schema.
    3: binary edge_data,   // decode according to edge_schema.
}

struct QueryResponse {
    1: required list<ResultCode> codes,
    2: required i32 latency_in_ms,      // Query latency from storage service
    3: optional Schema vertex_schema,   // vertex related props
    4: optional Schema edge_schema,     // edge related props
    5: optional list<VertexResponse> vertices,
}

struct ExecResponse {
    1: required list<ResultCode> codes,
    2: required i32 latency_in_ms,      // Execution latency
}

struct EdgePropResponse {
    1: required list<ResultCode> codes,
    2: required i32 latency_in_ms,      // Query latency from storage service
    3: optional Schema schema,          // edge related props
    4: optional binary data,
}

struct QueryStatsResponse {
    // If error for the whole request, the codes would have only one element and 
    // related part_id equals -1
    1: required list<ResultCode> codes,
    2: required i32 latency_in_ms,      // Query latency from storage service
    3: optional Schema schema,
    4: optional binary data,
}

struct Tag {
    1: i32 tag_id,
    2: binary props,
}

struct Vertex {
    1: i64 id,
    2: list<Tag> tags,
}

struct EdgeKey {
    1: i64 src,
    // When edge_type > 0, it's an out-edge, otherwise, it's an in-edge
    // When query edge props, the field could be unset.
    2: i32 edge_type,
    3: i64 dst,
    4: i64 ranking,
}

struct Edge {
    1: EdgeKey key,
    2: binary props,
}

struct GetNeighborsRequest {
    1: GraphSpaceID space_id,
    // partId => ids
    2: map<PartitionID, list<i64>>(cpp.template = "std::unordered_map") ids,
    // When edge_type > 0, going along the out-edge, otherwise, along the in-edge
    3: i32 edge_type,
    4: binary filter,
    5: list<PropDef> return_columns,
}

struct VertexPropRequest {
    1: GraphSpaceID space_id,
    2: map<PartitionID, list<i64>>(cpp.template = "std::unordered_map") ids,
    3: list<PropDef> return_columns,
}

struct EdgePropRequest {
    1: GraphSpaceID space_id,
    // partId => edges
    2: map<PartitionID, list<EdgeKey>>(cpp.template = "std::unordered_map") edges,
    3: i32 edge_type,
    4: list<PropDef> return_columns,
}

struct AddVerticesRequest {
    1: GraphSpaceID space_id,
    // partId => vertices
    2: map<PartitionID, list<Vertex>>(cpp.template = "std::unordered_map") vertices,
    // If true, it equals an upsert operation.
    3: bool overwritable,
}

struct AddEdgesRequest {
    1: GraphSpaceID space_id,
    // partId => edges
    2: map<PartitionID, list<Edge>>(cpp.template = "std::unordered_map") edges,
    // If true, it equals an upsert operation.
    3: bool overwritable,
}

service StorageService {
    QueryResponse getOutBound(1: GetNeighborsRequest req)
    QueryResponse getInBound(1: GetNeighborsRequest req)

    QueryStatsResponse outBoundStats(1: GetNeighborsRequest req)
    QueryStatsResponse inBoundStats(1: GetNeighborsRequest req)

    // When return_columns is empty, return all properties
    QueryResponse getProps(1: VertexPropRequest req);
    EdgePropResponse getEdgeProps(1: EdgePropRequest req)

    ExecResponse addVertices(1: AddVerticesRequest req);
    ExecResponse addEdges(1: AddEdgesRequest req);
}

