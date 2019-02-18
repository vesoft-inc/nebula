/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

namespace cpp nebula.storage
namespace java nebula.storage
namespace go nebula.storage

include "common.thrift"

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


enum PropOwner {
    SOURCE = 1,
    DEST = 2,
    EDGE = 3,
} (cpp.enum_strict)

struct PropDef {
    1: PropOwner owner,
    2: common.TagID tag_id,       // Only valid when owner is SOURCE or DEST
    3: string name,      // Property name
    4: StatType stat,    // calc stats when setted.
}

enum StatType {
    SUM = 1,
    COUNT = 2,
    AVG = 3,
} (cpp.enum_strict)

struct ResultCode {
    1: required ErrorCode code,
    2: required common.PartitionID part_id,
    // Only valid when code is E_LEADER_CHANAGED.
    3: optional common.HostAddr  leader,
}

struct VertexData {
    1: common.VertexID vertex_id,
    2: binary vertex_data, // decode according to vertex_schema.
    3: binary edge_data,   // decode according to edge_schema.
}

struct ResponseCommon {
    // Only contains the partition that returns error
    1: required list<ResultCode> failed_codes,
    // Query latency from storage service
    2: required i32 latency_in_ms,
}

struct QueryResponse {
    1: required ResponseCommon result,
    2: optional common.Schema vertex_schema,   // vertex related props
    3: optional common.Schema edge_schema,     // edge related props
    4: optional list<VertexData> vertices,
}

struct ExecResponse {
    1: required ResponseCommon result,
}

struct EdgePropResponse {
    1: required ResponseCommon result,
    2: optional common.Schema schema,          // edge related props
    3: optional binary data,
}


struct QueryStatsResponse {
    1: required ResponseCommon result,
    2: optional common.Schema schema,
    3: optional binary data,
}

struct Tag {
    1: common.TagID tag_id,
    2: binary props,
}

struct Vertex {
    1: common.VertexID id,
    2: list<Tag> tags,
}

struct EdgeKey {
    1: common.VertexID src,
    // When edge_type > 0, it's an out-edge, otherwise, it's an in-edge
    // When query edge props, the field could be unset.
    2: common.EdgeType edge_type,
    3: common.VertexID dst,
    4: common.EdgeRanking ranking,
}

struct Edge {
    1: EdgeKey key,
    2: binary props,
}

struct GetNeighborsRequest {
    1: common.GraphSpaceID space_id,
    // partId => ids
    2: map<common.PartitionID, list<common.VertexID>>(cpp.template = "std::unordered_map") parts,
    // When edge_type > 0, going along the out-edge, otherwise, along the in-edge
    3: common.EdgeType edge_type,
    4: binary filter,
    5: list<PropDef> return_columns,
}

struct VertexPropRequest {
    1: common.GraphSpaceID space_id,
    2: map<common.PartitionID, list<common.VertexID>>(cpp.template = "std::unordered_map") parts,
    3: list<PropDef> return_columns,
}

struct EdgePropRequest {
    1: common.GraphSpaceID space_id,
    // partId => edges
    2: map<common.PartitionID, list<EdgeKey>>(cpp.template = "std::unordered_map") parts,
    3: common.EdgeType edge_type,
    4: list<PropDef> return_columns,
}

struct AddVerticesRequest {
    1: common.GraphSpaceID space_id,
    // partId => vertices
    2: map<common.PartitionID, list<Vertex>>(cpp.template = "std::unordered_map") parts,
    // If true, it equals an upsert operation.
    3: bool overwritable,
}

struct AddEdgesRequest {
    1: common.GraphSpaceID space_id,
    // partId => edges
    2: map<common.PartitionID, list<Edge>>(cpp.template = "std::unordered_map") parts,
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

