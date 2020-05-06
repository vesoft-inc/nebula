/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace cpp nebula.storage
namespace java com.vesoft.nebula.storage
namespace go nebula.storage

include "common.thrift"
include "meta.thrift"

/*
 *
 *  Note: In order to support multiple languages, all strings
 *        have to be defined as **binary** in the thrift file
 *
 */

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
    E_KEY_NOT_FOUND = -15,
    E_CONSENSUS_ERROR = -16,

    // meta failures
    E_EDGE_PROP_NOT_FOUND = -21,
    E_TAG_PROP_NOT_FOUND = -22,
    E_IMPROPER_DATA_TYPE = -23,
    E_EDGE_NOT_FOUND = -24,
    E_TAG_NOT_FOUND = -25,

    // Invalid request
    E_INVALID_FILTER = -31,
    E_INVALID_UPDATER = -32,
    E_INVALID_STORE = -33,
    E_INVALID_PEER  = -34,
    E_RETRY_EXHAUSTED = -35,
    E_TRANSFER_LEADER_FAILED = -36,
    E_INVALID_STAT_TYPE = -37,

    // meta client failed
    E_LOAD_META_FAILED = -41,

    // checkpoint failed
    E_FAILED_TO_CHECKPOINT = -50,
    E_CHECKPOINT_BLOCKED = -51,

    // partial result, used for kv interfaces
    E_PARTIAL_RESULT = -61,

    // task manager failed
    E_INVALID_TASK_PARA = -90,
    E_USER_CANCEL = -99,

    E_UNKNOWN = -100,
} (cpp.enum_strict)


struct PartitionResult {
    1: required ErrorCode           code,
    2: required common.PartitionID  part_id,
    // Only valid when code is E_LEADER_CHANAGED.
    3: optional common.HostAddr     leader,
}


struct ResponseCommon {
    // Only contains the partition that returns error
    1: required list<PartitionResult>   failed_parts,
    // Query latency from storage service
    2: required i32                     latency_in_us,
}


/*
 *
 * Common types used by all services
 *
 */
// Enumeration of the statistic methods
enum StatType {
    SUM = 1,
    COUNT = 2,
    AVG = 3,
    MAX = 4,
    MIN = 5,
} (cpp.enum_strict)


// Define a statistic properties
struct StatProp {
    // An eperssion. In most of cases, it is a reference to a specific property
    1: binary           prop,
    // Stats method
    2: StatType         stat,
}


enum OrderDirection {
    ASCENDING = 1,
    DESCENDING = 2,
}


struct OrderBy {
    // An expression which result will be used to sort
    1: binary           prop,
    2: OrderDirection   direction,
}


enum EdgeDirection {
    BOTH = 1,
    IN_EDGE = 2,
    OUT_EDGE = 3,
}


///////////////////////////////////////////////////////////
//
//  Requests, responses for the GraphStorageService
//
///////////////////////////////////////////////////////////

/*
 * Start of GetNeighbors section
 */
struct GetNeighborsRequest {
    1: common.GraphSpaceID                      space_id,
    // Column names for input data. The first column name must be "_vid"
    2: list<binary>                             column_names,
    // partId => rows
    3: map<common.PartitionID, list<common.Row>>
        (cpp.template = "std::unordered_map")   parts,
    // When edge_type > 0, going along the out-edge, otherwise, along the in-edge
    // If the edge type list is empty, all edges will be scaned
    4: list<common.EdgeType>                    edge_types,
    // When above edge_types is not empty, edge_direction should be ignored
    // When edge_types is empty, edge_direction decided which edge types will be
    //   followed. The default value is BOTH, which means both in-edges and out-edges
    //   will be traversed. If the edge_direction is IN_EDGE, then only the in-edges
    //   will be traversed. OUT_EDGE indicates only the out-edges will be traversed
    5: EdgeDirection                            edge_direction = EdgeDirection.BOTH,
    // Whether to do the dedup based on the entire row. The dedup will be done on the
    //   neighbors of each vertex
    6: bool                                     dedup = false,

    7: optional list<StatProp>                  stat_props,
    // A list of expressions which can only refer to the vertex proerties. If the list
    //   is not given, no prop will be returned. If an empty prop list is given, all
    //   properties will be returned. If the property name is the wild card "*", all
    //   properties for the given tag/edge type will be returned.
    8: optional list<binary>                    vertex_props,
    9: optional list<binary>                    edge_props,
    // A list of expressions used to sort the result
    10: optional list<OrderBy>                  order_by,
    // Return the top/bottom N rows for each given vertex
    11: optional i64                            limit,
    // If provided, only the rows satified the given expression will be returned
    12: optional binary                         filter,
}


struct GetNeighborsResponse {
    1: required ResponseCommon result,
    // The result will be returned in a dataset, which is in the following form
    //
    // Each row represents one source vertex and its neighbors
    // The name of the first column is "_vid", it's the ID of the source vertex
    // If stat_props is specfied in the request, the second column will be the result
    //   of the stats. The column name is "_stats", it's the list of Values for
    //   the stats properties, in the order specified as GetNeighborsRequest::stat_props.
    // After the stats column, it's the vertex property if vertex_props is specified.
    //   If the vertex_props is not empty, the column name will be the one specified in
    //   the vertex_props. Otherwise, it is "tag:<tag_name>". The value is a DataSet.
    //   If the vertex does NOT have the given tag, the value will be a NULL
    // The column following the vertex properties are edge properties if edge_props is
    //   specified. If edge_props is not empty, the column is the one specified in the
    //   edg_props. Otherwise, the column name is the "edge:<type_name>". The value is
    //   a DataSet.
    2: optional common.DataSet vertices,
}
/*
 * End of GetNeighbors section
 */


//
// Response for data modification requests
//
struct ExecResponse {
    1: required ResponseCommon result,
}


/*
 * Start of GetProp section
 */
struct GetPropRequest {
    1: common.GraphSpaceID                      space_id,
    // Column names for the pass-in data. When getting the vertex props, the first
    //   column has to be "_vid", when getting the edge props, the first four columns
    //   have to be "_src", "_type", "_ranking", and "_dst"
    2: list<binary>                             column_names,
    3: map<common.PartitionID, list<common.Row>>
        (cpp.template = "std::unordered_map")   parts,
    // A list of expressions. In most of cases, they refer to the properties.
    //   If the list is empty, return all properties. If the property name
    //   is the wild card "*", all properties for the given tag/edge type will be
    //   returned
    4: list<binary>                             props,
    // Whether to do the dedup based on the entire row
    5: bool                                     dedup = false,
    // List of expressions used by the order-by clause
    6: optional list<OrderBy>                   order_by,
    7: optional i64                             limit,
    // If a filter is provided, only vertices that are satisfied the filter
    // will be returned
    8: optional binary                          filter,
}


struct GetPropResponse {
    1: ResponseCommon           result,
    // The result will be returned in a dataset, which is in the following form
    //
    // Each row represents one vertex's or one edge's properties, one column per
    //   peoperty. If the list vertex_props or edge_props in the request is not
    //   empty, the expression string will be used as the column name. Otherwise,
    //   the column name is in the form of "<tag_name>:<prop_name>" or
    //   "<edge_type_name>:<prop_name>". If the vertex does NOT have the given
    //   property, the value will be a NULL
    2: optional common.DataSet  props,
}
/*
 * End of GetProp section
 */


/*
 * Start of AddVertices section
 */
struct NewTag {
    1: common.TagID         tag_id,
    2: list<common.Value>   props,
}


struct NewVertex {
    1: common.VertexID id,
    2: list<NewTag> tags,
}


struct EdgeKey {
    1: common.VertexID      src,
    // When edge_type > 0, it's an out-edge, otherwise, it's an in-edge
    // When query edge props, the field could be unset.
    2: common.EdgeType      edge_type,
    3: common.EdgeRanking   ranking,
    4: common.VertexID      dst,
}


struct NewEdge {
    1: EdgeKey              key,
    2: list<common.Value>   props,
}


struct AddVerticesRequest {
    1: common.GraphSpaceID                      space_id,
    // partId => vertices
    2: map<common.PartitionID, list<NewVertex>>
        (cpp.template = "std::unordered_map")   parts,
    // A map from TagID -> list of prop_names
    // The order of the property names should match the data order specified
    //   in the NewVertex.NewTag.props
    3: map<common.TagID, list<binary>>
        (cpp.template = "std::unordered_map")   prop_names,
    // If true, it equals an (up)sert operation.
    4: bool                                     overwritable = true,
}

struct AddEdgesRequest {
    1: common.GraphSpaceID                      space_id,
    // partId => edges
    2: map<common.PartitionID, list<NewEdge>>(
        cpp.template = "std::unordered_map")    parts,
    // A list of property names. The order of the property names should match
    //   the data order specified in the NewEdge.props
    3: list<binary>                             prop_names,
    // If true, it equals an upsert operation.
    4: bool                                     overwritable = true,
}
/*
 * End of AddVertices section
 */


/*
 * Start of DeleteVertex section
 */
struct DeleteVerticesRequest {
    1: common.GraphSpaceID                              space_id,
    // partId => vertexId
    2: map<common.PartitionID, list<common.VertexID>>
        (cpp.template = "std::unordered_map")           parts,
}


struct DeleteEdgesRequest {
    1: common.GraphSpaceID                      space_id,
    // partId => edgeKeys
    2: map<common.PartitionID, list<EdgeKey>>
        (cpp.template = "std::unordered_map")   parts,
}
/*
 * End of DeleteVertex section
 */


// Response for update requests
struct UpdateResponse {
    1: required ResponseCommon      result,
    // The result will be returned in a dataset, which is in the following form
    //
    // The name of the first column is "_inserted". It has a boolean value. It's
    //   TRUE if insertion happens
    // Starting from the second column, it's the all returned properties, one column
    //   per peoperty. If there is no given property, the value will be a NULL
    2: optional common.DataSet      props,
}


/*
 * Start of UpdateVertex section
 */
struct UpdatedVertexProp {
    1: required common.TagID    tag_id,     // the Tag ID
    2: required binary          name,       // property name
    3: required binary          value,      // new value (encoded expression)
}


struct UpdateVertexRequest {
    1: common.GraphSpaceID          space_id,
    2: common.PartitionID           part_id,
    3: common.VertexID              vertex_id,
    4: list<UpdatedVertexProp>      updated_props,
    5: optional bool                insertable = false,
    // A list of expressions
    6: optional list<binary>        return_props,
    // If provided, the update happens only when the condition evaluates true
    7: optional binary              condition,
}
/*
 * End of UpdateVertex section
 */


/*
 * Start of UpdateEdge section
 */
struct UpdatedEdgeProp {
    1: required binary          name,       // property name
    2: required binary          value,      // new value (encoded expression)
}


struct UpdateEdgeRequest {
    1: common.GraphSpaceID      space_id,
    2: common.PartitionID       part_id,
    3: EdgeKey                  edge_key,
    4: list<UpdatedEdgeProp>    updated_props,
    5: optional bool            insertable = false,
    // A list of expressions
    6: optional list<binary>    return_props,
    // If provided, the update happens only when the condition evaluates true
    7: optional binary          condition,
}
/*
 * End of UpdateEdge section
 */


/*
 * Start of GetUUID section
 */
struct GetUUIDReq {
    1: common.GraphSpaceID  space_id,
    2: common.PartitionID   part_id,
    3: binary               name,
}


struct GetUUIDResp {
    1: required ResponseCommon result,
    2: common.VertexID id,
}
/*
 * End of GetUUID section
 */


/*
 * Start of Index section
 */
struct LookUpIndexResp {
    1: required ResponseCommon          result,
    // The result will be returned in a dataset, which is in the following form
    //
    // When looking up the vertex index, each row represents one vertex and its
    //   properties; when looking up the edge index, each row represents one edge
    //   and its properties.
    //
    // When returning the data for the vertex index, it follows this convention:
    // 1. The name of the first column is "_vid", it's the ID of the vertex
    // 2. Starting from the second column, it's the vertex property, one column
    //    per peoperty. The column name is in the form of "tag_name.prop_name".
    //    If the vertex does NOT have the given property, the value will be a NULL
    //
    // When returning the data for the edge index, it follows this convention:
    // 1. The name of the first column is "_src", it's the ID of the source vertex
    // 2. The name of the second column is "_type", it's the edge type
    // 3. The name of the third column is "_ranking", it's the edge ranking
    // 4. The name of the fource column is "_dst", it's the ID of the destination
    //    vertex
    // 5. Starting from the fifth column, it's the edge property, one column per
    //    peoperty. The column name is in the form of "edge_type_name.prop_name".
    //    If the vertex does NOT have the given property, the value will be a NULL
    2: optional common.DataSet          data,
}

struct LookUpIndexRequest {
    1: common.GraphSpaceID      space_id,
    2: list<common.PartitionID> parts,
    3: common.IndexID           index_id,
    4: binary                   filter,
    // We only support specified fields here, not wild card "*"
    // If the list is empty, only the VertexID or the EdgeKey will
    // be returned
    5: list<binary>             return_columns,
}
/*
 * End of GetUUID section
 */

struct TaskPara {
    1: common.GraphSpaceID                  space_id,
    2: optional list<common.PartitionID>    parts,
    3: optional list<binary>                task_specfic_paras
}

struct AddAdminTaskRequest {
    1: meta.AdminCmd                        cmd        // rebuild index / flush / compact
    2: i32                                  job_id
    3: i32                                  task_id
    4: TaskPara                             para
    5: optional i32                         concurrency
}

struct StopAdminTaskRequest {
    1: i32                                  job_id
    2: i32                                  task_id
}

service GraphStorageService {
    GetNeighborsResponse getNeighbors(1: GetNeighborsRequest req)

    // Get vertex or edge properties
    GetPropResponse getProps(1: GetPropRequest req);

    ExecResponse addVertices(1: AddVerticesRequest req);
    ExecResponse addEdges(1: AddEdgesRequest req);

    ExecResponse deleteEdges(1: DeleteEdgesRequest req);
    ExecResponse deleteVertices(1: DeleteVerticesRequest req);

    UpdateResponse updateVertex(1: UpdateVertexRequest req);
    UpdateResponse updateEdge(1: UpdateEdgeRequest req);

    GetUUIDResp getUUID(1: GetUUIDReq req);

    // Interfaces for edge and vertex index scan
    LookUpIndexResp lookUpVertexIndex(1: LookUpIndexRequest req);
    LookUpIndexResp lookUpEdgeIndex(1: LookUpIndexRequest req);
}


//////////////////////////////////////////////////////////
//
//  Requests, responses for the StorageAdminService
//
//////////////////////////////////////////////////////////
// Common response for admin methods
struct AdminExecResp {
    1: required ResponseCommon result,
}


struct TransLeaderReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: common.HostAddr     new_leader,
}


struct AddPartReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: bool                as_learner,
}


struct AddLearnerReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: common.HostAddr     learner,
}


struct RemovePartReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
}


struct MemberChangeReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: common.HostAddr     peer,
    // true means add a peer, false means remove a peer.
    4: bool                add,
}


struct CatchUpDataReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: common.HostAddr     target,
}

struct GetLeaderReq {
}

struct CreateCPRequest {
    1: common.GraphSpaceID  space_id,
    2: binary               name,
}


struct DropCPRequest {
    1: common.GraphSpaceID  space_id,
    2: binary               name,
}


enum EngineSignType {
    BLOCK_ON = 1,
    BLOCK_OFF = 2,
}


struct BlockingSignRequest {
    1: common.GraphSpaceID      space_id,
    2: required EngineSignType  sign,
}


struct GetLeaderPartsResp {
    1: required ResponseCommon result,
    2: map<common.GraphSpaceID, list<common.PartitionID>> (
        cpp.template = "std::unordered_map") leader_parts;
}


struct CheckPeersReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: list<common.HostAddr> peers,
}


struct RebuildIndexRequest {
    1: common.GraphSpaceID          space_id,
    2: list<common.PartitionID>     parts,
    3: common.IndexID               index_id,
    4: bool                         is_offline,
}


service StorageAdminService {
    // Interfaces for admin operations
    AdminExecResp transLeader(1: TransLeaderReq req);
    AdminExecResp addPart(1: AddPartReq req);
    AdminExecResp addLearner(1: AddLearnerReq req);
    AdminExecResp removePart(1: RemovePartReq req);
    AdminExecResp memberChange(1: MemberChangeReq req);
    AdminExecResp waitingForCatchUpData(1: CatchUpDataReq req);

    // Interfaces for nebula cluster checkpoint
    AdminExecResp createCheckpoint(1: CreateCPRequest req);
    AdminExecResp dropCheckpoint(1: DropCPRequest req);
    AdminExecResp blockingWrites(1: BlockingSignRequest req);

    // Interfaces for rebuild index
    AdminExecResp rebuildTagIndex(1: RebuildIndexRequest req);
    AdminExecResp rebuildEdgeIndex(1: RebuildIndexRequest req);

    // Return all leader partitions on this host
    GetLeaderPartsResp getLeaderParts(1: GetLeaderReq req);
    // Return all peers
    AdminExecResp checkPeers(1: CheckPeersReq req);

    AdminExecResp addAdminTask(1: AddAdminTaskRequest req);
    AdminExecResp stopAdminTask(1: StopAdminTaskRequest req);
}


//////////////////////////////////////////////////////////
//
//  Requests, responses for the GeneralStorageService
//
//////////////////////////////////////////////////////////
struct KVGetRequest {
    1: common.GraphSpaceID space_id,
    2: map<common.PartitionID, list<binary>>(
        cpp.template = "std::unordered_map") parts,
    // When return_partly is true and some of the keys not found, will return the keys
    // which exist
    3: bool return_partly
}


struct KVGetResponse {
    1: required ResponseCommon result,
    2: map<binary, binary>(cpp.template = "std::unordered_map") key_values,
}


struct KVPutRequest {
    1: common.GraphSpaceID space_id,
    // part -> key/value
    2: map<common.PartitionID, list<common.KeyValue>>(
        cpp.template = "std::unordered_map") parts,
}


struct KVRemoveRequest {
    1: common.GraphSpaceID space_id,
    // part -> key
    2: map<common.PartitionID, list<binary>>(
        cpp.template = "std::unordered_map") parts,
}


struct KeyRange {
    1: binary start,
    2: binary end,
}


struct KVRemoveRangeRequest {
    1: common.GraphSpaceID space_id,
    2: map<common.PartitionID, list<KeyRange>>(
        cpp.template = "std::unordered_map") parts,
}


service GeneralStorageService {
    // Interfaces for key-value storage
    KVGetResponse   get(1: KVGetRequest req);
    ExecResponse    put(1: KVPutRequest req);
    ExecResponse    remove(1: KVRemoveRequest req);
    ExecResponse    removeRange(1: KVRemoveRangeRequest req);
}

