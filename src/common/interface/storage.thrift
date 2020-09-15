/* vim: ft=proto
 * Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace cpp nebula.storage
namespace java com.vesoft.nebula.storage
namespace go nebula.storage
namespace csharp nebula.storage
namespace js nebula.storage
namespace py nebula2.storage

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
    E_LEADER_CHANGED       = -11,
    E_KEY_HAS_EXISTS       = -12,
    E_SPACE_NOT_FOUND      = -13,
    E_PART_NOT_FOUND       = -14,
    E_KEY_NOT_FOUND        = -15,
    E_CONSENSUS_ERROR      = -16,
    E_DATA_TYPE_MISMATCH   = -17,
    E_INVALID_FIELD_VALUE  = -18,
    E_REBUILD_INDEX_FAILED = -19,
    E_INVALID_OPERATION    = -20,
    E_NOT_NULLABLE         = -21,   // Not allowed to be null
    E_FIELD_UNSET          = -22,   // The field neither can be NULL, nor has a default value
    E_OUT_OF_RANGE         = -23,   // Value exceeds the range of type
    E_ATOMIC_OP_FAILED     = -24,   // Atomic operation failed

    // meta failures
    E_EDGE_PROP_NOT_FOUND    = -31,
    E_TAG_PROP_NOT_FOUND     = -32,
    E_IMPROPER_DATA_TYPE     = -33,
    E_EDGE_NOT_FOUND         = -34,
    E_TAG_NOT_FOUND          = -35,
    E_INVALID_SPACEVIDLEN    = -36,
    E_INDEX_NOT_FOUND        = -37,

    // Invalid request
    E_INVALID_FILTER         = -41,
    E_INVALID_UPDATER        = -42,
    E_INVALID_STORE          = -43,
    E_INVALID_PEER           = -44,
    E_RETRY_EXHAUSTED        = -45,
    E_TRANSFER_LEADER_FAILED = -46,
    E_INVALID_STAT_TYPE      = -47,
    E_INVALID_VID            = -48,

    // meta client failed
    E_LOAD_META_FAILED       = -51,

    // checkpoint failed
    E_FAILED_TO_CHECKPOINT   = -60,
    E_CHECKPOINT_BLOCKED     = -61,

    // partial result, used for kv interfaces
    E_PARTIAL_RESULT         = -71,

    // Filter out
    E_FILTER_OUT             = -81,
    E_INVALID_DATA           = -82,

    // task manager failed
    E_INVALID_TASK_PARA      = -90,
    E_USER_CANCEL            = -99,

    E_UNKNOWN               = -100,
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


// Define a statistic calculator for each source vertex
struct StatProp {
    // Alias of the stats property
    1: binary           alias,
    // An eperssion. In most of cases, it is a reference to a specific property
    2: binary           prop,
    // Stats method
    3: StatType         stat,
}


// Define an expression
struct Expr {
    // Alias of the expression
    1: binary           alias,
    // An eperssion. It could be any valid expression,
    2: binary           expr,
}


// Define an edge property
struct EdgeProp {
    // A valid edge type
    1: common.EdgeType  type,
    // The list of property names. If it is empty, then all properties on
    // the given edge type will be returned
    2: list<binary>     props,
}


// Define a vertex property
struct VertexProp {
    // A valid tag id
    1: common.TagID tag,
    // The list of property names. If it is empty, then all properties on
    // the given tag will be returned
    2: list<binary> props,
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

struct TraverseSpec {
    // When edge_type > 0, going along the out-edge, otherwise, along the in-edge
    // If the edge type list is empty, all edges will be scaned
    1: list<common.EdgeType>                    edge_types,
    // When above edge_types is not empty, edge_direction should be ignored
    // When edge_types is empty, edge_direction decided which edge types will be
    //   followed. The default value is BOTH, which means both in-edges and out-edges
    //   will be traversed. If the edge_direction is IN_EDGE, then only the in-edges
    //   will be traversed. OUT_EDGE indicates only the out-edges will be traversed
    2: EdgeDirection                            edge_direction = EdgeDirection.BOTH,
    // Whether to do the dedup based on the entire row. The dedup will be done on the
    //   neighbors of each vertex
    3: bool                                     dedup = false,

    4: optional list<StatProp>                  stat_props,
    // A list of source vertex properties to be returned. If the list is not given,
    //   no prop will be returned. If an empty prop list is given, all properties
    //   will be returned.
    5: optional list<VertexProp>                vertex_props,
    // A list of edge properties to be returned. If the list is not given,
    //   no prop will be returned. If an empty prop list is given, all edge properties
    //   will be returned.
    6: optional list<EdgeProp>                  edge_props,
    // A list of expressions which are evaluated on each edge
    7: optional list<Expr>                      expressions,
    // A list of expressions used to sort the result
    8: optional list<OrderBy>                   order_by,
    // Combined with "limit", the random flag makes the result of each query different
    9: optional bool                            random,
    // Return the top/bottom N rows for each given vertex
    10: optional i64                            limit,
    // If provided, only the rows satified the given expression will be returned
    11: optional binary                         filter,
}


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
    4: TraverseSpec                             traverse_spec;
}


struct GetNeighborsResponse {
    1: required ResponseCommon result,
    // The result will be returned in a dataset, which is in the following form
    //
    // Each row represents one source vertex and its neighbors
    //
    // Here is the layout of the dataset
    // ===========================================================================================
    // |  _vid  | stats_column | tag_column  | ... |    edge_column    | ... |    expr_column    |
    // |=========================================================================================|
    // | string |  list<Value> | list<Value> | ... | list<list<Value>> | ... | list<list<Value>> |
    // |-----------------------------------------------------------------------------------------|
    // | .....                                                                                   |
    // ===========================================================================================
    //
    // The first column in the dataset stores the source vertex id. The column name
    //   is "_vid". The value should be type of string
    //
    // The second column contains the statistic result for the vertex if stat_props
    //   in the request is not empty. The column name is in the following format
    //
    //   "_stats:<alias1>:<alias2>:..."
    //
    // Basically the column name contains the statistic alias names, separated by
    //   the ":". The value in this column is a list of Values, which are corresponding
    //   to the properties specified in the column name.
    //
    // Starting from the third column, source vertex properties will be returned if the
    //   GetNeighborsRequest::vertex_props is not empty. Each column contains properties
    //   from one tag. The column name will be in the following format
    //
    //   "_tag:<tag_name>:<alias1>:<alias2>:..."
    //
    // The value of each tag column is a list of Values, which follows the order of the
    //   property names specified in the above column name. If a vertex does not have
    //   the specific tag, the value will be NULL
    //
    // Columns after the tag columns are edge columns. One column per edge type. The
    //   column name is in the following format
    //
    //   "_edge:<edge_name>:<alias1>:<alias2>:..."
    //
    // The value of each edge column is list<list<Value>>, which represents multiple
    //   edges. For each edge, the list of Values will follow the order of the property
    //   names specified in the column name. If a vertex does not have any edge for a
    //   specific edge type, the value for that column will be NULL
    //
    // The last column is the expression column, if the GetNeighborsRequest::expressions
    //   is not empty. The column name is in the format of this
    //
    //   "_expr:<alias1>:<alias2>:..."
    //
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
    // Based on whether to get the vertex properties or to get the edge properties,
    //   One of the following can be set. If an empty list is given then all properties
    //   of the vertex or the edge will be returned
    4: optional list<VertexProp>                vertex_props,
    5: optional list<EdgeProp>                  edge_props,
    // A list of expressions with alias
    6: optional list<Expr>                      expressions,
    // Whether to do the dedup based on the entire result row
    7: bool                                     dedup = false,
    // List of expressions used by the order-by clause
    8: optional list<OrderBy>                   order_by,
    9: optional i64                             limit,
    // If a filter is provided, only vertices that are satisfied the filter
    // will be returned
    10: optional binary                          filter,
}


struct GetPropResponse {
    1: ResponseCommon           result,
    // The result will be returned in a dataset, which is in the following form
    //
    // Each row represents properties for one vertex or one edge
    //
    // Here is the layout of the dataset
    //   ====================================
    //   |  alias1 | alias2 | alias3  | ... |
    //   |==================================|
    //   |  Value  | Value  | Value   | ... |
    //   |----------------------------------|
    //   | .....                            |
    //   ====================================
    //
    // If the request is to get tag prop, the first column will **always** be the vid,
    // and the first column name is "_vid".
    //
    // Each column represents one peoperty. When all properties are returned, the
    //   column name is in the form of
    //   "<tag_name>:<prop_alias>" or "<edge_type_name>:<prop_alias>"
    //
    // If the vertex or the edge does NOT have the given property, the value will
    //   be a NULL
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
    1: common.Value id,
    2: list<NewTag> tags,
}


struct EdgeKey {
    1: common.Value         src,
    // When edge_type > 0, it's an out-edge, otherwise, it's an in-edge
    // When query edge props, the field could be unset.
    2: common.EdgeType      edge_type,
    3: common.EdgeRanking   ranking,
    4: common.Value         dst,
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
    2: map<common.PartitionID, list<common.Value>>
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


struct UpdatedProp {
    1: required binary          name,       // property name
    2: required binary          value,      // new value (encoded expression)
}

/*
 * Start of UpdateVertex section
 */
struct UpdateVertexRequest {
    1: common.GraphSpaceID          space_id,
    2: common.PartitionID           part_id,
    3: common.Value                 vertex_id,
    4: required common.TagID        tag_id
    5: list<UpdatedProp>            updated_props,
    6: optional bool                insertable = false,
    // A list of expressions
    7: optional list<binary>        return_props,
    // If provided, the update happens only when the condition evaluates true
    8: optional binary              condition,
}
/*
 * End of UpdateVertex section
 */


/*
 * Start of UpdateEdge section
 */
struct UpdateEdgeRequest {
    1: common.GraphSpaceID      space_id,
    2: common.PartitionID       part_id,
    3: EdgeKey                  edge_key,
    4: list<UpdatedProp>        updated_props,
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
struct LookupIndexResp {
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

 enum ScanType {
    PREFIX = 1,
    RANGE  = 2,
 } (cpp.enum_strict)

struct IndexColumnHint {
    1: binary                   column_name,
    // If scan_type == PREFIX, using begin_value to handler prefix.
    // Else using begin_value and end_value to handler ranger.
    2: ScanType                 scan_type,
    3: common.Value             begin_value,
    4: common.Value             end_value,
}

struct IndexQueryContext {
    1: common.IndexID           index_id,
    // filter is an encoded expression of where clause.
    // Used for secondary filtering from a result set
    2: binary                   filter,
    // There are two types of scan: 1, range scan; 2, match scan (prefix);
    // The columns_hints are not allowed to be empty, At least one index column must be hit.
    3: list<IndexColumnHint>    column_hints,
}


struct IndexSpec {
    // In order to union multiple indices, multiple index hints are allowed
    1: required list<IndexQueryContext>   contexts,
    2: required bool                      is_edge,
    3: required i32                       tag_or_edge_id,
}


struct LookupIndexRequest {
    1: required common.GraphSpaceID         space_id,
    2: required list<common.PartitionID>    parts,
    3: IndexSpec                            indices,
    // We only support specified fields here, not wild card "*"
    // If the list is empty, only the VertexID or the EdgeKey will
    // be returned
    4: optional list<binary>                return_columns,
}


// This request will make the storage lookup the index first, then traverse
//   to the neighbor nodes from the index results. So it is the combination
//   of lookupIndex() and getNeighbors()
struct LookupAndTraverseRequest {
    1: required common.GraphSpaceID         space_id,
    2: required list<common.PartitionID>    parts,
    3: IndexSpec                            indices,
    4: TraverseSpec                         traverse_spec,
}

/*
 * End of Index section
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
    LookupIndexResp lookupIndex(1: LookupIndexRequest req);

    GetNeighborsResponse lookupAndTraverse(1: LookupAndTraverseRequest req);
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


service GeneralStorageService {
    // Interfaces for key-value storage
    KVGetResponse   get(1: KVGetRequest req);
    ExecResponse    put(1: KVPutRequest req);
    ExecResponse    remove(1: KVRemoveRequest req);
}

