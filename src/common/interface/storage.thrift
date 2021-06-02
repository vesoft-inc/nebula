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

struct PartitionResult {
    1: required common.ErrorCode    code,
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
    // The list of property names. If it is empty, then all properties in value will be returned.
    // Support kSrc, kType, kRank and kDst as well.
    2: list<binary>     props,
}


// Define a vertex property
struct VertexProp {
    // A valid tag id
    1: common.TagID tag,
    // The list of property names. If it is empty, then all properties in value will be returned.
    // Support kVid and kTag as well.
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
    //   from one tag. The column name will be in the following format in the same order
    //   which specified in VertexProp
    //
    //   "_tag:<tag_name>:<alias1>:<alias2>:..."
    //
    // The value of each tag column is a list of Values, which follows the order of the
    //   property names specified in the above column name. If a vertex does not have
    //   the specific tag, the value will be empty value
    //
    // Columns after the tag columns are edge columns. One column per edge type. The
    //   column name is in the following format in the same order which specified in EdgeProp
    //
    //   "_edge:<edge_name>:<alias1>:<alias2>:..."
    //
    // The value of each edge column is list<list<Value>>, which represents multiple
    //   edges. For each edge, the list of Values will follow the order of the property
    //   names specified in the column name. If a vertex does not have any edge for a
    //   specific edge type, the value for that column will be empty value
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
    2: map<common.PartitionID, list<common.Row>>
        (cpp.template = "std::unordered_map")   parts,
    // Based on whether to get the vertex properties or to get the edge properties,
    // exactly one of the vertex_props or edge_props must be set. If an empty list is given
    // then all properties of the vertex or the edge will be returned in tagId/edgeType ascending order
    3: optional list<VertexProp>                vertex_props,
    4: optional list<EdgeProp>                  edge_props,
    // A list of expressions with alias
    5: optional list<Expr>                      expressions,
    // Whether to do the dedup based on the entire result row
    6: bool                                     dedup = false,
    // List of expressions used by the order-by clause
    7: optional list<OrderBy>                   order_by,
    8: optional i64                             limit,
    // If a filter is provided, only vertices that are satisfied the filter
    // will be returned
    9: optional binary                          filter,
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
    // Each column represents one peoperty. the column name is in the form of "tag_name.prop_alias"
    // or "edge_type_name.prop_alias" in the same order which specified in VertexProp or EdgeProp
    //
    // If the request is to get tag prop, the first column will **always** be the vid,
    // and the first column name is "_vid".
    //
    // If the vertex or the edge does NOT have the given property, the value will be an empty value
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
    // if ture, when (vertexID,tagID) already exists, do nothing
    4: bool                                     if_not_exists,
}

struct AddEdgesRequest {
    1: common.GraphSpaceID                      space_id,
    // partId => edges
    2: map<common.PartitionID, list<NewEdge>>(
        cpp.template = "std::unordered_map")    parts,
    // A list of property names. The order of the property names should match
    //   the data order specified in the NewEdge.props
    3: list<binary>                             prop_names,
    // if ture, when edge already exists, do nothing
    4: bool                                     if_not_exists,
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
    // A list of kSrcProperty expressions, support kVid and kTag
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
    // A list of kEdgeProperty expressions, support kSrc, kType, kRank and kDst
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
    2: common.Value id,
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
    // Each column represents one peoperty. the column name is in the form of "tag_name.prop_alias"
    // or "edge_type_name.prop_alias" in the same order which specified in return_columns of request
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
    // When the field size of index_id IndexItem is not zero, the columns_hints are not allowed
    //    to be empty, At least one index column must be hit.
    // When the field size of index_id IndexItem is zero, the columns_hints must be empty.
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
    // The list of property names. Should not be empty.
    // Support kVid and kTag for vertex, kSrc, kType, kRank and kDst for edge.
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

struct ScanVertexRequest {
    1: common.GraphSpaceID                  space_id,
    2: common.PartitionID                   part_id,
    // start key of this block
    3: optional binary                      cursor,
    4: VertexProp                           return_columns,
    // max row count of tag in this response
    5: i64                                  limit,
    // only return data in time range [start_time, end_time)
    6: optional i64                         start_time,
    7: optional i64                         end_time,
    8: optional binary                      filter,
    // when storage enable multi versions and only_latest_version is true, only return latest version.
    // when storage disable multi versions, just use the default value.
    9: bool                                 only_latest_version = false,
    // if set to false, forbid follower read
    10: bool                                enable_read_from_follower = true,
}

struct ScanVertexResponse {
    1: required ResponseCommon              result,
    // The data will return as a dataset. The format is as follows:
    // Each column represents one property. the column name is in the form of "tag_name.prop_alias"
    // in the same order which specified in VertexProp in request.
    2: common.DataSet                       vertex_data,
    3: bool                                 has_next,
    // next start key of scan, only valid when has_next is true
    4: optional binary                      next_cursor,
}

struct ScanEdgeRequest {
    1: common.GraphSpaceID                  space_id,
    2: common.PartitionID                   part_id,
    // start key of this block
    3: optional binary                      cursor,
    4: EdgeProp                             return_columns,
    // max row count of edge in this response
    5: i64                                  limit,
    // only return data in time range [start_time, end_time)
    6: optional i64                         start_time,
    7: optional i64                         end_time,
    8: optional binary                      filter,
    // when storage enable multi versions and only_latest_version is true, only return latest version.
    // when storage disable multi versions, just use the default value.
    9: bool                                only_latest_version = false,
    // if set to false, forbid follower read
    10: bool                                enable_read_from_follower = true,
}

struct ScanEdgeResponse {
    1: required ResponseCommon              result,
    // The data will return as a dataset. The format is as follows:
    // Each column represents one property. the column name is in the form of "edge_name.prop_alias"
    // in the same order which specified in EdgeProp in requesss.
    2: common.DataSet                       edge_data,
    3: bool                                 has_next,
    // next start key of scan, only valid when has_next is true
    4: optional binary                      next_cursor,
}

struct TaskPara {
    1: common.GraphSpaceID                  space_id,
    2: optional list<common.PartitionID>    parts,
    3: optional list<binary>                task_specfic_paras
}

struct AddAdminTaskRequest {
    // rebuild index / flush / compact / statis
    1: meta.AdminCmd                        cmd
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

    ScanVertexResponse scanVertex(1: ScanVertexRequest req)
    ScanEdgeResponse scanEdge(1: ScanEdgeRequest req)

    GetUUIDResp getUUID(1: GetUUIDReq req);

    // Interfaces for edge and vertex index scan
    LookupIndexResp lookupIndex(1: LookupIndexRequest req);

    GetNeighborsResponse lookupAndTraverse(1: LookupAndTraverseRequest req);
    ExecResponse addEdgesAtomic(1: AddEdgesRequest req);
}


//////////////////////////////////////////////////////////
//
//  Requests, responses for the StorageAdminService
//
//////////////////////////////////////////////////////////
// Common response for admin methods
struct AdminExecResp {
    1: required ResponseCommon   result,
    2: optional meta.StatisItem  statis,
}


struct TransLeaderReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: common.HostAddr     new_leader,
}


struct AddPartReq {
    1: common.GraphSpaceID   space_id,
    2: common.PartitionID    part_id,
    3: bool                  as_learner,
    4: list<common.HostAddr> peers,
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
    1: common.GraphSpaceID   space_id,
    2: common.PartitionID    part_id,
    3: list<common.HostAddr> peers,
}


struct RebuildIndexRequest {
    1: common.GraphSpaceID          space_id,
    2: list<common.PartitionID>     parts,
    3: common.IndexID               index_id,
}

struct CreateCPResp {
    1: required ResponseCommon      result,
    2: list<common.CheckpointInfo>  info,
}

struct ListClusterInfoResp {
    1: required ResponseCommon  result,
    2: common.DirInfo           dir,
}

struct ListClusterInfoReq {
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
    CreateCPResp  createCheckpoint(1: CreateCPRequest req);
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

    ListClusterInfoResp listClusterInfo(1: ListClusterInfoReq req);
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

//////////////////////////////////////////////////////////
//
//  Requests, responses for the InternalStorageService
//
//////////////////////////////////////////////////////////

// transaction request
struct InternalTxnRequest {
    1: i64                                  txn_id,
    2: i32                                  space_id,
    // need this(part_id) to satisfy getResponse
    3: i32                                  part_id,
    // position of chain
    4: i32                                  position,
    5: list<list<binary>>                   data
}

struct GetValueRequest {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID part_id,
    3: binary key
}

struct GetValueResponse {
    1: required ResponseCommon result
    2: binary value
}

service InternalStorageService {
    GetValueResponse  getValue(1: GetValueRequest req);
    ExecResponse    forwardTransaction(1: InternalTxnRequest req);
}
