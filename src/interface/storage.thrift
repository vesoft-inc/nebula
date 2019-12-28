/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace cpp nebula.storage
namespace java com.vesoft.nebula.storage
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

    // meta client failed
    E_LOAD_META_FAILED = -41,

    // checkpoint failed
    E_FAILED_TO_CHECKPOINT = -50,
    E_CHECKPOINT_BLOCKED = -51,

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


/**********************************************************
 *
 * Common types used by all services
 *
 *********************************************************/
// Define a vertex property
struct VertexProp {
    1: common.TagID tag,    // Tag ID
    2: string name,         // Property name
}


// Define an edge property
struct EdgeProp {
    1: common.EdgeType  type,   // Edge type
    2: string           name,   // Property name
}


// Enumeration of the statistic methods
enum StatType {
    SUM = 1,
    COUNT = 2,
    AVG = 3,
} (cpp.enum_strict)


// Define a statistic properties
struct StatProp {
    1: common.EdgeType  type,   // Edge type
    2: string           name,   // Property name
    3: StatType         stat,   // Stats method
}


// A list of property values to return
struct PropData {
    // The order of the props follows the order to the corresponding prop list,
    // so no need to return prop names
    1: list<common.Value>   props,
}


/**********************************************************
 *
 * Requests, responses for the GraphStorageService
 *
 *********************************************************/
/*********************************************************
 *                                                       *
 * GetNeighbors                                          *
 *                                                       */
struct GetNeighborsRequest {
    1: common.GraphSpaceID space_id,
    // partId => ids
    2: map<common.PartitionID, list<common.VertexID>>(
        cpp.template = "std::unordered_map") parts,
    // When edge_type > 0, going along the out-edge, otherwise, along the in-edge
    // If the edge type list is empty, all edges will be scaned
    3: list<common.EdgeType> edge_types,
    // If there is no property specified, only neighbor vertex id will be returned
    4: optional list<VertexProp> vertex_props,
    5: optional list<StatProp> stat_props,
    6: optional list<EdgeProp> edge_props,
    7: optional binary filter,
}


// Data returned for a given vertex, includintg the vertex properties,
// statistic properties, and edges
struct Vertex {
    1: common.VertexID          id,  // Source vertex id
    2: optional PropData        vertex_data,
    3: optional PropData        stat_data,
    4: optional list<PropData>  edge_data,
}


struct GetNeighborsResponse {
    1: required ResponseCommon result,
    2: optional list<Vertex> vertices,
}
/*                                                       *
 * GetNeighbors                                          *
 *                                                       *
 *********************************************************/


//
// Response for data modification requests
//
struct ExecResponse {
    1: required ResponseCommon result,
}


/*********************************************************
 *                                                       *
 * GetVertexProp                                         *
 *                                                       */
// Return properties for a given vertex
struct VertexPropData {
    1: required common.VertexID     id,
    2: optional list<PropData>      props,
    // When the get prop request does not provide property name list (ask for
    // all properties), this will return all property names
    3: optional list<VertexProp>    names,
}


struct VertexPropRequest {
    1: common.GraphSpaceID                      space_id,
    2: map<common.PartitionID, list<common.VertexID>>(
        cpp.template = "std::unordered_map")    parts,
    // If the property list is empty, return all properties
    // If the property list is not set, no property but the vertex ID
    // will be returned
    3: optional list<VertexProp>                vertex_props,
    4: optional binary                          filter,
}


struct VertexPropResponse {
    1: ResponseCommon       result,
    2: list<VertexPropData> data,
}
/*                                                       *
 * GetVertexProp                                         *
 *                                                       *
 *********************************************************/


/*********************************************************
 *                                                       *
 * GetEdgeProp                                           *
 *                                                       */
struct EdgeKey {
    1: common.VertexID      src,
    // When edge_type > 0, it's an out-edge, otherwise, it's an in-edge
    // When query edge props, the field could be unset.
    2: common.EdgeType      edge_type,
    3: common.EdgeRanking   ranking,
    4: common.VertexID      dst,
}


// Return properties for a given edge
struct EdgePropData {
    1: required EdgeKey         key,
    2: optional list<PropData>  props,
    // When the get prop request does not provide property name list (ask for
    //   all properties), this will return all property names
    3: optional list<string>    names,
}


struct EdgePropRequest {
    1: common.GraphSpaceID                      space_id,
    // partId => edges
    2: map<common.PartitionID, list<EdgeKey>>(
        cpp.template = "std::unordered_map")    parts,
    // If the property list is empty, return all properties
    // If the property list is not set, no property but the edge key
    // will be returned
    3: optional list<EdgeProp>                  edge_props,
    4: optional binary                          filter,
}


struct EdgePropResponse {
    1: ResponseCommon       result,
    2: list<EdgePropData>   data,
}
/*                                                       *
 * GetEdgeProp                                           *
 *                                                       *
 *********************************************************/


/*********************************************************
 *                                                       *
 * AddVertices                                           *
 *                                                       */
struct NewTag {
    1: common.TagID tag_id,
    2: list<PropData> props,
    // If the name list is empty, the order of the properties has to be
    // exactly same as that the schema defines
    3: optional list<string> names,
}


struct NewVertex {
    1: common.VertexID id,
    2: list<NewTag> tags,
}


struct NewEdge {
    1: EdgeKey key,
    2: list<PropData> props,
    // If the name list is empty, the order of the properties has to be
    // exactly same as that the schema defines
    3: optional list<string> names,
}


struct AddVerticesRequest {
    1: common.GraphSpaceID space_id,
    // partId => vertices
    2: map<common.PartitionID, list<NewVertex>>(
        cpp.template = "std::unordered_map") parts,
    // If true, it equals an (up)sert operation.
    3: bool overwritable = true,
}

struct AddEdgesRequest {
    1: common.GraphSpaceID space_id,
    // partId => edges
    2: map<common.PartitionID, list<NewEdge>>(
        cpp.template = "std::unordered_map") parts,
    // If true, it equals an upsert operation.
    3: bool overwritable = true,
}
/*                                                       *
 * AddVertices                                           *
 *                                                       *
 *********************************************************/


/*********************************************************
 *                                                       *
 * DeleteVertex                                          *
 *                                                       */
struct DeleteVertexRequest {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: common.VertexID     vid;
}


struct DeleteEdgesRequest {
    1: common.GraphSpaceID space_id,
    // partId => edgeKeys
    2: map<common.PartitionID, list<EdgeKey>>(
        cpp.template = "std::unordered_map") parts,
}
/*                                                       *
 * DeleteVertex                                          *
 *                                                       *
 *********************************************************/


// Response for update requests
struct UpdateResponse {
    1: required ResponseCommon result,
    // it's true when the vertex or the edge is inserted
    2: optional bool inserted = false,
    3: optional list<PropData> data,
}


/*********************************************************
 *                                                       *
 * UpdateVertex                                          *
 *                                                       */
struct UpdatedVertexProp {
    1: required common.TagID    tag_id,     // the Tag ID
    2: required string          name,       // property name
    3: required common.Value    value,      // new value
}


struct UpdateVertexRequest {
    1: common.GraphSpaceID          space_id,
    2: common.PartitionID           part_id,
    3: common.VertexID              vertex_id,
    4: list<UpdatedVertexProp>       updated_props,
    5: optional bool                insertable = false,
    6: optional list<VertexProp>    return_props,
    // If provided, the update happens only when the condition evaluates true
    7: optional binary              condition,
}
/*                                                       *
 * UpdateVertex                                          *
 *                                                       *
 *********************************************************/


/*********************************************************
 *                                                       *
 * UpdateEdge                                            *
 *                                                       */
struct UpdatedEdgeProp {
    1: required string          name,       // property name
    2: required common.Value    value,      // new value
}


struct CheckPeersReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: list<common.HostAddr> peers,
}


struct UpdateEdgeRequest {
    1: common.GraphSpaceID      space_id,
    2: common.PartitionID       part_id,
    3: EdgeKey                  edge_key,
    4: list<UpdatedEdgeProp>    updated_props,
    5: optional bool            insertable = false,
    6: optional list<EdgeProp>  return_props,
    // If provided, the update happens only when the condition evaluates true
    7: optional binary          condition,
}
/*                                                       *
 * UpdateEdge                                            *
 *                                                       *
 *********************************************************/


/*********************************************************
 *                                                       *
 * GetUUID                                               *
 *                                                       */
struct GetUUIDReq {
    1: common.GraphSpaceID  space_id,
    2: common.PartitionID   part_id,
    3: string               name,
}


struct GetUUIDResp {
    1: required ResponseCommon result,
    2: common.VertexID id,
}
/*                                                       *
 * GetUUID                                               *
 *                                                       *
 *********************************************************/


service GraphStorageService {
    GetNeighborsResponse getNeighbors(1: GetNeighborsRequest req)

    VertexPropResponse getVertexProps(1: VertexPropRequest req);
    EdgePropResponse getEdgeProps(1: EdgePropRequest req)

    ExecResponse addVertices(1: AddVerticesRequest req);
    ExecResponse addEdges(1: AddEdgesRequest req);

    ExecResponse deleteEdges(1: DeleteEdgesRequest req);
    ExecResponse deleteVertex(1: DeleteVertexRequest req);

    UpdateResponse updateVertex(1: UpdateVertexRequest req);
    UpdateResponse updateEdge(1: UpdateEdgeRequest req);

    GetUUIDResp getUUID(1: GetUUIDReq req);
}


/**********************************************************
 *
 * Requests, responses for the StorageAdminService
 *
 *********************************************************/
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


struct CreateCPRequest {
    1: common.GraphSpaceID  space_id,
    2: string               name,
}


struct DropCPRequest {
    1: common.GraphSpaceID  space_id,
    2: string               name,
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

    // Return all leader partitions on this host
    GetLeaderPartsResp getLeaderParts();
    // Return all peers
    AdminExecResp checkPeers(1: CheckPeersReq req);
}


/**********************************************************
 *
 * Requests, responses for the GeneralStorageService
 *
 *********************************************************/
struct KVGetRequest {
    1: common.GraphSpaceID space_id,
    2: map<common.PartitionID, list<binary>>(
        cpp.template = "std::unordered_map") parts,
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

