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
    E_INDEX_NOT_FOUND = -26,

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

    // Filter out
    E_FILTER_OUT         = -60,

    // partial result, used for kv interfaces
    E_PARTIAL_RESULT = -99,

    E_UNKNOWN = -100,
} (cpp.enum_strict)


enum PropOwner {
    SOURCE = 1,
    DEST = 2,
    EDGE = 3,
} (cpp.enum_strict)

enum EngineSignType {
    BLOCK_ON = 1,
    BLOCK_OFF = 2,
}

union EntryId {
    1: common.TagID tag_id,
    2: common.EdgeType edge_type,
}

struct PropDef {
    1: PropOwner owner,
    2: EntryId   id,
    3: string    name,    // Property name
    4: StatType  stat,    // calc stats when setted.
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

struct EdgeKey {
    1: common.VertexID src,
    // When edge_type > 0, it's an out-edge, otherwise, it's an in-edge
    // When query edge props, the field could be unset.
    2: common.EdgeType edge_type,
    3: common.EdgeRanking ranking,
    4: common.VertexID dst,
}

struct Edge {
    1: EdgeKey key,
    2: binary props,
}

struct IdAndProp {
    1: common.VertexID dst,
    2: binary props,
}

struct EdgeData {
    1: common.EdgeType   type,
    3: list<IdAndProp>   edges,  // dstId and it's props
}

struct TagData {
    1: common.TagID          tag_id,
    2: binary                data,
}

struct VertexData {
    1: common.VertexID       vertex_id,
    2: list<TagData>         tag_data,
    3: list<EdgeData>        edge_data,
}

struct VertexIndexData {
    1: common.VertexID       vertex_id,
    2: binary                props,
}

struct ResponseCommon {
    // Only contains the partition that returns error
    1: required list<ResultCode> failed_codes,
    // Query latency from storage service
    2: required i32 latency_in_us,
}

struct QueryResponse {
    1: required ResponseCommon result,
    2: optional map<common.TagID, common.Schema>(cpp.template = "std::unordered_map")       vertex_schema,
    3: optional map<common.EdgeType, common.Schema>(cpp.template = "std::unordered_map")    edge_schema,
    4: optional list<VertexData> vertices,
    5: optional i32 total_edges,
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

struct GetNeighborsRequest {
    1: common.GraphSpaceID space_id,
    // partId => ids
    2: map<common.PartitionID, list<common.VertexID>>(cpp.template = "std::unordered_map") parts,
    // When edge_type > 0, going along the out-edge, otherwise, along the in-edge
    3: list<common.EdgeType> edge_types,
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
    4: binary filter,
    5: list<PropDef> return_columns,
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

struct DeleteVerticesRequest {
    1: common.GraphSpaceID space_id,
    2: map<common.PartitionID, list<common.VertexID>>(cpp.template = "std::unordered_map") parts,
}

struct DeleteEdgesRequest {
    1: common.GraphSpaceID space_id,
    // partId => edgeKeys
    2: map<common.PartitionID, list<EdgeKey>>(cpp.template = "std::unordered_map") parts,
}

struct AdminExecResp {
    1: required ResponseCommon result,
}

struct AddPartReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: bool                as_learner,
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

struct TransLeaderReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: common.HostAddr     new_leader,
}

struct AddLearnerReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: common.HostAddr     learner,
}

struct CatchUpDataReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: common.HostAddr     target,
}

struct CheckPeersReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: list<common.HostAddr> peers,
}

struct GetLeaderReq {
}

struct GetLeaderResp {
    1: required ResponseCommon result,
    2: map<common.GraphSpaceID, list<common.PartitionID>> (cpp.template = "std::unordered_map") leader_parts;
}

struct UpdateResponse {
    1: required ResponseCommon result,
    2: optional common.Schema schema,   // return column related props schema
    3: optional binary data,            // return column related props value
    4: optional bool upsert = false,    // it's true when need to be inserted by UPSERT
}

struct UpdateItem {
    1: required binary name,    // the Tag name or Edge name
    2: required binary prop,    // property
    3: required binary value,   // new value expression which is encoded
}

struct UpdateVertexRequest {
    1: common.GraphSpaceID      space_id,
    2: common.VertexID          vertex_id,
    3: common.PartitionID       part_id,
    4: binary                   filter,
    5: list<UpdateItem>         update_items,
    6: list<binary>             return_columns,
    7: bool                     insertable,
}

struct UpdateEdgeRequest {
    1: common.GraphSpaceID      space_id,
    2: EdgeKey                  edge_key,
    3: common.PartitionID       part_id,
    4: binary                   filter,
    5: list<UpdateItem>         update_items,
    6: list<binary>             return_columns,
    7: bool                     insertable,
}

struct ScanEdgeRequest {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID part_id,
    // start key of this block
    3: optional binary cursor,
    // If specified, only return specified columns, if not, no columns are returned
    4: map<common.EdgeType, list<PropDef>> (cpp.template = "std::unordered_map") return_columns,
    5: bool all_columns,
    // max row count of edge in this response
    6: i32 limit,
    7: i64 start_time,
    8: i64 end_time,
}

struct ScanEdgeResponse {
    1: required ResponseCommon result,
    2: map<common.EdgeType, common.Schema> (cpp.template = "std::unordered_map") edge_schema,
    3: list<ScanEdge> edge_data,
    4: bool has_next,
    5: binary next_cursor, // next start key of scan
}

struct ScanEdge {
    1: common.VertexID src,
    2: common.EdgeType type,
    3: common.VertexID dst,
    4: binary value, // decode according to edge_schema.
}

struct ScanVertexRequest {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID part_id,
    // start key of this block
    3: optional binary cursor,
    // If specified, only return specified columns, if not, no columns are returned
    4: map<common.TagID, list<PropDef>> (cpp.template = "std::unordered_map") return_columns,
    5: bool all_columns,
    // max row count of tag in this response
    6: i32 limit,
    7: i64 start_time,
    8: i64 end_time,
}

struct ScanVertex {
    1: common.VertexID  vertexId,
    2: common.TagID     tagId,
    3: binary           value,                  // decode according to vertex_schema.
    4: i64              version,
}

struct ScanVertexResponse {
    1: required ResponseCommon result,
    2: map<common.TagID, common.Schema> (cpp.template = "std::unordered_map") vertex_schema,
    3: list<ScanVertex> vertex_data,
    4: bool has_next,
    5: binary next_cursor,          // next start key of scan
}

struct PutRequest {
    1: common.GraphSpaceID space_id,
    2: map<common.PartitionID, list<common.Pair>>(cpp.template = "std::unordered_map") parts,
}

struct RemoveRequest {
    1: common.GraphSpaceID space_id,
    2: map<common.PartitionID, list<string>>(cpp.template = "std::unordered_map") parts,
}

struct RemoveRangeRequest {
    1: common.GraphSpaceID space_id,
    2: map<common.PartitionID, list<common.Pair>>(cpp.template = "std::unordered_map") parts,
}

struct GetRequest {
    1: common.GraphSpaceID space_id,
    2: map<common.PartitionID, list<string>>(cpp.template = "std::unordered_map") parts,
    // When return_partly is true and some of the keys not found, will return the keys
    // which exist
    3: bool return_partly
}

struct PrefixRequest {
    1: common.GraphSpaceID space_id,
    2: map<common.PartitionID, string>(cpp.template = "std::unordered_map") parts,
}

struct ScanRequest {
    1: common.GraphSpaceID space_id,
    2: map<common.PartitionID, common.Pair>(cpp.template = "std::unordered_map") parts,
}

struct GeneralResponse {
    1: required ResponseCommon result,
    2: map<string, string>(cpp.template = "std::unordered_map") values,
}

struct GetUUIDReq {
    1: common.GraphSpaceID space_id,
    2: common.PartitionID  part_id,
    3: string name,
}

struct GetUUIDResp {
    1: required ResponseCommon result,
    2: common.VertexID id,
}

struct BlockingSignRequest {
    1: common.GraphSpaceID          space_id,
    2: required EngineSignType      sign,
}

struct CreateCPRequest {
    1: common.GraphSpaceID          space_id,
    2: string                       name,
}

struct DropCPRequest {
    1: common.GraphSpaceID          space_id,
    2: string                       name,
}

struct RebuildIndexRequest {
    1: common.GraphSpaceID          space_id,
    2: list<common.PartitionID>     parts,
    3: common.IndexID               index_id,
    4: bool                         is_offline,
}

struct LookUpIndexRequest {
    1: common.GraphSpaceID       space_id,
    2: list<common.PartitionID>  parts,
    3: common.IndexID            index_id,
    4: binary                    filter,
    5: list<string>              return_columns,
    6: bool                      is_edge,
}

struct LookUpIndexResp {
    1: required ResponseCommon             result,
    2: optional common.Schema              schema,
    3: optional list<VertexIndexData>      vertices,
    4: optional list<Edge>                 edges,
}

service StorageService {
    QueryResponse getBound(1: GetNeighborsRequest req)

    QueryStatsResponse boundStats(1: GetNeighborsRequest req)

    // When return_columns is empty, return all properties
    QueryResponse getProps(1: VertexPropRequest req);
    EdgePropResponse getEdgeProps(1: EdgePropRequest req)

    ExecResponse addVertices(1: AddVerticesRequest req);
    ExecResponse addEdges(1: AddEdgesRequest req);

    ExecResponse deleteEdges(1: DeleteEdgesRequest req);
    ExecResponse deleteVertices(1: DeleteVerticesRequest req);

    UpdateResponse updateVertex(1: UpdateVertexRequest req)
    UpdateResponse updateEdge(1: UpdateEdgeRequest req)

    ScanEdgeResponse scanEdge(1: ScanEdgeRequest req)
    ScanVertexResponse scanVertex(1: ScanVertexRequest req)

    // Interfaces for admin operations
    AdminExecResp transLeader(1: TransLeaderReq req);
    AdminExecResp addPart(1: AddPartReq req);
    AdminExecResp addLearner(1: AddLearnerReq req);
    AdminExecResp waitingForCatchUpData(1: CatchUpDataReq req);
    AdminExecResp removePart(1: RemovePartReq req);
    AdminExecResp memberChange(1: MemberChangeReq req);
    AdminExecResp checkPeers(1: CheckPeersReq req);
    GetLeaderResp getLeaderPart(1: GetLeaderReq req);

    // Interfaces for manage checkpoint
    AdminExecResp createCheckpoint(1: CreateCPRequest req);
    AdminExecResp dropCheckpoint(1: DropCPRequest req);
    AdminExecResp blockingWrites(1: BlockingSignRequest req);

    // Interfaces for rebuild index
    AdminExecResp rebuildTagIndex(1: RebuildIndexRequest req);
    AdminExecResp rebuildEdgeIndex(1: RebuildIndexRequest req);

    // Interfaces for key-value storage
    ExecResponse      put(1: PutRequest req);
    GeneralResponse   get(1: GetRequest req);
    ExecResponse      remove(1: RemoveRequest req);
    ExecResponse      removeRange(1: RemoveRangeRequest req);

    GetUUIDResp getUUID(1: GetUUIDReq req);

    // Interfaces for edge and vertex index scan
    LookUpIndexResp   lookUpIndex(1: LookUpIndexRequest req);
}
