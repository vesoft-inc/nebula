/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace cpp nebula.meta
namespace java com.vesoft.nebula.meta
namespace go nebula.meta

include "common.thrift"

enum ErrorCode {
    SUCCEEDED = 0,

    // RPC Failure
    E_DISCONNECTED = -1,
    E_FAIL_TO_CONNECT = -2,
    E_RPC_FAILURE = -3,

    E_LEADER_CHANGED = -11,

    // Operation Failure
    E_NO_HOSTS       = -21,
    E_EXISTED        = -22,
    E_NOT_FOUND      = -23,
    E_INVALID_HOST   = -24,
    E_UNSUPPORTED    = -25,

    // KV Failure
    E_STORE_FAILURE          = -31,
    E_STORE_SEGMENT_ILLEGAL  = -32,

    E_UNKNOWN        = -99,
} (cpp.enum_strict)


enum AlterSchemaOp {
    ADD    = 0x01,
    CHANGE = 0x02,
    DROP   = 0x03,
    UNKNOWN = 0x04,
} (cpp.enum_strict)


union ID {
    1: common.GraphSpaceID  space_id,
    2: common.TagID         tag_id,
    3: common.EdgeType      edge_type,
}

struct IdName {
    1: ID     id,
    2: string name,
}

struct Pair {
    1: string key,
    2: string value,
}

struct SpaceProperties {
    1: string               space_name,
    2: i32                  partition_num,
    3: i32                  replica_factor,
}

struct SpaceItem {
    1: common.GraphSpaceID  space_id,
    2: SpaceProperties      properties,
}

struct TagItem {
    1: common.TagID         tag_id,
    2: string               tag_name,
    3: common.SchemaVer     version,
    4: common.Schema        schema,
}

struct AlterSchemaItem {
    1: AlterSchemaOp        op,
    2: common.Schema        schema,
}

struct EdgeItem {
    1: common.EdgeType      edge_type,
    2: string               edge_name,
    3: common.SchemaVer     version,
    4: common.Schema        schema,
}

struct ExecResp {
    1: ErrorCode        code,
    // For custom kv operations, it is useless.
    2: ID               id,
    // Valid if ret equals E_LEADER_CHANGED.
    3: common.HostAddr  leader,
}

// Graph space related operations.
struct CreateSpaceReq {
    1: SpaceProperties  properties,
}

struct DropSpaceReq {
    1: string space_name
}

struct ListSpacesReq {
}

struct ListSpacesResp {
    1: ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: list<IdName> spaces,
}

struct GetSpaceReq {
    1: string     space_name,
}

struct GetSpaceResp {
    1: ErrorCode         code,
    2: common.HostAddr   leader,
    3: SpaceItem         item,
}

// Tags related operations
struct CreateTagReq {
    1: common.GraphSpaceID space_id,
    2: string              tag_name,
    3: common.Schema       schema,
}

struct AlterTagReq {
    1: common.GraphSpaceID    space_id,
    2: string                 tag_name,
    3: list<AlterSchemaItem>  tag_items,
}

struct DropTagReq {
    1: common.GraphSpaceID space_id,
    2: string              tag_name,
}

struct ListTagsReq {
    1: common.GraphSpaceID space_id,
}

struct ListTagsResp {
    1: ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: list<TagItem> tags,
}

struct GetTagReq {
    1: common.GraphSpaceID space_id,
    2: string              tag_name,
    3: common.SchemaVer    version,
}

struct GetTagResp {
    1: ErrorCode        code,
    2: common.HostAddr  leader,
    3: common.Schema    schema,
}

// Edge related operations.
struct CreateEdgeReq {
    1: common.GraphSpaceID space_id,
    2: string              edge_name,
    3: common.Schema       schema,
}

struct AlterEdgeReq {
    1: common.GraphSpaceID     space_id,
    2: string                  edge_name,
    3: list<AlterSchemaItem>   edge_items,
}

struct GetEdgeReq {
    1: common.GraphSpaceID space_id,
    2: string              edge_name,
    3: common.SchemaVer    version,
}

struct GetEdgeResp {
    1: ErrorCode        code,
    2: common.HostAddr  leader,
    3: common.Schema    schema,
}

struct DropEdgeReq {
    1: common.GraphSpaceID space_id,
    2: string              edge_name,
}

struct ListEdgesReq {
    1: common.GraphSpaceID space_id,
}

struct ListEdgesResp {
    1: ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: list<EdgeItem> edges,
}

// Host related operations.
struct AddHostsReq {
    1: list<common.HostAddr> hosts;
}

struct ListHostsReq {
}

struct ListHostsResp {
    1: ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: list<common.HostAddr> hosts,
}

struct RemoveHostsReq {
    1: list<common.HostAddr> hosts;
}

// Parts related operations.
struct GetPartsAllocReq {
    1: common.GraphSpaceID space_id,
}

struct GetPartsAllocResp {
    1: ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: map<common.PartitionID, list<common.HostAddr>>(cpp.template = "std::unordered_map") parts,
}

struct MultiPutReq {
    // segment is used to avoid conflict with system data.
    // it should be comprised of numbers and letters.
    1: string     segment,
    2: list<Pair> pairs,
}

struct GetReq {
    1: string segment,
    2: string key,
}

 struct GetResp {
    1: ErrorCode code,
    2: common.HostAddr  leader,
    3: string    value,
}

struct MultiGetReq {
    1: string       segment,
    2: list<string> keys,
}

struct MultiGetResp {
    1: ErrorCode    code,
    2: common.HostAddr  leader,
    3: list<string> values,
}

struct RemoveReq {
    1: string segment,
    2: string key,
}

struct RemoveRangeReq {
    1: string segment,
    2: string start,
    3: string end,
}

struct ScanReq {
    1: string segment,
    2: string start,
    3: string end,
}

struct ScanResp {
    1: ErrorCode code,
    2: common.HostAddr  leader,
    3: list<string> values,
}

struct HBResp {
    1: ErrorCode code,
    2: common.HostAddr  leader,
}

struct HBReq {
    1: common.HostAddr host,
}

service MetaService {
    ExecResp createSpace(1: CreateSpaceReq req);
    ExecResp dropSpace(1: DropSpaceReq req);
    GetSpaceResp getSpace(1: GetSpaceReq req);
    ListSpacesResp listSpaces(1: ListSpacesReq req);

    ExecResp createTag(1: CreateTagReq req);
    ExecResp alterTag(1: AlterTagReq req);
    ExecResp dropTag(1: DropTagReq req);
    GetTagResp getTag(1: GetTagReq req);
    ListTagsResp listTags(1: ListTagsReq req);

    ExecResp createEdge(1: CreateEdgeReq req);
    ExecResp alterEdge(1: AlterEdgeReq req);
    ExecResp dropEdge(1: DropEdgeReq req);
    GetEdgeResp getEdge(1: GetEdgeReq req);
    ListEdgesResp listEdges(1: ListEdgesReq req);

    ExecResp addHosts(1: AddHostsReq req);
    ExecResp removeHosts(1: RemoveHostsReq req);
    ListHostsResp listHosts(1: ListHostsReq req);

    GetPartsAllocResp getPartsAlloc(1: GetPartsAllocReq req);

    ExecResp multiPut(1: MultiPutReq req);
    GetResp get(1: GetReq req);
    MultiGetResp multiGet(1: MultiGetReq req);
    ExecResp remove(1: RemoveReq req);
    ExecResp removeRange(1: RemoveRangeReq req);
    ScanResp scan(1: ScanReq req);

    HBResp           heartBeat(1: HBReq req);
}

