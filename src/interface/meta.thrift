/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
    E_SPACE_EXISTED  = -22,
    E_NOT_FOUND      = -23,

    // KV Failure
    E_STORE_FAILURE  = -31,

    E_UNKNOWN        = -99,
} (cpp.enum_strict)

union ID {
    common.GraphSpaceID  space_id,
    common.TagID         tag_id,
    common.EdgeType      edge_type,
}

struct IdName {
    1: ID     id,
    2: string name,
}

struct Pair {
    1: string key,
    2: string value,
}

struct ExecResp {
    1: ErrorCode        code,
    2: ID               id,
    // Valid if ret equals E_LEADER_CHANGED.
    3: common.HostAddr  leader,
}

// Graph space related operations.
struct CreateSpaceReq {
    1: string space_name,
    2: i32 parts_num,
    3: i32 replica_factor,
}

struct DeleteSpaceReq {
    1: common.GraphSpaceID space_id,
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
    1: common.GraphSpaceID space_id,
}

struct GetSpaceResp {
    1: IdName space,
    2: i32    parts_num,
    3: i32    replica_factor,
}

// Tags related operations
struct AddTagReq {
    1: common.GraphSpaceID space_id,
    2: string              tag_name,
    3: common.Schema       schema,
}

struct RemoveTagReq {
    1: common.GraphSpaceID space_id,
    2: common.TagID        tag_id,
}

struct ListTagsReq {
    1: common.GraphSpaceID space_id,
}

struct ListTagsResp {
    1: ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: list<IdName> tags,
}

struct GetTagReq {
    1: common.GraphSpaceID space_id,
    2: common.TagID        tag_id,
    3: i32                 version,
}

struct GetTagResp {
    1: common.Schema    schema,
}

// Edge related operations.
struct AddEdgeReq {
    1: common.GraphSpaceID space_id,
    2: string              edge_name,
    3: common.Schema       schema,
}

struct RemoveEdgeReq {
    1: common.GraphSpaceID space_id,
    2: common.EdgeType     edge_type,
}

struct ListEdgesReq {
    1: common.GraphSpaceID space_id,
}

struct ListEdgesResp {
    1: ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: list<IdName> edges,
}

struct GetEdgeReq {
    1: common.GraphSpaceID space_id,
    2: common.EdgeType     edge_type,
    3: i32                 version,
}

struct GetEdgeResp {
    1: common.Schema    schema,
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

struct PutReq {
    1: string key,
    2: string value,
}

struct PutResp {
    1: ErrorCode code,
}

struct MultiPutReq {
    1: list<Pair> pairs
}

struct MultiPutResp {
    1: ErrorCode code,
} 

struct GetReq {
    1: string key,
}

struct GetResp {
    1: ErrorCode code,
    2: string    value,
}

struct MultiGetReq {
    1: list<string> keys,
}

struct MultiGetResp {
    1: ErrorCode    code,
    2: list<string> values
}

struct RemoveReq {
    1: string key,
}

struct RemoveResp {
    1: ErrorCode code,
}

struct RemoveRangeReq {
    1: string start,
    2: string end,
}

struct RemoveRangeResp {
    1: ErrorCode code,
}

struct ScanReq {
    1: string start,
    2: string end,
}

struct ScanResp {
    1: ErrorCode code,
    2: list<string> values,
}

service MetaService {
    ExecResp createSpace(1: CreateSpaceReq req);
    ExecResp deleteSpace(1: DeleteSpaceReq req);
    GetSpaceResp getSpace(1: GetSpaceReq req);
    ListSpacesResp listSpaces(1: ListSpacesReq req);

    ExecResp addTag(1: AddTagReq req);
    ExecResp RemoveTag(1: RemoveTagReq req);
    GetTagResp getTag(1: GetTagReq req);
    ListTagsResp listTags(1: ListTagsReq req);

    ExecResp addEdge(1: AddEdgeReq req);
    ExecResp RemoveEdge(1: RemoveEdgeReq req);
    GetEdgeResp getEdge(1: GetEdgeReq req);
    ListEdgesResp listEdges(1: ListEdgesReq req);

    ExecResp addHosts(1: AddHostsReq req);
    ExecResp removeHosts(1: RemoveHostsReq req);
    ListHostsResp listHosts(1: ListHostsReq req);

    GetPartsAllocResp getPartsAlloc(1: GetPartsAllocReq req);

    PutResp          put(1: PutReq req);
    MultiPutResp     multiPut(1: MultiPutReq req);
    GetResp          get(1: GetReq req);
    MultiGetResp     multiGet(1: MultiGetReq req);
    RemoveResp       remove(1: RemoveReq req);
    RemoveRangeResp  removeRange(1: RemoveRangeReq req);
    ScanResp         scan(1: ScanReq req);
}

