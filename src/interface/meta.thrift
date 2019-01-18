/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

namespace cpp nebula.meta
namespace java nebula.meta
namespace go nebula.meta

include "common.thrift"

enum ErrorCode {
    SUCCEEDED = 0,

    // RPC Failure
    E_DISCONNECTED = -1,
    E_FAIL_TO_CONNECT = -2,
    E_RPC_FAILURE = -3,

    E_LEADER_CHANAGED = -11,

    // Operation Failure
} (cpp.enum_strict)

struct ExecResp {
    1: ErrorCode ret,
    // Valid if ret equals E_LEADER_CHANAGED.
    2: common.HostAddr  leader,
}

struct CreateSpaceReq {
    1: common.GraphSpaceID space_id,
    2: i32 parts_num,
    3: i32 replica_factor,
}

struct DeleteSpaceReq {
    1: common.GraphSpaceID space_id,
}

struct AddTagReq {
    1: common.GraphSpaceID space_id,
    2: i32                 tag_id,
    3: common.Schema       schema,
}

struct AddEdgeReq {
    1: common.GraphSpaceID space_id,
    2: i32                 edge_type,
    3: common.Schema       schema,
}

struct GetTagSchemaReq {
    1: common.GraphSpaceID space_id,
    2: i32                 tag_id,
    3: i32                 version,
}

struct GetTagSchemaResp {
    1: common.Schema    schema,
}

struct GetEdgeSchemaReq {
    1: common.GraphSpaceID space_id,
    2: i32                 edge_type,
    3: i32                 version,
}

struct GetEdgeSchemaResp {
    1: common.Schema    schema,
}

struct GetPartsAllocReq {
    1: common.GraphSpaceID space_id,
}

struct GetPartsAllocResp {
    1: ExecResp exec_code,
    2: map<common.PartitionID, list<common.HostAddr>>(cpp.template = "std::unordered_map") parts,
}

struct AddHostsReq {
    1: list<common.HostAddr> hosts;
}

struct GetAllHostsResp {
    1: ExecResp exec_code,
    2: list<common.HostAddr> hosts,
}

struct RemoveHostsReq {
    1: list<common.HostAddr> hosts;
}

service MetaService {
    ExecResp createSpace(1: CreateSpaceReq req);
    ExecResp deleteSpace(1: DeleteSpaceReq req);

    ExecResp addTag(1: AddTagReq req);
    GetTagSchemaResp getTagSchema(1: GetTagSchemaReq req);

    ExecResp addEdge(1: AddEdgeReq req);
    GetEdgeSchemaResp getEdgeSchema(1: GetEdgeSchemaReq req);

    ExecResp addHosts(1: AddHostsReq req);
    GetAllHostsResp getAllHosts();
    ExecResp removeHosts(1: RemoveHostsReq req);

    GetPartsAllocResp getPartsAlloc(1: GetPartsAllocReq req);
}

