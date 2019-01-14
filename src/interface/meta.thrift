/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

namespace cpp nebula.meta
namespace java nebula.meta
namespace go nebula.meta

cpp_include "base/ThriftTypes.h"

typedef i32 (cpp.type = "nebula::IPv4") IPv4
typedef i32 (cpp.type = "nebula::Port") Port

enum ErrorCode {
    SUCCEEDED = 0,

    // RPC Failure
    E_DISCONNECTED = -1,
    E_FAIL_TO_CONNECT = -2,
    E_RPC_FAILURE = -3,

    E_LEADER_CHANAGED = -11,

    // Operation Failure
    E_NODE_HAS_EXISTED = -21,
    E_NODE_NOT_EXISTED = -22,
} (cpp.enum_strict)

struct HostAddr {
    1: IPv4  ip,
    2: Port  port,
}

struct ExecResponse {
    1: ErrorCode ret,
    // Valid if ret equals E_LEADER_CHANAGED.
    2: HostAddr  leader,
}

struct CreateNodeRequest {
    1: string path,
    2: string value,
}

struct SetNodeRequest {
    1: string path,
    2: string value,
}

struct GetNodeRequest {
    1: string path,
}

struct GetNodeResponse {
    1: ErrorCode ret,
    2: HostAddr leader,
    3: string value,
    4: i64 last_updated_time,
}

struct ListChildrenRequest {
    1: string path,
}

struct ListChildrenResponse {
    1: ErrorCode ret,
    2: HostAddr leader,
    3: list<string> children,
}

service MetaService {
    ExecResponse createNode(1: CreateNodeRequest req);
    ExecResponse setNode(1: SetNodeRequest req);
    GetNodeResponse getNode(1: GetNodeRequest req);
    ListChildrenResponse listChildren(1: ListChildrenRequest req);
}

