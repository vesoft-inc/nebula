/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


namespace cpp nebula
namespace java com.vesoft.nebula
namespace go nebula

cpp_include "base/ThriftTypes.h"

typedef i32 (cpp.type = "nebula::GraphSpaceID") GraphSpaceID
typedef i32 (cpp.type = "nebula::PartitionID") PartitionID
typedef i32 (cpp.type = "nebula::TagID") TagID
typedef i32 (cpp.type = "nebula::EdgeType") EdgeType
typedef i64 (cpp.type = "nebula::EdgeRanking") EdgeRanking
typedef i64 (cpp.type = "nebula::VertexID") VertexID
typedef i32 (cpp.type = "nebula::IndexID") IndexID

typedef i32 (cpp.type = "nebula::IPv4") IPv4
typedef i32 (cpp.type = "nebula::Port") Port

typedef i64 (cpp.type = "nebula::SchemaVer") SchemaVer

typedef i32 (cpp.type = "nebula::UserID") UserID
typedef i64 (cpp.type = "nebula::ClusterID") ClusterID

// These are all data types supported in the graph properties
enum SupportedType {
    UNKNOWN = 0,

    // Simple types
    BOOL = 1,
    INT = 2,
    VID = 3,
    FLOAT = 4,
    DOUBLE = 5,
    STRING = 6,

    // Date time
    TIMESTAMP = 21,
    YEAR = 22,
    YEARMONTH = 23,
    DATE = 24,
    DATETIME = 25,

    // Graph specific
    PATH = 41,

    // Container types
    // LIST = 101,
    // SET = 102,
    // MAP = 103,      // The key type is always a STRING
    // STRUCT = 104,
} (cpp.enum_strict)


struct ValueType {
    1: SupportedType type;
    // vtype only exists when the type is a LIST, SET, or MAP
    2: optional ValueType value_type (cpp.ref = true);
    // When the type is STRUCT, schema defines the struct
    3: optional Schema schema (cpp.ref = true);
} (cpp.virtual)

union Value {
    1: i64     int_value;
    2: bool    bool_value;
    3: double  double_value;
    4: string  string_value;
    5: i64     timestamp;
}

struct ColumnDef {
    1: required string name,
    2: required ValueType type,
    3: optional Value default_value,
}

struct SchemaProp {
    1: optional i64      ttl_duration,
    2: optional string   ttl_col,
}

struct Schema {
    1: list<ColumnDef> columns,
    2: SchemaProp schema_prop,
}

union SchemaID {
    1: TagID         tag_id,
    2: EdgeType      edge_type,
}

struct IndexItem {
    1: IndexID             index_id,
    2: string              index_name,
    3: SchemaID            schema_id
    4: string              schema_name,
    5: list<ColumnDef>     fields,
}

struct HostAddr {
    1: IPv4  ip,
    2: Port  port,
}

struct Pair {
    1: string key,
    2: string value,
}

/**
** GOD is A global senior administrator.like root of Linux systems.
** ADMIN is an administrator for a given Graph Space.
** DBA is an schema administrator for a given Graph Space.
** USER is a normal user for a given Graph Space. A User can access (read and write) the data in the Graph Space.
** GUEST is a read-only role for a given Graph Space. A Guest cannot modify the data in the Graph Space.
** Refer to header file src/graph/PermissionManager.h for details.
**/

enum RoleType {
    GOD    = 0x01,
    ADMIN  = 0x02,
    DBA    = 0x03,
    USER   = 0x04,
    GUEST  = 0x05,
} (cpp.enum_strict)

enum UserLoginType {
   PASSWORD = 0x01,
   LDAP     = 0x02,
}

struct RoleItem {
    1: string        user,
    2: string        space,
    3: RoleType      role_type,
}

struct UserItem {
    1: required string        account;
    // Disable user if lock status is true.
    2: optional bool          is_lock,
    // user login type
    3: optional UserLoginType login_type,
    // encoded password
    4: optional string        encoded_pwd,
    // The number of queries an account can issue per hour
    5: optional i32           max_queries_per_hour,
    // The number of updates an account can issue per hour
    6: optional i32           max_updates_per_hour,
    // The number of times an account can connect to the server per hour
    7: optional i32           max_connections_per_hour,
    // The number of simultaneous connections to the server by an account
    8: optional i32           max_user_connections,
}

const ValueType kInvalidValueType = {"type" : UNKNOWN}
