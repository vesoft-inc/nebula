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
typedef i32 (cpp.type = "nebula::TagIndexID") TagIndexID
typedef i32 (cpp.type = "nebula::EdgeIndexID") EdgeIndexID

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

struct HostAddr {
    1: IPv4  ip,
    2: Port  port,
}

struct Pair {
    1: string key,
    2: string value,
}

const ValueType kInvalidValueType = {"type" : UNKNOWN}
