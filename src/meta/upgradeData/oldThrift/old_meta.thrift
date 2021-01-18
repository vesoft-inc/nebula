/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace cpp nebula.oldmeta

cpp_include "common/thrift/ThriftTypes.h"

typedef i32 (cpp.type = "nebula::GraphSpaceID") GraphSpaceID
typedef i32 (cpp.type = "nebula::PartitionID") PartitionID
typedef i32 (cpp.type = "nebula::TagID") TagID
typedef i32 (cpp.type = "nebula::EdgeType") EdgeType
typedef i64 (cpp.type = "nebula::EdgeRanking") EdgeRanking
typedef i64 (cpp.type = "nebula::VertexID") VertexID
typedef i32 (cpp.type = "nebula::IndexID") IndexID

typedef i32 IPv4
typedef i32 (cpp.type = "nebula::Port") Port

typedef i64 (cpp.type = "nebula::SchemaVer") SchemaVer

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

struct SpaceProperties {
    1: string               space_name,
    2: i32                  partition_num,
    3: i32                  replica_factor,
    4: string               charset_name,
    5: string               collate_name,
}

enum ConfigModule {
    UNKNOWN = 0x00,
    ALL     = 0x01,
    GRAPH   = 0x02,
    META    = 0x03,
    STORAGE = 0x04,
} (cpp.enum_strict)

enum ConfigType {
    INT64   = 0x00,
    DOUBLE  = 0x01,
    BOOL    = 0x02,
    STRING  = 0x03,
    NESTED  = 0x04,
} (cpp.enum_strict)

enum ConfigMode {
    IMMUTABLE   = 0x00,
    REBOOT      = 0x01,
    MUTABLE     = 0x02,
    IGNORED     = 0x03,
} (cpp.enum_strict)

struct ConfigItem {
    1: ConfigModule         module,
    2: string               name,
    3: ConfigType           type,
    4: ConfigMode           mode,
    5: binary               value,
}