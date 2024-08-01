/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

namespace cpp nebula.meta.v2

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

struct SpaceDesc {
    1: binary                   space_name,
    2: i32                      partition_num = 0,
    3: i32                      replica_factor = 0,
    4: binary                   charset_name,
    5: binary                   collate_name,
    6: ColumnTypeDef            vid_type = {"type": "PropertyType.FIXED_STRING", "type_length": 8},
    7: optional binary          group_name,
    8: optional IsolationLevel  isolation_level,
    9: optional binary          comment,
}

enum PropertyType {
    UNKNOWN = 0,

    // Simple types
    BOOL = 1,
    INT64 = 2,          // This is the same as INT in v1
    VID = 3,            // Deprecated, only supported by v1
    FLOAT = 4,
    DOUBLE = 5,
    STRING = 6,
    // String with fixed length. If the string content is shorteri
    // than the given length, '\0' will be padded to the end
    FIXED_STRING = 7,   // New in v2
    INT8 = 8,           // New in v2
    INT16 = 9,          // New in v2
    INT32 = 10,         // New in v2

    // Date time
    TIMESTAMP = 21,
    DATE = 24,
    DATETIME = 25,
    TIME = 26,

    // Geo spatial
    GEOGRAPHY = 31,
    
    // NEW List
    LIST_STRING = 32,
    LIST_INT = 33,
    LIST_FLOAT = 34,
    LIST_LIST_STRING = 35,
    LIST_LIST_INT = 36,
    LIST_LIST_FLOAT = 37,

    // NEW Set
    SET_STRING = 38,
    SET_INT = 39,
    SET_FLOAT = 40,
    SET_SET_STRING = 41,
    SET_SET_INT = 42,
    SET_SET_FLOAT = 43,
    
} (cpp.enum_strict)

// Geo shape type
enum GeoShape {
    ANY = 0,
    POINT = 1,
    LINESTRING = 2,
    POLYGON = 3,
} (cpp.enum_strict)

struct ColumnTypeDef {
    1: required PropertyType    type,
    // type_length is valid for fixed_string type
    2: optional i16             type_length = 0,
    // geo_shape is valid for geography type
    3: optional GeoShape        geo_shape,
}

enum IsolationLevel {
    DEFAULT  = 0x00,    // allow add half edge(either in or out edge succeeded)
    TOSS     = 0x01,    // add in and out edge atomic
} (cpp.enum_strict)
