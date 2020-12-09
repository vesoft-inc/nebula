/* vim: ft=proto
 * Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


namespace cpp nebula
namespace java com.vesoft.nebula
namespace go nebula
namespace js nebula
namespace csharp nebula
namespace py nebula2.common

cpp_include "common/thrift/ThriftTypes.h"
cpp_include "common/datatypes/DateOps.h"
cpp_include "common/datatypes/VertexOps.h"
cpp_include "common/datatypes/EdgeOps.h"
cpp_include "common/datatypes/PathOps.h"
cpp_include "common/datatypes/ValueOps.h"
cpp_include "common/datatypes/MapOps.h"
cpp_include "common/datatypes/ListOps.h"
cpp_include "common/datatypes/SetOps.h"
cpp_include "common/datatypes/DataSetOps.h"
cpp_include "common/datatypes/KeyValueOps.h"
cpp_include "common/datatypes/HostAddrOps.h"

/*
 *
 *  Note: In order to support multiple languages, all strings
 *        have to be defined as **binary** in the thrift file
 *
 */

typedef i32 (cpp.type = "nebula::GraphSpaceID") GraphSpaceID
typedef i32 (cpp.type = "nebula::PartitionID") PartitionID
typedef i32 (cpp.type = "nebula::TagID") TagID
typedef i32 (cpp.type = "nebula::EdgeType") EdgeType
typedef i64 (cpp.type = "nebula::EdgeRanking") EdgeRanking
typedef binary (cpp.type = "nebula::VertexID") VertexID
typedef i64 (cpp.type = "nebula::LogID") LogID
typedef i64 (cpp.type = "nebula::TermID") TermID

typedef i64 (cpp.type = "nebula::Timestamp") Timestamp

typedef i32 (cpp.type = "nebula::IndexID") IndexID

typedef i32 (cpp.type = "nebula::Port") Port

// !! Struct Date has a shadow data type defined in the Date.h
// So any change here needs to be reflected to the shadow type there
struct Date {
    1: i16 year;    // Calendar year, such as 2019
    2: byte month;    // Calendar month: 1 - 12
    3: byte day;      // Calendar day: 1 -31
} (cpp.type = "nebula::Date")

// !! Struct Time has a shadow data type defined in the Date.h
// So any change here needs to be reflected to the shadow type there
struct Time {
    1: byte hour;         // Hour: 0 - 23
    2: byte minute;       // Minute: 0 - 59
    3: byte sec;          // Second: 0 - 59
    4: i32 microsec;    // Micro-second: 0 - 999,999
} (cpp.type = "nebula::Time")

// !! Struct DateTime has a shadow data type defined in the Date.h
// So any change here needs to be reflected to the shadow type there
struct DateTime {
    1: i16 year;
    2: byte month;
    3: byte day;
    4: byte hour;         // Hour: 0 - 23
    5: byte minute;       // Minute: 0 - 59
    6: byte sec;          // Second: 0 - 59
    7: i32 microsec;    // Micro-second: 0 - 999,999
} (cpp.type = "nebula::DateTime")


enum NullType {
    __NULL__ = 0,
    NaN      = 1,
    BAD_DATA = 2,
    BAD_TYPE = 3,
    ERR_OVERFLOW = 4,
    UNKNOWN_PROP = 5,
    DIV_BY_ZERO = 6,
    OUT_OF_RANGE = 7,
} (cpp.enum_strict cpp.type = "nebula::NullType")


// The type to hold any supported values during the query
union Value {
    1: NullType                                 nVal;
    2: bool                                     bVal;
    3: i64                                      iVal;
    4: double                                   fVal;
    5: binary                                   sVal;
    6: Date                                     dVal;
    7: Time                                     tVal;
    8: DateTime                                 dtVal;
    9: Vertex (cpp.type = "nebula::Vertex")     vVal (cpp.ref_type = "unique");
    10: Edge (cpp.type = "nebula::Edge")        eVal (cpp.ref_type = "unique");
    11: Path (cpp.type = "nebula::Path")        pVal (cpp.ref_type = "unique");
    12: NList (cpp.type = "nebula::List")       lVal (cpp.ref_type = "unique");
    13: NMap (cpp.type = "nebula::Map")         mVal (cpp.ref_type = "unique");
    14: NSet (cpp.type = "nebula::Set")         uVal (cpp.ref_type = "unique");
    15: DataSet (cpp.type = "nebula::DataSet")  gVal (cpp.ref_type = "unique");
} (cpp.type = "nebula::Value")


// Ordered list
struct NList {
    1: list<Value> values;
} (cpp.type = "nebula::List")


// Unordered key/values pairs
struct NMap {
    1: map<binary, Value> (cpp.template = "std::unordered_map") kvs;
} (cpp.type = "nebula::Map")


// Unordered and unique values
struct NSet {
    1: set<Value> (cpp.template = "std::unordered_set") values;
} (cpp.type = "nebula::Set")


struct Row {
    1: list<Value> values;
} (cpp.type = "nebula::Row")


struct DataSet {
    1: list<binary>    column_names;   // Column names
    2: list<Row>       rows;
} (cpp.type = "nebula::DataSet")


struct Tag {
    1: binary name,
    // List of <prop_name, prop_value>
    2: map<binary, Value> (cpp.template = "std::unordered_map") props,
} (cpp.type = "nebula::Tag")


struct Vertex {
    1: VertexID vid,
    2: list<Tag> tags,
} (cpp.type = "nebula::Vertex")


struct Edge {
    1: VertexID src,
    2: VertexID dst,
    3: EdgeType type,
    4: binary name,
    5: EdgeRanking ranking,
    // List of <prop_name, prop_value>
    6: map<binary, Value> (cpp.template = "std::unordered_map") props,
} (cpp.type = "nebula::Edge")


struct Step {
    1: Vertex dst,
    2: EdgeType type,
    3: binary name,
    4: EdgeRanking ranking,
    5: map<binary, Value> (cpp.template = "std::unordered_map") props,
} (cpp.type = "nebula::Step")


// Special type to support path during the query
struct Path {
    1: Vertex src,
    2: list<Step> steps;
} (cpp.type = "nebula::Path")


struct HostAddr {
    // Host could be a valid IPv4 or IPv6 address, or a valid domain name
    1: string   host,
    2: Port     port,
} (cpp.type = "nebula::HostAddr")


struct KeyValue {
    1: binary key,
    2: binary value,
} (cpp.type = "nebula::KeyValue")

struct LogInfo {
    1: LogID  log_id;
    2: TermID term_id;
}

struct PartitionBackupInfo {
    1: map<PartitionID, LogInfo> (cpp.template = "std::unordered_map")  info,
}
