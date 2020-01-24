/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


namespace cpp nebula
namespace java com.vesoft.nebula
namespace go nebula

cpp_include "thrift/ThriftTypes.h"
cpp_include "datatypes/Date.h"
cpp_include "datatypes/Path.h"
cpp_include "datatypes/Value.h"
cpp_include "datatypes/KeyValue.h"
cpp_include "datatypes/HostAddr.h"

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
typedef i64 (cpp.type = "nebula::VertexID") VertexID

typedef i64 (cpp.type = "nebula::Timestamp") Timestamp

typedef i32 (cpp.type = "nebula::IPv4") IPv4
typedef i32 (cpp.type = "nebula::Port") Port

// !! Struct Date has a shadow data type defined in the ThriftTypes.h
// So any change here needs to be reflected to the shadow type there
struct Date {
    1: i16 year;    // Calendar year, such as 2019
    2: byte month;    // Calendar month: 1 - 12
    3: byte day;      // Calendar day: 1 -31
} (cpp.type = "nebula::Date")


// !! Struct DateTime has a shadow data type defined in the ThriftTypes.h
// So any change here needs to be reflected to the shadow type there
struct DateTime {
    1: i16 year;
    2: byte month;
    3: byte day;
    4: byte hour;         // Hour: 0 - 23
    5: byte minute;       // Minute: 0 - 59
    6: byte sec;          // Second: 0 - 59
    7: i32 microsec;    // Micro-second: 0 - 999,999
    8: i32 timezone;    // Time difference in seconds
} (cpp.type = "nebula::DateTime")


struct Step {
    1: EdgeType type;
    2: EdgeRanking ranking;
    3: VertexID dst;
} (cpp.type = "nebula::Step")


// Special type to support path during the query
struct Path {
    1: VertexID src;
    2: list<Step> steps;
} (cpp.type = "nebula::Path")


enum NullType {
    NT_Null     = 0,
    NT_NaN      = 1,
    NT_BadType  = 2,
} (cpp.enum_strict cpp.type = "nebula::NullType")


// The type to hold any supported values during the query
union Value {
    1: NullType                             nVal;
    2: VertexID                             vVal;
    3: bool                                 bVal;
    4: i64                                  iVal;
    5: double                               fVal;
    6: binary                               sVal;
    7: Timestamp                            tVal;
    8: Date                                 dVal;
    9: DateTime                             dtVal;
    10: Path                                pVal;
    11: List (cpp.type = "nebula::List")    listVal (cpp.ref_type = "unique");
    12: Map (cpp.type = "nebula::Map")      mapVal (cpp.ref_type = "unique");
} (cpp.type = "nebula::Value")


struct List {
    1: list<Value> values;
} (cpp.type = "nebula::List")


struct Map {
    1: map<binary, Value> (cpp.template = "std::unordered_map") kvs;
} (cpp.type = "nebula::Map")


struct HostAddr {
    1: IPv4  ip,
    2: Port  port,
} (cpp.type = "nebula::HostAddr")


struct KeyValue {
    1: binary key,
    2: binary value,
} (cpp.type = "nebula::KeyValue")

