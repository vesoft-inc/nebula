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

typedef i64 (cpp.type = "nebula::Timestamp") Timestamp

typedef i32 (cpp.type = "nebula::IPv4") IPv4
typedef i32 (cpp.type = "nebula::Port") Port


// !! Struct Date has a shadow data type defined in the ThriftTypes.h
// So any change here needs to be reflected to the shadow type there
struct Date {
    1: i16 year;    // Calendar year, such as 2019
    2: byte month;    // Calendar month: 1 - 12
    3: byte day;      // Calendar day: 1 -31
}


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
}


struct Step {
    1: EdgeType type;
    2: EdgeRanking ranking;
    3: VertexID dst;
}


// Special type to support path during the query
struct Path {
    1: VertexID src;
    2: list<Step> steps;
}


// The type to hold any supported values during the query
union Value {
    1: bool         bVal;
    2: i64          iVal;
    3: double       fVal;
    4: string       sVal;
    5: Timestamp    tVal;
    6: Date         dVal;
    7: DateTime     dtVal;
    8: Path         pVal (cpp2.ref_type = "unique");
    9: List         lVal (cpp2.ref_type = "unique");
    10: Map         mVal (cpp2.ref_type = "unique");
}


struct List {
    1: list<Value> values;
}


struct Map {
    1: map<string, Value> (cpp.template = "std::unordered_map") kvs;
}


struct HostAddr {
    1: IPv4  ip,
    2: Port  port,
}


struct KeyValue {
    1: binary key,
    2: binary value,
}

