/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_DATATYPES_COMMON_CPP2OPS_H_
#define COMMON_DATATYPES_COMMON_CPP2OPS_H_
#include "common/thrift/ThriftCpp2OpsHelper.h"

namespace nebula {
struct Value;
struct Vertex;
struct Tag;
struct Edge;
struct Step;
struct Path;
struct HostAddr;
struct KeyValue;
struct Date;
struct DateTime;
struct Time;
struct Map;
struct Set;
struct List;
struct DataSet;
struct Coordinate;
struct Point;
struct LineString;
struct Polygon;
struct Geography;
struct Duration;
}  // namespace nebula

namespace apache::thrift {

SPECIALIZE_CPP2OPS(nebula::Value);
SPECIALIZE_CPP2OPS(nebula::Vertex);
SPECIALIZE_CPP2OPS(nebula::Tag);
SPECIALIZE_CPP2OPS(nebula::Edge);
SPECIALIZE_CPP2OPS(nebula::Step);
SPECIALIZE_CPP2OPS(nebula::Path);
SPECIALIZE_CPP2OPS(nebula::HostAddr);
SPECIALIZE_CPP2OPS(nebula::KeyValue);
SPECIALIZE_CPP2OPS(nebula::Date);
SPECIALIZE_CPP2OPS(nebula::Time);
SPECIALIZE_CPP2OPS(nebula::DateTime);
SPECIALIZE_CPP2OPS(nebula::Map);
SPECIALIZE_CPP2OPS(nebula::Set);
SPECIALIZE_CPP2OPS(nebula::List);
SPECIALIZE_CPP2OPS(nebula::DataSet);
SPECIALIZE_CPP2OPS(nebula::Coordinate);
SPECIALIZE_CPP2OPS(nebula::Point);
SPECIALIZE_CPP2OPS(nebula::LineString);
SPECIALIZE_CPP2OPS(nebula::Polygon);
SPECIALIZE_CPP2OPS(nebula::Geography);
SPECIALIZE_CPP2OPS(nebula::Duration);

}  // namespace apache::thrift

#endif  // COMMON_DATATYPES_COMMON_CPP2OPS_H_
