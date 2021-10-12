/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/base/Base.h"
#include "common/datatypes/Geography.h"
#include "common/geo/io/wkb/ByteOrder.h"
#include "common/geo/io/wkb/ByteOrderDataIOStream.h"

namespace nebula {
namespace geo {

class WKBWriter {
 public:
  WKBWriter() {}

  ~WKBWriter() {}

  std::string write(const Geography& geog, ByteOrder byteOrder = ByteOrder::LittleEndian);

  void writePoint(const Point& point);

  void writeLineString(const LineString& line);

  void writePolygon(const Polygon& polygon);

  void writeByteOrder(ByteOrder byteOrder);

  void writeShapeType(GeoShape geoShape);

  void writeCoordinate(const Coordinate& coord);

  void writeCoordinateList(const std::vector<Coordinate>& coordList);

  void writeCoordinateListList(const std::vector<std::vector<Coordinate>>& coordListList);

 private:
  ByteOrderDataOutStream os_;
};

}  // namespace geo
}  // namespace nebula
