/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_GEO_IO_WKB_WKBWRITER_H
#define COMMON_GEO_IO_WKB_WKBWRITER_H

#include <string>  // for string
#include <vector>  // for vector

#include "common/base/Base.h"
#include "common/datatypes/Geography.h"               // for Coordinate (ptr...
#include "common/geo/io/wkb/ByteOrder.h"              // for ByteOrder, Byte...
#include "common/geo/io/wkb/ByteOrderDataIOStream.h"  // for ByteOrderDataOu...

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
#endif
