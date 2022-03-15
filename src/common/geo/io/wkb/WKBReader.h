/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_GEO_IO_WKB_WKBREADER_H
#define COMMON_GEO_IO_WKB_WKBREADER_H

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/Geography.h"
#include "common/geo/io/wkb/ByteOrder.h"
#include "common/geo/io/wkb/ByteOrderDataIOStream.h"

namespace nebula {
namespace geo {

class WKBReader {
 public:
  WKBReader() {}

  ~WKBReader() {}

  StatusOr<Geography> read(const std::string &wkb);

 private:
  StatusOr<Point> readPoint();

  StatusOr<LineString> readLineString();

  StatusOr<Polygon> readPolygon();

  StatusOr<ByteOrder> readByteOrder();

  StatusOr<GeoShape> readShapeType();

  StatusOr<Coordinate> readCoordinate();

  StatusOr<std::vector<Coordinate>> readCoordinateList(uint32_t num);

  StatusOr<std::vector<std::vector<Coordinate>>> readCoordinateListList(uint32_t num);

 private:
  ByteOrderDataInStream is_;
};

}  // namespace geo
}  // namespace nebula
#endif
