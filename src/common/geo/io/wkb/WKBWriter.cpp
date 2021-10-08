/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/geo/io/wkb/WKBWriter.h"

namespace nebula {

// TODO(jie) Check the validity of geom when writing wkb
std::string WKBWriter::write(const Geometry& geom, ByteOrder byteOrder) {
  os_.setByteOrder(byteOrder);
  os_.writeUint8(folly::to<uint8_t>(byteOrder));

  GeoShape shape = geom.shape();
  uint32_t shapeType = folly::to<uint32_t>(shape);
  os_.writeUint32(shapeType);
  switch (shape) {
    case GeoShape::POINT: {
      const Point& point = geom.point();
      writePoint(point);
      return os_.str();
    }
    case GeoShape::LINESTRING: {
      const LineString& line = geom.lineString();
      writeLineString(line);
      return os_.str();
    }
    case GeoShape::POLYGON: {
      const Polygon& polygon = geom.polygon();
      writePolygon(polygon);
      return os_.str();
    }
    default:
      LOG(FATAL)
          << "Geomtry shapes other than Point/LineString/Polygon are not currently supported";
      return "";
  }
}

void WKBWriter::writePoint(const Point& point) { writeCoordinate(point.coord); }

void WKBWriter::writeLineString(const LineString& line) {
  auto coordList = line.coordList;
  uint32_t numCoord = coordList.size();
  os_.writeUint32(numCoord);
  writeCoordinateList(coordList);
}

void WKBWriter::writePolygon(const Polygon& polygon) {
  auto coordListList = polygon.coordListList;
  uint32_t numCoordList = coordListList.size();
  os_.writeUint32(numCoordList);
  writeCoordinateListList(coordListList);
}

void WKBWriter::writeCoordinate(const Coordinate& coord) {
  os_.writeDouble(coord.x);
  os_.writeDouble(coord.y);
}

void WKBWriter::writeCoordinateList(const std::vector<Coordinate>& coordList) {
  for (size_t i = 0; i < coordList.size(); ++i) {
    writeCoordinate(coordList[i]);
  }
}

void WKBWriter::writeCoordinateListList(const std::vector<std::vector<Coordinate>>& coordListList) {
  for (size_t i = 0; i < coordListList.size(); ++i) {
    const auto& coordList = coordListList[i];
    uint32_t numCoord = coordList.size();
    os_.writeUint32(numCoord);
    writeCoordinateList(coordList);
  }
}

}  // namespace nebula
