/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/geo/io/wkb/WKBWriter.h"

namespace nebula {

// TODO(jie) Check the validity of geom when writing wkb
std::string WKBWriter::write(const Geometry& geom) const {
  std::string wkb = "";

  uint8_t byteOrder =
      static_cast<std::underlying_type_t<ByteOrder>>(ByteOrderData::getMachineByteOrder());
  writeUint8(wkb, byteOrder);

  auto shape = geom.shape();
  uint32_t shapeType = folly::to<uint32_t>(shape);
  writeUint32(wkb, shapeType);
  switch (shape) {
    case GeoShape::POINT: {
      const Point& point = geom.point();
      writeCoordinate(wkb, point.coord);
      return wkb;
    }
    case GeoShape::LINESTRING: {
      const LineString& line = geom.lineString();
      auto coordList = line.coordList;
      uint32_t numPoints = coordList.size();
      writeUint32(wkb, numPoints);
      writeCoordinateList(wkb, coordList);
      return wkb;
    }
    case GeoShape::POLYGON: {
      const Polygon& polygon = geom.polygon();
      auto coordListList = polygon.coordListList;
      uint32_t numRings = coordListList.size();
      writeUint32(wkb, numRings);
      writeCoordinateListList(wkb, coordListList);
      return wkb;
    }
    default:
      LOG(FATAL)
          << "Geomtry shapes other than Point/LineString/Polygon are not currently supported";
      return "";
  }
}

void WKBWriter::writeCoordinate(std::string& wkb, const Coordinate& coord) const {
  writeDouble(wkb, coord.x);
  writeDouble(wkb, coord.y);
}

void WKBWriter::writeCoordinateList(std::string& wkb,
                                    const std::vector<Coordinate>& coordList) const {
  for (size_t i = 0; i < coordList.size(); ++i) {
    writeCoordinate(wkb, coordList[i]);
  }
}

void WKBWriter::writeCoordinateListList(
    std::string& wkb, const std::vector<std::vector<Coordinate>>& coordListList) const {
  for (size_t i = 0; i < coordListList.size(); ++i) {
    const auto& coordList = coordListList[i];
    uint32_t numPoints = coordList.size();
    writeUint32(wkb, numPoints);
    writeCoordinateList(wkb, coordList);
  }
}

void WKBWriter::writeUint8(std::string& wkb, uint8_t v) const {
  wkb.append(reinterpret_cast<char*>(&v), sizeof(v));
}

void WKBWriter::writeUint32(std::string& wkb, uint32_t v) const {
  wkb.append(reinterpret_cast<char*>(&v), sizeof(v));
}

void WKBWriter::writeDouble(std::string& wkb, double v) const {
  wkb.append(reinterpret_cast<char*>(&v), sizeof(v));
}

}  // namespace nebula
