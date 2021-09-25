/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/geo/io/Geometry.h"
#include "common/geo/io/wkb/ByteOrder.h"

namespace nebula {

class WKBWriter {
 public:
  WKBWriter() {}

  ~WKBWriter() {}

  std::string write(const Geometry* geom) const {
    std::string wkb = "";

    uint8_t byteOrder =
        static_cast<std::underlying_type_t<ByteOrder>>(ByteOrderData::getMachineByteOrder());
    writeUint8(wkb, byteOrder);

    auto shape = geom->shape();
    uint32_t shapeType = static_cast<std::underlying_type_t<ShapeType>>(shape);
    writeUint32(wkb, shapeType);
    switch (shape) {
      case ShapeType::Point: {
        const Point* point = static_cast<const Point*>(geom);
        writeCoordinate(wkb, point->coord);
        return wkb;
      }
      case ShapeType::LineString: {
        const LineString* line = static_cast<const LineString*>(geom);
        auto coordList = line->coordList;
        uint32_t numPoints = coordList.size();
        writeUint32(wkb, numPoints);
        writeCoordinateList(wkb, coordList);
        return wkb;
      }
      case ShapeType::Polygon: {
        const Polygon* polygon = static_cast<const Polygon*>(geom);
        auto coordListList = polygon->coordListList;
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

  void writeCoordinate(std::string& wkb, const Coordinate& coord) const {
    writeDouble(wkb, coord.x);
    writeDouble(wkb, coord.y);
  }

  void writeCoordinateList(std::string& wkb, const std::vector<Coordinate>& coordList) const {
    for (size_t i = 0; i < coordList.size(); ++i) {
      writeCoordinate(wkb, coordList[i]);
    }
  }

  void writeCoordinateListList(std::string& wkb,
                               const std::vector<std::vector<Coordinate>>& coordListList) const {
    for (size_t i = 0; i < coordListList.size(); ++i) {
      const auto& coordList = coordListList[i];
      uint32_t numPoints = coordList.size();
      writeUint32(wkb, numPoints);
      writeCoordinateList(wkb, coordList);
    }
  }

  void writeUint8(std::string& wkb, uint8_t v) const {
    wkb.append(reinterpret_cast<char*>(&v), sizeof(v));
  }

  void writeUint32(std::string& wkb, uint32_t v) const {
    wkb.append(reinterpret_cast<char*>(&v), sizeof(v));
  }

  void writeDouble(std::string& wkb, double v) const {
    wkb.append(reinterpret_cast<char*>(&v), sizeof(v));
  }
};

}  // namespace nebula
