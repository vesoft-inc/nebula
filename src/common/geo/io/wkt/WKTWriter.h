/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/geo/io/Geometry.h"

namespace nebula {

class WKTWriter {
 public:
  WKTWriter() {}

  ~WKTWriter() {}

  std::string write(const Geometry* geom) const {
    std::string wkt = "";

    auto shape = geom->shape();
    switch (shape) {
      case ShapeType::Point: {
        wkt.append("POINT");
        const Point* point = static_cast<const Point*>(geom);
        wkt.append("(");
        writeCoordinate(wkt, point->coord);
        wkt.append(")");
        return wkt;
      }
      case ShapeType::LineString: {
        wkt.append("LINESTRING");
        const LineString* line = static_cast<const LineString*>(geom);
        auto coordList = line->coordList;
        uint32_t numPoints = coordList.size();
        UNUSED(numPoints);
        wkt.append("(");
        writeCoordinateList(wkt, coordList);
        wkt.append(")");
        return wkt;
      }
      case ShapeType::Polygon: {
        wkt.append("POLYGON");
        const Polygon* polygon = static_cast<const Polygon*>(geom);
        auto coordListList = polygon->coordListList;
        uint32_t numRings = coordListList.size();
        UNUSED(numRings);
        wkt.append("(");
        writeCoordinateListList(wkt, coordListList);
        wkt.append(")");
        return wkt;
      }
      default:
        LOG(FATAL)
            << "Geomtry shapes other than Point/LineString/Polygon are not currently supported";
        return "";
    }
  }

  void writeCoordinate(std::string& wkt, const Coordinate& coord) const {
    writeDouble(wkt, coord.x);
    wkt.append(" ");
    writeDouble(wkt, coord.y);
  }

  void writeCoordinateList(std::string& wkt, const std::vector<Coordinate>& coordList) const {
    for (size_t i = 0; i < coordList.size(); ++i) {
      writeCoordinate(wkt, coordList[i]);
      wkt.append(",");
    }
    wkt.pop_back();
  }

  void writeCoordinateListList(std::string& wkt,
                               const std::vector<std::vector<Coordinate>>& coordListList) const {
    for (size_t i = 0; i < coordListList.size(); ++i) {
      const auto& coordList = coordListList[i];
      uint32_t numPoints = coordList.size();
      UNUSED(numPoints);
      wkt.append("(");
      writeCoordinateList(wkt, coordList);
      wkt.append(")");
      wkt.append(",");
    }
    wkt.pop_back();
  }

  void writeDouble(std::string& wkt, double v) const {
    wkt.append(folly::to<std::string>(v));
    LOG(INFO) << "writeDouble(wkt, v): " << wkt << ", " << v
              << ", folly::to: " << folly::to<std::string>(v);
  }
};

}  // namespace nebula
