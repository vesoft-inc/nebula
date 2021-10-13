/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/base/Base.h"
#include "common/datatypes/Geography.h"

namespace nebula {
namespace geo {

class WKTWriter {
 public:
  WKTWriter() {}

  ~WKTWriter() {}

  std::string write(const Geography& geog) const;

  void writeCoordinate(std::string& wkt, const Coordinate& coord) const;

  void writeCoordinateList(std::string& wkt, const std::vector<Coordinate>& coordList) const;

  void writeCoordinateListList(std::string& wkt,
                               const std::vector<std::vector<Coordinate>>& coordListList) const;

  void writeDouble(std::string& wkt, double v) const;
};

}  // namespace geo
}  // namespace nebula
