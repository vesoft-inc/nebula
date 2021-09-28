/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/base/Base.h"
#include "common/geo/io/Geometry.h"
#include "common/geo/io/wkb/ByteOrder.h"

namespace nebula {

class WKBWriter {
 public:
  WKBWriter() {}

  ~WKBWriter() {}

  std::string write(const Geometry& geom) const;

  void writeCoordinate(std::string& wkb, const Coordinate& coord) const;

  void writeCoordinateList(std::string& wkb, const std::vector<Coordinate>& coordList) const;

  void writeCoordinateListList(std::string& wkb,
                               const std::vector<std::vector<Coordinate>>& coordListList) const;

  void writeUint8(std::string& wkb, uint8_t v) const;

  void writeUint32(std::string& wkb, uint32_t v) const;

  void writeDouble(std::string& wkb, double v) const;
};

}  // namespace nebula
