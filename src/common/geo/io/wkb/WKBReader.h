/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/geo/io/Geometry.h"
#include "common/geo/io/wkb/ByteOrder.h"

namespace nebula {

class WKBReader {
 public:
  WKBReader() {}

  ~WKBReader() {}

  // TODO(jie) Check the validity of geometry when reading the wkb
  StatusOr<std::unique_ptr<Geometry>> read(std::string wkb) const;

  StatusOr<std::unique_ptr<Geometry>> read(uint8_t *&beg, uint8_t *end) const;

  StatusOr<ByteOrder> readByteOrder(uint8_t *&beg, uint8_t *end) const;

  StatusOr<GeoShape> readShapeType(uint8_t *&beg, uint8_t *end, ByteOrder byteOrder) const;

  StatusOr<Coordinate> readCoordinate(uint8_t *&beg, uint8_t *end, ByteOrder byteOrder) const;

  StatusOr<std::vector<Coordinate>> readCoordinateList(uint8_t *&beg,
                                                       uint8_t *end,
                                                       ByteOrder byteOrder,
                                                       uint32_t num) const;

  StatusOr<std::vector<std::vector<Coordinate>>> readCoordinateListList(uint8_t *&beg,
                                                                        uint8_t *end,
                                                                        ByteOrder byteOrder,
                                                                        uint32_t num) const;
  StatusOr<uint8_t> readUint8(uint8_t *&beg, uint8_t *end) const;

  StatusOr<uint32_t> readUint32(uint8_t *&beg, uint8_t *end, ByteOrder byteOrder) const;

  StatusOr<double> readDouble(uint8_t *&beg, uint8_t *end, ByteOrder byteOrder) const;
};

}  // namespace nebula
