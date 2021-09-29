/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/geo/io/wkb/WKBReader.h"

namespace nebula {

StatusOr<Geometry> WKBReader::read(const std::string &wkb) const {
  const uint8_t *beg = reinterpret_cast<const uint8_t *>(wkb.data());
  const uint8_t *end = beg + wkb.size();
  return read(beg, end);
}

StatusOr<Geometry> WKBReader::read(const uint8_t *&beg, const uint8_t *end) const {
  auto byteOrderRet = readByteOrder(beg, end);
  NG_RETURN_IF_ERROR(byteOrderRet);
  ByteOrder byteOrder = byteOrderRet.value();

  auto shapeTypeRet = readShapeType(beg, end, byteOrder);
  NG_RETURN_IF_ERROR(shapeTypeRet);
  GeoShape shapeType = shapeTypeRet.value();

  switch (shapeType) {
    case GeoShape::POINT: {
      auto coordRet = readCoordinate(beg, end, byteOrder);
      NG_RETURN_IF_ERROR(coordRet);
      Coordinate coord = coordRet.value();
      return Point(coord);
    }
    case GeoShape::LINESTRING: {
      auto numPointsRet = readUint32(beg, end, byteOrder);
      NG_RETURN_IF_ERROR(numPointsRet);
      uint32_t numPoints = numPointsRet.value();
      auto coordListRet = readCoordinateList(beg, end, byteOrder, numPoints);
      NG_RETURN_IF_ERROR(coordListRet);
      std::vector<Coordinate> coordList = coordListRet.value();
      return LineString(coordList);
    }
    case GeoShape::POLYGON: {
      auto numRingsRet = readUint32(beg, end, byteOrder);
      NG_RETURN_IF_ERROR(numRingsRet);
      uint32_t numRings = numRingsRet.value();
      auto coordListListRet = readCoordinateListList(beg, end, byteOrder, numRings);
      NG_RETURN_IF_ERROR(coordListListRet);
      std::vector<std::vector<Coordinate>> coordListList = coordListListRet.value();
      return Polygon(coordListList);
    }
    default:
      LOG(FATAL)
          << "Geography shapes other than Point/LineString/Polygon are not currently supported";
      return Status::Error(
          "Geography shapes other than Point/LineString/Polygon are not currently supported");
  }
}

StatusOr<ByteOrder> WKBReader::readByteOrder(const uint8_t *&beg, const uint8_t *end) const {
  auto vRet = readUint8(beg, end);
  NG_RETURN_IF_ERROR(vRet);
  uint8_t v = vRet.value();
  if (v != 0 && v != 1) {
    return Status::Error("Unknown byte order");
  }
  ByteOrder byteOrder = static_cast<ByteOrder>(v);
  return byteOrder;
}

StatusOr<GeoShape> WKBReader::readShapeType(const uint8_t *&beg,
                                            const uint8_t *end,
                                            ByteOrder byteOrder) const {
  auto vRet = readUint32(beg, end, byteOrder);
  NG_RETURN_IF_ERROR(vRet);
  uint32_t v = vRet.value();
  if (v != 1 && v != 2 && v != 3) {
    return Status::Error("Unknown shape type");
  }
  GeoShape shapeType = static_cast<GeoShape>(v);
  return shapeType;
}

StatusOr<Coordinate> WKBReader::readCoordinate(const uint8_t *&beg,
                                               const uint8_t *end,
                                               ByteOrder byteOrder) const {
  auto xRet = readDouble(beg, end, byteOrder);
  NG_RETURN_IF_ERROR(xRet);
  double x = xRet.value();
  auto yRet = readDouble(beg, end, byteOrder);
  NG_RETURN_IF_ERROR(yRet);
  double y = yRet.value();
  return Coordinate(x, y);
}

StatusOr<std::vector<Coordinate>> WKBReader::readCoordinateList(const uint8_t *&beg,
                                                                const uint8_t *end,
                                                                ByteOrder byteOrder,
                                                                uint32_t num) const {
  std::vector<Coordinate> coordList;
  coordList.reserve(num);
  for (size_t i = 0; i < num; ++i) {
    auto coordRet = readCoordinate(beg, end, byteOrder);
    NG_RETURN_IF_ERROR(coordRet);
    Coordinate coord = coordRet.value();
    coordList.emplace_back(coord);
  }
  return coordList;
}

StatusOr<std::vector<std::vector<Coordinate>>> WKBReader::readCoordinateListList(
    const uint8_t *&beg, const uint8_t *end, ByteOrder byteOrder, uint32_t num) const {
  std::vector<std::vector<Coordinate>> coordListList;
  coordListList.reserve(num);
  for (size_t i = 0; i < num; ++i) {
    auto numPointsRet = readUint32(beg, end, byteOrder);
    NG_RETURN_IF_ERROR(numPointsRet);
    uint32_t numPoints = numPointsRet.value();
    auto coordListRet = readCoordinateList(beg, end, byteOrder, numPoints);
    NG_RETURN_IF_ERROR(coordListRet);
    std::vector<Coordinate> coordList = coordListRet.value();
    coordListList.emplace_back(coordList);
  }
  return coordListList;
}

StatusOr<uint8_t> WKBReader::readUint8(const uint8_t *&beg, const uint8_t *end) const {
  auto requiredSize = static_cast<int64_t>(sizeof(uint8_t));
  if (end - beg < requiredSize) {
    return Status::Error("Unable to parse uint8_t");
  }
  uint8_t v = *beg;
  beg += requiredSize;
  return v;
}

StatusOr<uint32_t> WKBReader::readUint32(const uint8_t *&beg,
                                         const uint8_t *end,
                                         ByteOrder byteOrder) const {
  auto requiredSize = static_cast<int64_t>(sizeof(uint32_t));
  if (end - beg < requiredSize) {
    return Status::Error("Unable to parse uint32_t");
  }
  uint32_t v = ByteOrderData::getUint32(beg, byteOrder);
  beg += requiredSize;
  return v;
}

StatusOr<double> WKBReader::readDouble(const uint8_t *&beg,
                                       const uint8_t *end,
                                       ByteOrder byteOrder) const {
  auto requiredSize = static_cast<int64_t>(sizeof(double));
  if (end - beg < requiredSize) {
    return Status::Error("Unable to parse double");
  }
  double v = ByteOrderData::getDouble(beg, byteOrder);
  beg += requiredSize;
  return v;
}

}  // namespace nebula
