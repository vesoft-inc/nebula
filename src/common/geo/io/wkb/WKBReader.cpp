/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/geo/io/wkb/WKBReader.h"

namespace nebula {
namespace geo {

StatusOr<Geography> WKBReader::read(const std::string &wkb) {
  is_.setInput(wkb);

  auto byteOrderRet = readByteOrder();
  NG_RETURN_IF_ERROR(byteOrderRet);
  ByteOrder byteOrder = byteOrderRet.value();
  is_.setByteOrder(byteOrder);

  auto shapeTypeRet = readShapeType();
  NG_RETURN_IF_ERROR(shapeTypeRet);
  GeoShape shapeType = shapeTypeRet.value();

  switch (shapeType) {
    case GeoShape::POINT: {
      return readPoint();
    }
    case GeoShape::LINESTRING: {
      return readLineString();
    }
    case GeoShape::POLYGON: {
      return readPolygon();
    }
    default:
      LOG(FATAL)
          << "Geography shapes other than Point/LineString/Polygon are not currently supported";
      return Status::Error(
          "Geography shapes other than Point/LineString/Polygon are not currently supported");
  }
}

StatusOr<Point> WKBReader::readPoint() {
  auto coordRet = readCoordinate();
  NG_RETURN_IF_ERROR(coordRet);
  Coordinate coord = coordRet.value();
  return Point(std::move(coord));
}

StatusOr<LineString> WKBReader::readLineString() {
  auto numCoordRet = is_.readUint32();
  NG_RETURN_IF_ERROR(numCoordRet);
  uint32_t numCoord = numCoordRet.value();
  auto coordListRet = readCoordinateList(numCoord);
  NG_RETURN_IF_ERROR(coordListRet);
  std::vector<Coordinate> coordList = coordListRet.value();
  return LineString(std::move(coordList));
}

StatusOr<Polygon> WKBReader::readPolygon() {
  auto numCoordListRet = is_.readUint32();
  NG_RETURN_IF_ERROR(numCoordListRet);
  uint32_t numCoordList = numCoordListRet.value();
  auto coordListListRet = readCoordinateListList(numCoordList);
  NG_RETURN_IF_ERROR(coordListListRet);
  std::vector<std::vector<Coordinate>> coordListList = coordListListRet.value();
  return Polygon(std::move(coordListList));
}

StatusOr<ByteOrder> WKBReader::readByteOrder() {
  auto vRet = is_.readUint8();
  NG_RETURN_IF_ERROR(vRet);
  uint8_t v = vRet.value();
  if (v != 0 && v != 1) {
    return Status::Error("Unknown byte order");
  }
  ByteOrder byteOrder = static_cast<ByteOrder>(v);
  return byteOrder;
}

StatusOr<GeoShape> WKBReader::readShapeType() {
  auto vRet = is_.readUint32();
  NG_RETURN_IF_ERROR(vRet);
  uint32_t v = vRet.value();
  if (v != 1 && v != 2 && v != 3) {
    return Status::Error("Unknown shape type");
  }
  GeoShape shapeType = static_cast<GeoShape>(v);
  return shapeType;
}

StatusOr<Coordinate> WKBReader::readCoordinate() {
  auto xRet = is_.readDouble();
  NG_RETURN_IF_ERROR(xRet);
  double x = xRet.value();
  auto yRet = is_.readDouble();
  NG_RETURN_IF_ERROR(yRet);
  double y = yRet.value();
  return Coordinate(x, y);
}

StatusOr<std::vector<Coordinate>> WKBReader::readCoordinateList(uint32_t num) {
  std::vector<Coordinate> coordList;
  coordList.reserve(num);
  for (size_t i = 0; i < num; ++i) {
    auto coordRet = readCoordinate();
    NG_RETURN_IF_ERROR(coordRet);
    Coordinate coord = coordRet.value();
    coordList.emplace_back(std::move(coord));
  }
  return coordList;
}

StatusOr<std::vector<std::vector<Coordinate>>> WKBReader::readCoordinateListList(uint32_t num) {
  std::vector<std::vector<Coordinate>> coordListList;
  coordListList.reserve(num);
  for (size_t i = 0; i < num; ++i) {
    auto numCoordRet = is_.readUint32();
    NG_RETURN_IF_ERROR(numCoordRet);
    uint32_t numCoord = numCoordRet.value();
    auto coordListRet = readCoordinateList(numCoord);
    NG_RETURN_IF_ERROR(coordListRet);
    std::vector<Coordinate> coordList = coordListRet.value();
    coordListList.emplace_back(std::move(coordList));
  }
  return coordListList;
}

}  // namespace geo
}  // namespace nebula
