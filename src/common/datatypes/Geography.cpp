/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/datatypes/Geography.h"

#include <folly/String.h>
#include <folly/hash/Hash.h>

#include <cstdint>

#include "common/geo/GeoUtils.h"
#include "common/geo/io/wkb/WKBReader.h"
#include "common/geo/io/wkb/WKBWriter.h"
#include "common/geo/io/wkt/WKTReader.h"
#include "common/geo/io/wkt/WKTWriter.h"

namespace nebula {

StatusOr<Geography> Geography::fromWKT(const std::string& wkt) {
  auto geomRet = WKTReader().read(wkt);
  NG_RETURN_IF_ERROR(geomRet);
  auto geom = geomRet.value();
  auto wkb = WKBWriter().write(geom);
  return Geography(wkb);
}

GeoShape Geography::shape() const {
  // TODO(jie) May store the shapetype as the data member of Geography is ok.
  const uint8_t* beg = reinterpret_cast<const uint8_t*>(wkb.data());
  const uint8_t* end = beg + wkb.size();
  WKBReader reader;
  auto byteOrderRet = reader.readByteOrder(beg, end);
  if (!byteOrderRet.ok()) {
    return GeoShape::UNKNOWN;
  }
  ByteOrder byteOrder = byteOrderRet.value();
  auto shapeTypeRet = reader.readShapeType(beg, end, byteOrder);
  if (!shapeTypeRet.ok()) {
    return GeoShape::UNKNOWN;
  }
  return shapeTypeRet.value();
}

std::unique_ptr<std::string> Geography::asWKT() const {
  auto geomRet = WKBReader().read(wkb);
  if (!geomRet.ok()) {
    LOG(ERROR) << geomRet.status();
    return nullptr;
  }
  auto geom = geomRet.value();
  return std::make_unique<std::string>(WKTWriter().write(geom));
}

std::unique_ptr<std::string> Geography::asWKBHex() const {
  auto geomRet = WKBReader().read(wkb);
  if (!geomRet.ok()) {
    LOG(ERROR) << geomRet.status();
    return nullptr;
  }
  auto geom = geomRet.value();
  return std::make_unique<std::string>(folly::hexlify(WKBWriter().write(geom)));
}

std::unique_ptr<S2Region> Geography::asS2() const {
  auto geomRet = WKBReader().read(wkb);
  if (!geomRet.ok()) {
    LOG(ERROR) << geomRet.status();
    return nullptr;
  }
  auto geom = geomRet.value();
  return GeoUtils::s2RegionFromGeomtry(geom);
}

}  // namespace nebula

namespace std {

// Inject a customized hash function
std::size_t hash<nebula::Geography>::operator()(const nebula::Geography& h) const noexcept {
  return hash<std::string>{}(h.wkb);
}

}  // namespace std
