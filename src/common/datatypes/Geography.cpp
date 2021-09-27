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

StatusOr<std::unique_ptr<Geography>> Geography::fromWKT(const std::string& wkt) {
  auto geomRet = WKTReader().read(wkt);
  NG_RETURN_IF_ERROR(geomRet);
  auto geom = std::move(geomRet).value();
  auto wkb = WKBWriter().write(geom.get());
  return std::make_unique<Geography>(wkb);
}

GeoShape Geography::shape() const {
  // TODO(jie) May store the shapetype as the data member of Geography is ok.
  std::string wkbCopy = wkb;
  uint8_t* beg = reinterpret_cast<uint8_t*>(wkbCopy.data());
  uint8_t* end = beg + wkbCopy.size();
  WKBReader reader;
  auto byteOrderRet = reader.readByteOrder(beg, end);
  DCHECK(byteOrderRet.ok());
  ByteOrder byteOrder = byteOrderRet.value();
  auto shapeTypeRet = reader.readShapeType(beg, end, byteOrder);
  DCHECK(shapeTypeRet.ok());
  return shapeTypeRet.value();
}

std::string Geography::asWKT() const {
  auto geomRet = WKBReader().read(wkb);
  DCHECK(geomRet.ok());
  std::unique_ptr<Geometry> geom = std::move(geomRet).value();
  std::string wkt = WKTWriter().write(geom.get());
  LOG(INFO) << "Geography::asWKT(): " << wkt;
  return wkt;
}

std::string Geography::asWKB() const { return folly::hexlify(wkb); }

std::unique_ptr<S2Region> Geography::asS2() const {
  auto geomRet = WKBReader().read(wkb);
  DCHECK(geomRet.ok());
  std::unique_ptr<Geometry> geom = std::move(geomRet).value();
  return GeoUtils::s2RegionFromGeomtry(geom.get());
}

}  // namespace nebula

namespace std {

// Inject a customized hash function
std::size_t hash<nebula::Geography>::operator()(const nebula::Geography& h) const noexcept {
  return hash<std::string>{}(h.wkb);
}

}  // namespace std
