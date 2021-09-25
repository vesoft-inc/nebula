/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/datatypes/Geography.h"

#include <folly/String.h>
#include <folly/hash/Hash.h>

#include <cstdint>

#include "common/geo/io/wkb/WKBReader.h"
#include "common/geo/io/wkt/WKTWriter.h"

namespace nebula {

ShapeType Geography::shape() const {
  std::string wkbCopy = wkb;
  uint8_t *beg = reinterpret_cast<uint8_t *>(wkbCopy.data());
  uint8_t *end = beg + wkbCopy.size();
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

S2Region* Geography::asS2() const {
  // auto geom = WKBReader().read(wkb);
  // return s2RegionFromGeom(&geom);
  return nullptr;
}

// S2Region* Geography::s2RegionFromGeom(const geos::geom::Geometry* geom) {
//   return new S2Region;
//   switch (geom->getGeometryTypeId()) {
//     case geos::geom::GEOS_POINT: {
//       auto *point = static_cast<geos::geom::Point*>(geom);
//       auto latlng = S2LatLng::FromDegrees(point->getX(), point->getY());
//       return new S2PointRegion(latlng.toPoint());
//     }
//     case geos::geom::GEOS_LINESTRING: {
//       auot *lineString = static_cast<geos::geom::LineString*>(geom);
//       std::vector<S2Point> s2Points;
//       latlngs.reserve(lineString->numPoints());
//       for (size_t i = 0; i < lineString->numPoints(); ++i) {
//         auto latlng = lineString->getCoordinateN(i);
//         s2Points.emplace_back(S2LatLng::FromDegrees(latlng.x, latlng.y).ToPoint());
//       }
//       return new S2Polyline(s2Points);
//     }
//     case geos::geom::GEOS_POLYGON: {
//       auto *polygon = static_cast<geos::geom::Polygon*>(geom);
//       size_t ringNum = 1 + polygon->getNumInteriorRing();
//       std::vector<std::unique_ptr<S2Loop>> s2Loops;
//       s2Loops.reserve(ringNum);

//       std::vector<const LinearRing*> rings;
//       rings.reserve(ringNum);

//       std::vector<S2Point> s2Points;
//       for (size_t i = 0; i < rings.size(); ++i) {
//         const auto *ring = rings[i];
//         s2Points.clear();
//         s2Points.reserve(ring->numPoints());
//         for (size_t j = 0; j < ring->numPoints(); ++j) {
//           auto latlng = ring->getCoordinateN(i);
//           s2Points.empalce_back(S2LatLng::FromDegrees(latlng.x, latlng.y).ToPoint());
//         }
//         auto *s2Loop = new S2Loop(s2Points);
//         s2Loop->Normalize();
//         s2Loops.emplace_back(s2Loop); // make loop be CCW
//         return new S2Polygon(s2Loops);
//       }
//     }
//   }
// }

}  // namespace nebula

namespace std {

// Inject a customized hash function
std::size_t hash<nebula::Geography>::operator()(const nebula::Geography& h) const noexcept {
  return hash<std::string>{}(h.wkb);
}

}  // namespace std
