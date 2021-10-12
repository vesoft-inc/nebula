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

void Coordinate::normalize() {
  // Reduce the x(longitude) to the range [-180, 180] degrees
  x = std::remainder(x, 360.0);

  // Reduce the y(latitude) to the range [-90, 90] degrees
  double tmp = remainder(y, 360.0);
  if (tmp > 90.0) {
    y = 180.0 - tmp;
  } else if (tmp < -90.0) {
    y = -180.0 - tmp;
  }
}

bool Coordinate::isValid() const { return std::abs(x) <= 180.0 && std::abs(y) <= 90.0; }

void Point::normalize() {}

bool Point::isValid() const { return coord.isValid(); }

void LineString::normalize() { geo::GeoUtils::removeAdjacentDuplicateCoordinates(coordList); }

bool LineString::isValid() const {
  // LineString must have at least 2 coordinates;
  if (coordList.size() < 2) {
    return false;
  }
  auto s2Region = geo::GeoUtils::s2RegionFromGeography(*this);
  return static_cast<S2Polyline*>(s2Region.get())->IsValid();
}

void Polygon::normalize() {
  for (auto& coordList : coordListList) {
    geo::GeoUtils::removeAdjacentDuplicateCoordinates(coordList);
  }
}

bool Polygon::isValid() const {
  for (const auto& coordList : coordListList) {
    // Polygon's LinearRing must have at least 4 coordinates
    if (coordList.size() < 4) {
      return false;
    }
    // Polygon's LinearRing must be closed
    if (coordList.front() != coordList.back()) {
      return false;
    }
  }
  auto s2Region = geo::GeoUtils::s2RegionFromGeography(*this);
  return static_cast<S2Polygon*>(s2Region.get())->IsValid();
}

StatusOr<Geography> Geography::fromWKT(const std::string& wkt,
                                       bool needNormalize,
                                       bool verifyValidity) {
  auto geogRet = geo::WKTReader().read(wkt);
  if (!geogRet.ok()) {
    return geogRet;
  }
  auto geog = std::move(geogRet).value();
  if (needNormalize) {
    geog.normalize();
  }
  if (verifyValidity) {
    if (!geog.isValid()) {
      return Status::Error("Failed to parse an valid Geography instance from the wkt `%s'",
                           wkt.c_str());
    }
  }

  return geog;
}

GeoShape Geography::shape() const {
  switch (geo_.index()) {
    case 0:
      return GeoShape::POINT;
    case 1:
      return GeoShape::LINESTRING;
    case 2:
      return GeoShape::POLYGON;
    default:  // May never reaches here, because the default constructor of the variant geo_ will
              // hold the value-initialized value of the first alternative(Point).
      return GeoShape::UNKNOWN;
  }
}

const Point& Geography::point() const {
  CHECK(std::holds_alternative<Point>(geo_));
  return std::get<Point>(geo_);
}

const LineString& Geography::lineString() const {
  CHECK(std::holds_alternative<LineString>(geo_));
  return std::get<LineString>(geo_);
}

const Polygon& Geography::polygon() const {
  CHECK(std::holds_alternative<Polygon>(geo_));
  return std::get<Polygon>(geo_);
}

Point& Geography::mutablePoint() {
  CHECK(std::holds_alternative<Point>(geo_));
  return std::get<Point>(geo_);
}

LineString& Geography::mutableLineString() {
  CHECK(std::holds_alternative<LineString>(geo_));
  return std::get<LineString>(geo_);
}

Polygon& Geography::mutablePolygon() {
  CHECK(std::holds_alternative<Polygon>(geo_));
  return std::get<Polygon>(geo_);
}

void Geography::normalize() {
  switch (shape()) {
    case GeoShape::POINT: {
      auto& point = mutablePoint();
      point.normalize();
      return;
    }
    case GeoShape::LINESTRING: {
      auto& line = mutableLineString();
      line.normalize();
      return;
    }
    case GeoShape::POLYGON: {
      auto& polygon = mutablePolygon();
      polygon.normalize();
      return;
    }
    case GeoShape::UNKNOWN:
    default: {
      LOG(ERROR)
          << "Geography shapes other than Point/LineString/Polygon are not currently supported";
      return;
    }
  }
}

bool Geography::isValid() const {
  switch (shape()) {
    case GeoShape::POINT: {
      const auto& point = this->point();
      return point.isValid();
    }
    case GeoShape::LINESTRING: {
      const auto& line = this->lineString();
      return line.isValid();
    }
    case GeoShape::POLYGON: {
      const auto& polygon = this->polygon();
      return polygon.isValid();
    }
    case GeoShape::UNKNOWN:
    default: {
      LOG(ERROR)
          << "Geography shapes other than Point/LineString/Polygon are not currently supported";
      return false;
    }
  }
}

std::string Geography::asWKT() const { return geo::WKTWriter().write(*this); }

std::string Geography::asWKB() const { return geo::WKBWriter().write(*this); }

std::string Geography::asWKBHex() const { return folly::hexlify(geo::WKBWriter().write(*this)); }

std::unique_ptr<S2Region> Geography::asS2() const {
  return geo::GeoUtils::s2RegionFromGeography(*this);
}

bool Geography::operator==(const Geography& rhs) const {
  auto lhsShape = shape();
  auto rhsShape = rhs.shape();
  if (lhsShape != rhsShape) {
    return false;
  }

  switch (lhsShape) {
    case GeoShape::POINT: {
      return point() == rhs.point();
    }
    case GeoShape::LINESTRING: {
      return lineString() == rhs.lineString();
    }
    case GeoShape::POLYGON: {
      return polygon() == rhs.polygon();
    }
    case GeoShape::UNKNOWN:
    default: {
      LOG(ERROR) << "Geography shapes other than Point/LineString/Polygon are not currently "
                    "supported";
      return false;
    }
  }
}

bool Geography::operator<(const Geography& rhs) const {
  auto lhsShape = shape();
  auto rhsShape = rhs.shape();
  if (lhsShape != rhsShape) {
    return lhsShape < rhsShape;
  }

  switch (lhsShape) {
    case GeoShape::POINT: {
      return point() < rhs.point();
    }
    case GeoShape::LINESTRING: {
      return lineString() < rhs.lineString();
    }
    case GeoShape::POLYGON: {
      return polygon() < rhs.polygon();
    }
    case GeoShape::UNKNOWN:
    default: {
      LOG(ERROR) << "Geography shapes other than Point/LineString/Polygon are not currently "
                    "supported";
      return false;
    }
  }
  return false;
}

}  // namespace nebula

namespace std {

// Inject a customized hash function
std::size_t hash<nebula::Geography>::operator()(const nebula::Geography& v) const noexcept {
  std::string wkb = v.asWKB();
  return hash<std::string>{}(wkb);
}

}  // namespace std
