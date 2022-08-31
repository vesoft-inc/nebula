/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
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

std::ostream& operator<<(std::ostream& os, const GeoShape& shape) {
  switch (shape) {
    case GeoShape::POINT: {
      os << "POINT";
      break;
    }
    case GeoShape::LINESTRING: {
      os << "LINESTRING";
      break;
    }
    case GeoShape::POLYGON: {
      os << "POLYGON";
      break;
    }
    case GeoShape::UNKNOWN:
    default: {
      os << "__UNKNOWN__";
      break;
    }
  }

  return os;
}

constexpr double kMaxLongitude = 180.0;
constexpr double kMaxLatitude = 90.0;

void Coordinate::normalize() {
  // Reduce the x(longitude) to the range [-180, 180] degrees
  x = std::remainder(x, 360.0);

  // Reduce the y(latitude) to the range [-90, 90] degrees
  double tmp = remainder(y, 360.0);
  if (tmp > kMaxLatitude) {
    y = 180.0 - tmp;
  } else if (tmp < -kMaxLatitude) {
    y = -180.0 - tmp;
  }
}

Status Coordinate::isValid() const {
  if (std::abs(x) > kMaxLongitude) {
    return Status::Error("Invalid longitude: %f", x);
  }
  if (std::abs(y) > kMaxLatitude) {
    return Status::Error("Invalid latitude: %f", y);
  }
  return Status::OK();
}

std::string Coordinate::toString() const {
  return "(" + std::to_string(x) + " " + std::to_string(y) + ")";
}

void Point::normalize() {}

Status Point::isValid() const {
  return coord.isValid();
}

void LineString::normalize() {
  geo::GeoUtils::removeAdjacentDuplicateCoordinates(coordList);
}

Status LineString::isValid() const {
  if (coordList.size() < 2) {
    return Status::Error("LineString contains %zu coordinates, requires at least 2 coordinates",
                         coordList.size());
  }
  for (size_t i = 0; i < coordList.size(); ++i) {
    auto& coord = coordList[i];
    auto status = coord.isValid();
    if (!status.ok()) {
      return Status::Error("The %zu-th coordinate is invalid: %s", i, status.message().c_str());
    }
  }
  auto s2Region = geo::GeoUtils::s2RegionFromGeography(*this);
  CHECK_NOTNULL(s2Region);

  S2Error error;
  if (static_cast<S2Polyline*>(s2Region.get())->FindValidationError(&error)) {
    std::stringstream ss;
    ss << "FindValidationError, S2 Errorcode: " << error.code() << ", message: " << error.text();
    return Status::Error(ss.str());
  }
  return Status::OK();
}

void Polygon::normalize() {
  for (auto& coordList : coordListList) {
    geo::GeoUtils::removeAdjacentDuplicateCoordinates(coordList);
  }
}

Status Polygon::isValid() const {
  for (size_t i = 0; i < coordListList.size(); ++i) {
    auto& coordList = coordListList[i];
    if (coordList.size() < 4) {
      return Status::Error(
          "Polygon's %zu-th LinearRing contains %zu coordinates, requires at least 4",
          i,
          coordList.size());
    }
    if (coordList.front() != coordList.back()) {
      return Status::Error("Polygon's %zu-th LinearRing is not closed", i);
    }
    for (size_t j = 0; j < coordList.size(); ++j) {
      auto& coord = coordList[j];
      auto status = coord.isValid();
      if (!status.ok()) {
        return Status::Error("The %zu-th coordinate of %zu-th LinearRing is invalid: %s",
                             j,
                             i,
                             status.message().c_str());
      }
    }
  }
  auto s2Region = geo::GeoUtils::s2RegionFromGeography(*this);
  CHECK_NOTNULL(s2Region);

  S2Error error;
  if (static_cast<S2Polygon*>(s2Region.get())->FindValidationError(&error)) {
    std::stringstream ss;
    ss << "FindValidationError, S2 Errorcode: " << error.code() << ", message: " << error.text();
    return Status::Error(ss.str());
  }
  return Status::OK();
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
    auto status = geog.isValid();
    if (!status.ok()) {
      std::stringstream ss;
      ss << "The geography " << geog.toString() << " built from wkt " << wkt
         << " is invalid, error: " << status.message();
      return Status::Error(ss.str());
    }
  }

  return geog;
}

StatusOr<Geography> Geography::fromWKB(const std::string& wkb,
                                       bool needNormalize,
                                       bool verifyValidity) {
  auto geogRet = geo::WKBReader().read(wkb);
  if (!geogRet.ok()) {
    return geogRet;
  }
  auto geog = std::move(geogRet).value();
  if (needNormalize) {
    geog.normalize();
  }
  if (verifyValidity) {
    auto status = geog.isValid();
    if (!status.ok()) {
      std::stringstream ss;
      ss << "The geography " << geog.toString() << " built from wkb " << wkb
         << " is invalid, error: " << status.message();
      return Status::Error(ss.str());
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

Status Geography::isValid() const {
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
      return Status::Error(
          "Geography shapes other than Point/LineString/Polygon are not currently supported");
    }
  }
}

Point Geography::centroid() const {
  switch (shape()) {
    case GeoShape::POINT: {
      return this->point();
    }
    case GeoShape::LINESTRING: {
      auto s2Region = geo::GeoUtils::s2RegionFromGeography(*this);
      CHECK_NOTNULL(s2Region);
      S2Point s2Point = static_cast<S2Polyline*>(s2Region.get())->GetCentroid();
      return Point(geo::GeoUtils::coordinateFromS2Point(s2Point));
    }
    case GeoShape::POLYGON: {
      auto s2Region = geo::GeoUtils::s2RegionFromGeography(*this);
      CHECK_NOTNULL(s2Region);
      S2Point s2Point = static_cast<S2Polygon*>(s2Region.get())->GetCentroid();
      return Point(geo::GeoUtils::coordinateFromS2Point(s2Point));
    }
    case GeoShape::UNKNOWN:
    default: {
      LOG(ERROR)
          << "Geography shapes other than Point/LineString/Polygon are not currently supported";
      return {};
    }
  }
}

std::string Geography::asWKT() const {
  return geo::WKTWriter().write(*this);
}

std::string Geography::asWKB() const {
  return geo::WKBWriter().write(*this);
}

std::string Geography::asWKBHex() const {
  return folly::hexlify(geo::WKBWriter().write(*this));
}

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
