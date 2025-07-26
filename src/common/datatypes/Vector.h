/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_DATATYPES_VECTOR_H_
#define COMMON_DATATYPES_VECTOR_H_
#include <folly/dynamic.h>

#include <cassert>
#include <cmath>
#include <iostream>
#include <sstream>
#include <vector>
namespace nebula {
/*
 * Vector Type is consist of a vector of values and dimension of vector.
 * The element type in vector is float.
 */
struct Vector {
  std::vector<float> values;

  Vector() = default;
  Vector(const Vector& rhs) = default;
  Vector(Vector&& rhs) noexcept = default;
  explicit Vector(const std::vector<float>& vals) : values(vals) {}
  explicit Vector(std::vector<float>&& vals) : values(std::move(vals)) {}

  Vector& operator=(const Vector& rhs) {
    if (this == &rhs) {
      return *this;
    }
    values = rhs.values;
    return *this;
  }

  Vector& operator=(Vector&& rhs) noexcept {
    if (this == &rhs) {
      return *this;
    }
    values = std::move(rhs.values);
    return *this;
  }

  size_t dim() const {
    return values.size();
  }

  std::vector<float> data() const {
    return values;
  }

  inline static bool floatcmp(float a,
                              float b,
                              double relative_tolerance = 1e-6,
                              double absolute_tolerance = 1e-6) {
    if (a == b) {
      return true;
    }
    double diff = std::fabs(a - b);
    if (diff <= absolute_tolerance) {
      return true;
    }
    return diff <= relative_tolerance * std::max(std::fabs(a), std::fabs(b));
  }

  bool operator==(const Vector& rhs) const {
    size_t dimension = values.size();
    for (size_t i = 0; i < static_cast<size_t>(dimension); ++i) {
      if (!floatcmp(values[i], rhs.values[i])) {
        return false;
      }
    }
    return true;
  }

  std::string toString() const;
  folly::dynamic toJson() const;
};

inline std::ostream& operator<<(std::ostream& os, const Vector& s) {
  return os << s.toString();
}

}  // namespace nebula

namespace std {
template <>
struct hash<nebula::Vector> {
  std::size_t operator()(const nebula::Vector& h) const {
    if (h.values.size() == 1) {
      return std::hash<float>()(h.values[0]);
    }
    size_t seed = 0;
    for (auto& v : h.values) {
      seed ^= hash<float>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

template <>
struct equal_to<nebula::Vector*> {
  bool operator()(const nebula::Vector* lhs, const nebula::Vector* rhs) const {
    return lhs == rhs ? true : (lhs != nullptr) && (rhs != nullptr) && (*lhs == *rhs);
  }
};

template <>
struct equal_to<const nebula::Vector*> {
  bool operator()(const nebula::Vector* lhs, const nebula::Vector* rhs) const {
    return lhs == rhs ? true : (lhs != nullptr) && (rhs != nullptr) && (*lhs == *rhs);
  }
};

template <>
struct hash<nebula::Vector*> {
  size_t operator()(const nebula::Vector* row) const {
    return !row ? 0 : hash<nebula::Vector>()(*row);
  }
};

template <>
struct hash<const nebula::Vector*> {
  size_t operator()(const nebula::Vector* row) const {
    return !row ? 0 : hash<nebula::Vector>()(*row);
  }
};

}  // namespace std

#endif  // COMMON_DATATYPES_VECTOR_H_
