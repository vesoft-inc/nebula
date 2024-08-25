/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_DATATYPES_SET_H_
#define COMMON_DATATYPES_SET_H_

#include <unordered_set>

#include "common/datatypes/Value.h"

namespace nebula {

struct Set {
  std::unordered_set<Value> values;

  Set() = default;
  Set(const Set&) = default;
  Set(Set&&) noexcept = default;
  explicit Set(std::unordered_set<Value> value) {
    values = std::move(value);
  }

  // Template Static Factory Method Declaration
  template <typename T>
  static Set createFromVector(const std::vector<T>& items);

  void clear() {
    values.clear();
  }

  void __clear() {
    clear();
  }

  std::string toString() const;
  folly::dynamic toJson() const;
  // Extract the metadata of each element
  folly::dynamic getMetaData() const;
  static Set set_intersection(const Set& lhs, const Set& rhs);

  Set& operator=(const Set& rhs) {
    if (this == &rhs) {
      return *this;
    }
    values = rhs.values;
    return *this;
  }

  Set& operator=(Set&& rhs) noexcept {
    if (this == &rhs) {
      return *this;
    }
    values = std::move(rhs.values);
    return *this;
  }

  bool operator==(const Set& rhs) const {
    return values == rhs.values;
  }

  bool contains(const Value& value) const {
    return values.count(value) != 0;
  }

  size_t size() const {
    return values.size();
  }
};

inline std::ostream& operator<<(std::ostream& os, const Set& s) {
  return os << s.toString();
}

// define using template static factory method
template <typename T>
inline Set Set::createFromVector(const std::vector<T>& items) {
  std::unordered_set<Value> values;
  for (const auto& item : items) {
    values.emplace(Value(item));
  }
  return Set(std::move(values));
}

}  // namespace nebula

namespace std {
template <>
struct hash<nebula::Set> {
  std::size_t operator()(const nebula::Set& s) const;
};

}  // namespace std
#endif  // COMMON_DATATYPES_SET_H_
