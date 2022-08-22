/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_DATATYPES_LIST_H_
#define COMMON_DATATYPES_LIST_H_

#include <algorithm>
#include <vector>

#include "common/datatypes/Value.h"

namespace nebula {

struct List {
  std::vector<Value> values;

  List() = default;
  List(const List&) = default;
  List(List&&) noexcept = default;
  explicit List(std::vector<Value>&& vals) {
    values = std::move(vals);
  }
  explicit List(const std::vector<Value>& l) : values(l) {}

  bool empty() const {
    return values.empty();
  }

  void reserve(std::size_t n) {
    values.reserve(n);
  }

  template <typename T,
            typename = typename std::enable_if<std::is_convertible<T, Value>::value>::type>
  void emplace_back(T&& v) {
    values.emplace_back(std::forward<T>(v));
  }

  void append(List&& other) {
    values.reserve(size() + other.size());
    values.insert(values.end(),
                  std::make_move_iterator(other.values.begin()),
                  std::make_move_iterator(other.values.end()));
  }

  void clear() {
    values.clear();
  }

  void __clear() {
    clear();
  }

  List& operator=(const List& rhs) {
    if (this == &rhs) {
      return *this;
    }
    values = rhs.values;
    return *this;
  }
  List& operator=(List&& rhs) noexcept {
    if (this == &rhs) {
      return *this;
    }
    values = std::move(rhs.values);
    return *this;
  }

  bool operator==(const List& rhs) const {
    return values == rhs.values;
  }

  bool operator<(const List& rhs) const {
    return values < rhs.values;
  }

  const Value& operator[](size_t i) const {
    return values[i];
  }

  bool contains(const Value& value) const {
    return std::find(values.begin(), values.end(), value) != values.end();
  }

  size_t size() const {
    return values.size();
  }

  std::string toString() const;
  folly::dynamic toJson() const;
  // Extract the metadata of each element
  folly::dynamic getMetaData() const;
};

inline std::ostream& operator<<(std::ostream& os, const List& l) {
  return os << l.toString();
}

}  // namespace nebula

namespace std {
template <>
struct hash<nebula::List> {
  std::size_t operator()(const nebula::List& h) const noexcept {
    if (h.values.size() == 1) {
      return std::hash<nebula::Value>()(h.values[0]);
    }
    size_t seed = 0;
    for (auto& v : h.values) {
      seed ^= hash<nebula::Value>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }
    return seed;
  }
};

template <>
struct equal_to<nebula::List*> {
  bool operator()(const nebula::List* lhs, const nebula::List* rhs) const {
    return lhs == rhs ? true : (lhs != nullptr) && (rhs != nullptr) && (*lhs == *rhs);
  }
};

template <>
struct equal_to<const nebula::List*> {
  bool operator()(const nebula::List* lhs, const nebula::List* rhs) const {
    return lhs == rhs ? true : (lhs != nullptr) && (rhs != nullptr) && (*lhs == *rhs);
  }
};

template <>
struct hash<nebula::List*> {
  size_t operator()(const nebula::List* row) const {
    return !row ? 0 : hash<nebula::List>()(*row);
  }
};

template <>
struct hash<const nebula::List*> {
  size_t operator()(const nebula::List* row) const {
    return !row ? 0 : hash<nebula::List>()(*row);
  }
};

}  // namespace std
#endif  // COMMON_DATATYPES_LIST_H_
