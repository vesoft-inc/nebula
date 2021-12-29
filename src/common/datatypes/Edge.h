/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_DATATYPES_EDGE_H_
#define COMMON_DATATYPES_EDGE_H_

#include <unordered_map>

#include "common/datatypes/Value.h"
#include "common/thrift/ThriftTypes.h"

namespace nebula {

struct Edge {
  Value src;
  Value dst;
  EdgeType type;
  std::string name;
  EdgeRanking ranking;
  std::unordered_map<std::string, Value> props;

  Edge() {}
  Edge(Edge&& v) noexcept
      : src(std::move(v.src)),
        dst(std::move(v.dst)),
        type(std::move(v.type)),
        name(std::move(v.name)),
        ranking(std::move(v.ranking)),
        props(std::move(v.props)) {}
  Edge(const Edge& v)
      : src(v.src), dst(v.dst), type(v.type), name(v.name), ranking(v.ranking), props(v.props) {}
  Edge(Value s,
       Value d,
       EdgeType t,
       std::string n,
       EdgeRanking r,
       std::unordered_map<std::string, Value>&& p)
      : src(std::move(s)),
        dst(std::move(d)),
        type(std::move(t)),
        name(std::move(n)),
        ranking(std::move(r)),
        props(std::move(p)) {}

  void clear();

  void __clear() {
    clear();
  }

  std::string toString() const;
  folly::dynamic toJson() const;
  // Used in Json form query result
  folly::dynamic getMetaData() const;

  bool operator==(const Edge& rhs) const;

  void format() {
    if (type < 0) {
      reverse();
    }
  }

  void reverse();

  bool operator<(const Edge& rhs) const;

  bool contains(const Value& key) const;

  const Value& value(const std::string& key) const;

  bool keyEqual(const Edge& rhs) const;
};

inline std::ostream& operator<<(std::ostream& os, const Edge& v) {
  return os << v.toString();
}

}  // namespace nebula

namespace std {

// Inject a customized hash function
template <>
struct hash<nebula::Edge> {
  std::size_t operator()(const nebula::Edge& h) const noexcept;
};

}  // namespace std
#endif  // COMMON_DATATYPES_EDGE_H_
