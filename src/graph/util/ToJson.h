// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_UTIL_TOJSON_H_
#define GRAPH_UTIL_TOJSON_H_

#include <folly/dynamic.h>

#include <iterator>
#include <utility>

namespace nebula {

class EdgeKeyRef;
class Expression;
struct List;
struct Value;
struct HostAddr;

namespace meta {
namespace cpp2 {
class SpaceDesc;
class AlterSchemaItem;
class ColumnDef;
class Schema;
class SchemaProp;
class IndexParams;
}  // namespace cpp2
}  // namespace meta

namespace storage {
namespace cpp2 {
class EdgeKey;
class NewTag;
class NewEdge;
class NewVertex;
class UpdatedProp;
class OrderBy;
class VertexProp;
class EdgeProp;
class StatProp;
class Expr;
class IndexQueryContext;
class IndexColumnHint;
}  // namespace cpp2
}  // namespace storage

namespace graph {
struct Variable;
}  // namespace graph
namespace util {

// Converts all elements in arr to JSON objects, returns a dynamic::array
template <typename T>
folly::dynamic toJson(const std::vector<T> &arr);

folly::dynamic toJson(const std::string &str);
folly::dynamic toJson(int32_t i);
folly::dynamic toJson(int64_t i);
folly::dynamic toJson(size_t i);
folly::dynamic toJson(bool b);

std::string toJson(const HostAddr &addr);
std::string toJson(const List &list);
folly::dynamic toJson(const Value &value);
std::string toJson(const EdgeKeyRef *ref);
std::string toJson(const Expression *expr);

// Concerts the given object to a dynamic object
folly::dynamic toJson(const meta::cpp2::SpaceDesc &desc);
folly::dynamic toJson(const meta::cpp2::ColumnDef &column);
folly::dynamic toJson(const meta::cpp2::Schema &schema);
folly::dynamic toJson(const meta::cpp2::SchemaProp &prop);
folly::dynamic toJson(const meta::cpp2::IndexParams &indexParams);
folly::dynamic toJson(const meta::cpp2::AlterSchemaItem &item);
folly::dynamic toJson(const storage::cpp2::EdgeKey &edgeKey);
folly::dynamic toJson(const storage::cpp2::NewTag &tag);
folly::dynamic toJson(const storage::cpp2::NewVertex &vert);
folly::dynamic toJson(const storage::cpp2::NewEdge &edge);
folly::dynamic toJson(const storage::cpp2::UpdatedProp &prop);
folly::dynamic toJson(const storage::cpp2::OrderBy &orderBy);
folly::dynamic toJson(const storage::cpp2::VertexProp &prop);
folly::dynamic toJson(const storage::cpp2::EdgeProp &prop);
folly::dynamic toJson(const storage::cpp2::StatProp &prop);
folly::dynamic toJson(const storage::cpp2::Expr &expr);
folly::dynamic toJson(const storage::cpp2::IndexQueryContext &iqc);
folly::dynamic toJson(const storage::cpp2::IndexColumnHint &hints);
folly::dynamic toJson(const graph::Variable *var);

// Converts std::pair p into a dynamic object where the key is p.first and value if p.second
template <typename K, typename V>
folly::dynamic toJson(const std::pair<K, V> &p) {
  return folly::dynamic::object(toJson(p.first), toJson(p.second));
}

template <typename T>
folly::dynamic toJson(const std::vector<T> &arr) {
  auto farr = folly::dynamic::array();
  std::transform(
      arr.cbegin(), arr.cend(), std::back_inserter(farr), [](const T &t) { return toJson(t); });
  return farr;
}

}  // namespace util
}  // namespace nebula

#endif  // GRAPH_UTIL_TOJSON_H_
