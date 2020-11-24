/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef UTIL_TOJSON_H_
#define UTIL_TOJSON_H_

#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include <folly/dynamic.h>

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
}   // namespace cpp2
}   // namespace meta

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
}   // namespace cpp2
}   // namespace storage

namespace graph {
struct Variable;
}   // namespace graph
namespace util {

template <typename T>
folly::dynamic toJson(const std::vector<T> &arr);

std::string toJson(const std::string &str);
std::string toJson(int32_t i);
std::string toJson(int64_t i);
std::string toJson(size_t i);
std::string toJson(bool b);

std::string toJson(const HostAddr &addr);
std::string toJson(const List &list);
std::string toJson(const Value &value);
std::string toJson(const EdgeKeyRef *ref);
std::string toJson(const Expression *expr);
folly::dynamic toJson(const meta::cpp2::SpaceDesc &desc);
folly::dynamic toJson(const meta::cpp2::ColumnDef &column);
folly::dynamic toJson(const meta::cpp2::Schema &schema);
folly::dynamic toJson(const meta::cpp2::SchemaProp &prop);
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

}   // namespace util
}   // namespace nebula

#endif   // UTIL_TOJSON_H_
