// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_UTIL_SCHEMAUTIL_H_
#define GRAPH_UTIL_SCHEMAUTIL_H_

#include "common/base/StatusOr.h"
#include "common/datatypes/DataSet.h"
#include "common/expression/Expression.h"
#include "common/meta/NebulaSchemaProvider.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/meta_types.h"
#include "parser/MaintainSentences.h"

namespace nebula {
namespace graph {
class QueryContext;
struct SpaceInfo;
struct EdgeInfo;
class SchemaUtil final {
 public:
  using VertexProp = nebula::storage::cpp2::VertexProp;
  using EdgeProp = nebula::storage::cpp2::EdgeProp;
  SchemaUtil() = delete;

 public:
  // Iterates schemaProps and sets the each shchemaProp into schema.
  // Returns Status error when when failed to set.
  static Status validateProps(const std::vector<SchemaPropItem*>& schemaProps,
                              meta::cpp2::Schema& schema);

  // Generates a NebulaSchemaProvider that contains schema info from the schema.
  static std::shared_ptr<const meta::NebulaSchemaProvider> generateSchemaProvider(
      const SchemaVer ver, const meta::cpp2::Schema& schema);

  // Sets TTLDuration from shcemaProp into shcema.
  static Status setTTLDuration(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

  // Sets TTLCol from shcemaProp into shcema.
  static Status setTTLCol(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

  // Sets Comment from shcemaProp into shcema.
  static Status setComment(SchemaPropItem* schemaProp, meta::cpp2::Schema& schema);

  // Calculates the vid value from expr.
  // If the result value type mismatches vidType, returns Status error.
  static StatusOr<Value> toVertexID(Expression* expr, Value::Type vidType);

  // Iterate exprs and calculate each element's value and return them as a vector.
  static StatusOr<std::vector<Value>> toValueVec(QueryContext* qctx,
                                                 std::vector<Expression*> exprs);

  // Returns the "Field", "Type", "Null", "Default", "Comment" of the schema as a dataset
  static StatusOr<DataSet> toDescSchema(const meta::cpp2::Schema& schema);

  // Returns the schema info of the given tag/edge as a dataset.
  static StatusOr<DataSet> toShowCreateSchema(bool isTag,
                                              const std::string& name,
                                              const meta::cpp2::Schema& schema);

  static std::string typeToString(const meta::cpp2::ColumnTypeDef& col);
  static std::string typeToString(const meta::cpp2::ColumnDef& col);

  // Returns the corresponding Value type of the given PropertyType.
  static Value::Type propTypeToValueType(nebula::cpp2::PropertyType propType);

  // Validates whether the value type matches the ColumnTypeDef.
  static bool isValidVid(const Value& value, const meta::cpp2::ColumnTypeDef& type);
  static bool isValidVid(const Value& value, nebula::cpp2::PropertyType type);
  static bool isValidVid(const Value& value);

  // Fetches all tags in the space and retrieve props from tags.
  // Only take _tag when withProp is false.
  static StatusOr<std::unique_ptr<std::vector<VertexProp>>> getAllVertexProp(QueryContext* qctx,
                                                                             GraphSpaceID spaceId,
                                                                             bool withProp);

  // Retrieves prop from specific edgetypes.
  // Only takes _src _dst _type _rank when withProp is false.
  static StatusOr<std::unique_ptr<std::vector<EdgeProp>>> getEdgeProps(
      QueryContext* qctx,
      const SpaceInfo& space,
      const std::vector<EdgeType>& edgeTypes,
      bool withProp);

  // Retrieves prop from EdgeInfo.
  static std::unique_ptr<std::vector<storage::cpp2::EdgeProp>> getEdgeProps(const EdgeInfo& edge,
                                                                            bool reversely,
                                                                            QueryContext* qctx,
                                                                            GraphSpaceID spaceId);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_UTIL_SCHEMAUTIL_H_
