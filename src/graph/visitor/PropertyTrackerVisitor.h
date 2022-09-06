/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VISITOR_PROPERTYTRACKERVISITOR_H
#define GRAPH_VISITOR_PROPERTYTRACKERVISITOR_H

#include "common/base/Status.h"
#include "common/thrift/ThriftTypes.h"
#include "graph/visitor/ExprVisitorImpl.h"

namespace nebula {

class Expression;

namespace graph {

class QueryContext;

struct PropertyTracker {
  std::unordered_map<std::string, std::unordered_map<TagID, std::unordered_set<std::string>>>
      vertexPropsMap;
  std::unordered_map<std::string, std::unordered_map<EdgeType, std::unordered_set<std::string>>>
      edgePropsMap;
  std::unordered_set<std::string> colsSet;

  Status update(const std::string& oldName, const std::string& newName);
  bool hasAlias(const std::string& name) const;
  void insertVertexProp(const std::string& name, TagID tagId, const std::string& propName);
  void insertEdgeProp(const std::string& name, EdgeType type, const std::string& propName);
  void insertCols(const std::string& name);
};

class PropertyTrackerVisitor : public ExprVisitorImpl {
 public:
  PropertyTrackerVisitor(const QueryContext* qctx,
                         GraphSpaceID space,
                         PropertyTracker& propsUsed,
                         const std::string& entityAlias);

  bool ok() const override {
    return status_.ok();
  }

  const Status& status() const {
    return status_;
  }

 private:
  using ExprVisitorImpl::visit;
  void visit(TagPropertyExpression* expr) override;
  void visit(EdgePropertyExpression* expr) override;
  void visit(LabelTagPropertyExpression* expr) override;
  void visit(InputPropertyExpression* expr) override;
  void visit(VariablePropertyExpression* expr) override;
  void visit(AttributeExpression* expr) override;
  void visit(FunctionCallExpression* expr) override;

  void visit(SourcePropertyExpression* expr) override;
  void visit(DestPropertyExpression* expr) override;
  void visit(EdgeSrcIdExpression* expr) override;
  void visit(EdgeTypeExpression* expr) override;
  void visit(EdgeRankExpression* expr) override;
  void visit(EdgeDstIdExpression* expr) override;
  void visit(UUIDExpression* expr) override;
  void visit(VariableExpression* expr) override;
  void visit(VersionedVariableExpression* expr) override;
  void visit(LabelExpression* expr) override;
  void visit(LabelAttributeExpression* expr) override;
  void visit(ConstantExpression* expr) override;
  void visit(VertexExpression* expr) override;
  void visit(EdgeExpression* expr) override;
  void visit(ColumnExpression* expr) override;
  void visit(AggregateExpression* expr) override;

  const QueryContext* qctx_{nullptr};
  const int unKnowType_{0};
  GraphSpaceID space_;
  PropertyTracker& propsUsed_;
  std::string entityAlias_;

  Status status_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VISITOR_PROPERTYTRACKERVISITOR_H
