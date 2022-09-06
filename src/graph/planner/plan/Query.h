/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_PLAN_QUERY_H_
#define GRAPH_PLANNER_PLAN_QUERY_H_

#include "common/expression/AggregateExpression.h"
#include "graph/context/QueryContext.h"
#include "graph/planner/plan/PlanNode.h"
#include "interface/gen-cpp2/storage_types.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"

// All query-related nodes would be put in this file,
// and they are derived from PlanNode.
namespace nebula {
namespace graph {
// Now we have four kind of exploration nodes:
//  GetNeighbors,
//  GetVertices,
//  GetEdges,
//  IndexScan
class Explore : public SingleInputNode {
 public:
  GraphSpaceID space() const {
    return space_;
  }

  void setSpace(GraphSpaceID spaceId) {
    space_ = spaceId;
  }

  bool dedup() const {
    return dedup_;
  }

  // Get the constant limit value
  int64_t limit(QueryContext* qctx = nullptr) const;

  // Get the limit value in runtime
  int64_t limit(QueryExpressionContext& ctx) const {
    return DCHECK_NOTNULL(limit_)->eval(ctx).getInt();
  }

  Expression* limitExpr() const {
    return limit_;
  }

  const Expression* filter() const {
    return filter_;
  }

  Expression* filter() {
    return filter_;
  }

  const std::vector<storage::cpp2::OrderBy>& orderBy() const {
    return orderBy_;
  }

  void setDedup(bool dedup = true) {
    dedup_ = dedup;
  }

  void setLimit(int64_t limit) {
    limit_ = ConstantExpression::make(qctx_->objPool(), limit);
  }

  void setLimit(Expression* limit) {
    limit_ = limit;
  }

  void setFilter(Expression* filter) {
    filter_ = filter;
  }

  void setOrderBy(std::vector<storage::cpp2::OrderBy> orderBy) {
    orderBy_ = std::move(orderBy);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 protected:
  Explore(QueryContext* qctx,
          Kind kind,
          PlanNode* input,
          GraphSpaceID space,
          bool dedup,
          int64_t limit,
          Expression* filter,
          std::vector<storage::cpp2::OrderBy> orderBy)
      : SingleInputNode(qctx, kind, input),
        space_(space),
        dedup_(dedup),
        limit_(ConstantExpression::make(qctx_->objPool(), limit)),
        filter_(std::move(filter)),
        orderBy_(std::move(orderBy)) {}

  Explore(QueryContext* qctx,
          Kind kind,
          PlanNode* input,
          GraphSpaceID space,
          bool dedup,
          Expression* limit,
          Expression* filter,
          std::vector<storage::cpp2::OrderBy> orderBy)
      : SingleInputNode(qctx, kind, input),
        space_(space),
        dedup_(dedup),
        limit_(limit),
        filter_(filter),
        orderBy_(std::move(orderBy)) {}

  Explore(QueryContext* qctx, Kind kind, PlanNode* input, GraphSpaceID space)
      : SingleInputNode(qctx, kind, input), space_(space) {}

  void cloneMembers(const Explore&);

 protected:
  GraphSpaceID space_;
  bool dedup_{false};
  // Use expression to get the limit value in runtime
  // Now for the GetNeighbors/Limit in Loop
  Expression* limit_{nullptr};
  Expression* filter_{nullptr};
  std::vector<storage::cpp2::OrderBy> orderBy_;
};

using VertexProp = nebula::storage::cpp2::VertexProp;
using EdgeProp = nebula::storage::cpp2::EdgeProp;
using StatProp = nebula::storage::cpp2::StatProp;
using Expr = nebula::storage::cpp2::Expr;
using Direction = nebula::storage::cpp2::EdgeDirection;

// Get neighbors' property
class GetNeighbors : public Explore {
 public:
  static GetNeighbors* make(QueryContext* qctx, PlanNode* input, GraphSpaceID space) {
    return qctx->objPool()->makeAndAdd<GetNeighbors>(qctx, Kind::kGetNeighbors, input, space);
  }

  static GetNeighbors* make(QueryContext* qctx,
                            PlanNode* input,
                            GraphSpaceID space,
                            Expression* src,
                            std::vector<EdgeType> edgeTypes,
                            Direction edgeDirection,
                            std::unique_ptr<std::vector<VertexProp>>&& vertexProps,
                            std::unique_ptr<std::vector<EdgeProp>>&& edgeProps,
                            std::unique_ptr<std::vector<StatProp>>&& statProps,
                            std::unique_ptr<std::vector<Expr>>&& exprs,
                            bool dedup = false,
                            bool random = false,
                            std::vector<storage::cpp2::OrderBy> orderBy = {},
                            int64_t limit = -1,
                            Expression* filter = nullptr) {
    auto gn = make(qctx, input, space);
    gn->setSrc(src);
    gn->setEdgeTypes(std::move(edgeTypes));
    gn->setEdgeDirection(edgeDirection);
    gn->setVertexProps(std::move(vertexProps));
    gn->setEdgeProps(std::move(edgeProps));
    gn->setExprs(std::move(exprs));
    gn->setStatProps(std::move(statProps));
    gn->setRandom(random);
    gn->setDedup(dedup);
    gn->setOrderBy(std::move(orderBy));
    gn->setLimit(limit);
    gn->setFilter(filter);
    return gn;
  }

  Expression* src() const {
    return src_;
  }

  storage::cpp2::EdgeDirection edgeDirection() const {
    return edgeDirection_;
  }

  const std::vector<EdgeType>& edgeTypes() const {
    return edgeTypes_;
  }

  const std::vector<VertexProp>* vertexProps() const {
    return vertexProps_.get();
  }

  const std::vector<EdgeProp>* edgeProps() const {
    return edgeProps_.get();
  }

  const std::vector<StatProp>* statProps() const {
    return statProps_.get();
  }

  const std::vector<Expr>* exprs() const {
    return exprs_.get();
  }

  bool random() const {
    return random_;
  }

  void setSrc(Expression* src) {
    src_ = src;
  }

  void setEdgeDirection(Direction direction) {
    edgeDirection_ = direction;
  }

  void setEdgeTypes(std::vector<EdgeType> edgeTypes) {
    edgeTypes_ = std::move(edgeTypes);
  }

  void setVertexProps(std::unique_ptr<std::vector<VertexProp>> vertexProps) {
    vertexProps_ = std::move(vertexProps);
  }

  void setEdgeProps(std::unique_ptr<std::vector<EdgeProp>> edgeProps) {
    edgeProps_ = std::move(edgeProps);
  }

  void setStatProps(std::unique_ptr<std::vector<StatProp>> statProps) {
    statProps_ = std::move(statProps);
  }

  void setExprs(std::unique_ptr<std::vector<Expr>> exprs) {
    exprs_ = std::move(exprs);
  }

  void setRandom(bool random = false) {
    random_ = random;
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 protected:
  friend ObjectPool;
  GetNeighbors(QueryContext* qctx, Kind kind, PlanNode* input, GraphSpaceID space)
      : Explore(qctx, kind, input, space) {
    setLimit(-1);
  }

  void cloneMembers(const GetNeighbors&);

 private:
  Expression* src_{nullptr};
  std::vector<EdgeType> edgeTypes_;
  storage::cpp2::EdgeDirection edgeDirection_{Direction::OUT_EDGE};
  std::unique_ptr<std::vector<VertexProp>> vertexProps_;
  std::unique_ptr<std::vector<EdgeProp>> edgeProps_;
  std::unique_ptr<std::vector<StatProp>> statProps_;
  std::unique_ptr<std::vector<Expr>> exprs_;
  bool random_{false};
};

// Get Edge dst id by src id
class GetDstBySrc : public Explore {
 public:
  static GetDstBySrc* make(QueryContext* qctx,
                           PlanNode* input,
                           GraphSpaceID space,
                           Expression* src = nullptr,
                           std::vector<EdgeType> edgeTypes = {}) {
    return qctx->objPool()->makeAndAdd<GetDstBySrc>(
        qctx, Kind::kGetDstBySrc, input, space, src, std::move(edgeTypes));
  }

  Expression* src() const {
    return src_;
  }

  void setSrc(Expression* src) {
    src_ = src;
  }

  const std::vector<EdgeType>& edgeTypes() const {
    return edgeTypes_;
  }

  void setEdgeTypes(std::vector<EdgeType> edgeTypes) {
    edgeTypes_ = std::move(edgeTypes);
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 protected:
  friend ObjectPool;
  GetDstBySrc(QueryContext* qctx,
              Kind kind,
              PlanNode* input,
              GraphSpaceID space,
              Expression* src,
              std::vector<EdgeType> edgeTypes)
      : Explore(qctx, kind, input, space), src_(src), edgeTypes_(std::move(edgeTypes)) {}

  void cloneMembers(const GetDstBySrc&);

 private:
  // vertices may be parsing from runtime.
  Expression* src_{nullptr};
  std::vector<EdgeType> edgeTypes_;
};

// Get property with given vertex keys.
class GetVertices : public Explore {
 public:
  static GetVertices* make(QueryContext* qctx,
                           PlanNode* input,
                           GraphSpaceID space,
                           Expression* src = nullptr,
                           std::unique_ptr<std::vector<VertexProp>>&& props = nullptr,
                           std::unique_ptr<std::vector<Expr>>&& exprs = nullptr,
                           bool dedup = false,
                           std::vector<storage::cpp2::OrderBy> orderBy = {},
                           int64_t limit = std::numeric_limits<int64_t>::max(),
                           Expression* filter = nullptr) {
    return qctx->objPool()->makeAndAdd<GetVertices>(qctx,
                                                    Kind::kGetVertices,
                                                    input,
                                                    space,
                                                    src,
                                                    std::move(props),
                                                    std::move(exprs),
                                                    dedup,
                                                    std::move(orderBy),
                                                    limit,
                                                    filter);
  }

  Expression* src() const {
    return src_;
  }

  void setSrc(Expression* src) {
    src_ = src;
  }

  const std::vector<VertexProp>* props() const {
    return props_.get();
  }

  const std::vector<Expr>* exprs() const {
    return exprs_.get();
  }

  void setVertexProps(std::unique_ptr<std::vector<VertexProp>> props) {
    props_ = std::move(props);
  }

  void setExprs(std::unique_ptr<std::vector<Expr>> exprs) {
    exprs_ = std::move(exprs);
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 protected:
  friend ObjectPool;
  GetVertices(QueryContext* qctx,
              Kind kind,
              PlanNode* input,
              GraphSpaceID space,
              Expression* src,
              std::unique_ptr<std::vector<VertexProp>>&& props,
              std::unique_ptr<std::vector<Expr>>&& exprs,
              bool dedup,
              std::vector<storage::cpp2::OrderBy> orderBy,
              int64_t limit,
              Expression* filter)
      : Explore(qctx, kind, input, space, dedup, limit, filter, std::move(orderBy)),
        src_(src),
        props_(std::move(props)),
        exprs_(std::move(exprs)) {}

  void cloneMembers(const GetVertices&);

 private:
  // vertices may be parsing from runtime.
  Expression* src_{nullptr};
  // props of the vertex
  std::unique_ptr<std::vector<VertexProp>> props_;
  // expression to get
  std::unique_ptr<std::vector<Expr>> exprs_;
};

// Get property with given edge keys.
class GetEdges final : public Explore {
 public:
  static GetEdges* make(QueryContext* qctx,
                        PlanNode* input,
                        GraphSpaceID space,
                        Expression* src = nullptr,
                        Expression* type = nullptr,
                        Expression* ranking = nullptr,
                        Expression* dst = nullptr,
                        std::unique_ptr<std::vector<EdgeProp>>&& props = nullptr,
                        std::unique_ptr<std::vector<Expr>>&& exprs = nullptr,
                        bool dedup = false,
                        int64_t limit = std::numeric_limits<int64_t>::max(),
                        std::vector<storage::cpp2::OrderBy> orderBy = {},
                        Expression* filter = nullptr) {
    return qctx->objPool()->makeAndAdd<GetEdges>(qctx,
                                                 input,
                                                 space,
                                                 src,
                                                 type,
                                                 ranking,
                                                 dst,
                                                 std::move(props),
                                                 std::move(exprs),
                                                 dedup,
                                                 limit,
                                                 std::move(orderBy),
                                                 filter);
  }

  Expression* src() const {
    return src_;
  }

  Expression* type() const {
    return type_;
  }

  Expression* ranking() const {
    return ranking_;
  }

  Expression* dst() const {
    return dst_;
  }

  const std::vector<EdgeProp>* props() const {
    return props_.get();
  }

  const std::vector<Expr>* exprs() const {
    return exprs_.get();
  }

  void setEdgeProps(std::unique_ptr<std::vector<EdgeProp>> props) {
    props_ = std::move(props);
  }

  void setExprs(std::unique_ptr<std::vector<Expr>> exprs) {
    exprs_ = std::move(exprs);
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  GetEdges(QueryContext* qctx,
           PlanNode* input,
           GraphSpaceID space,
           Expression* src,
           Expression* type,
           Expression* ranking,
           Expression* dst,
           std::unique_ptr<std::vector<EdgeProp>>&& props,
           std::unique_ptr<std::vector<Expr>>&& exprs,
           bool dedup,
           int64_t limit,
           std::vector<storage::cpp2::OrderBy> orderBy,
           Expression* filter)
      : Explore(qctx, Kind::kGetEdges, input, space, dedup, limit, filter, std::move(orderBy)),
        src_(src),
        type_(type),
        ranking_(ranking),
        dst_(dst),
        props_(std::move(props)),
        exprs_(std::move(exprs)) {}

  void cloneMembers(const GetEdges&);

 private:
  // edges_ may be parsed from runtime.
  Expression* src_{nullptr};
  Expression* type_{nullptr};
  Expression* ranking_{nullptr};
  Expression* dst_{nullptr};
  // props of edge to get
  std::unique_ptr<std::vector<EdgeProp>> props_;
  // expression to show
  std::unique_ptr<std::vector<Expr>> exprs_;
};

// Read data through the index.
class IndexScan : public Explore {
 public:
  using IndexQueryContext = storage::cpp2::IndexQueryContext;

  static IndexScan* make(QueryContext* qctx,
                         PlanNode* input,
                         GraphSpaceID space = -1,  //  TBD: -1 is inValid spaceID?
                         std::vector<IndexQueryContext>&& contexts = {},
                         std::vector<std::string> returnCols = {},
                         bool isEdge = false,
                         int32_t schemaId = -1,
                         bool isEmptyResultSet = false,
                         bool dedup = false,
                         std::vector<storage::cpp2::OrderBy> orderBy = {},
                         int64_t limit = std::numeric_limits<int64_t>::max(),
                         Expression* filter = nullptr) {
    return qctx->objPool()->makeAndAdd<IndexScan>(qctx,
                                                  input,
                                                  space,
                                                  std::move(contexts),
                                                  std::move(returnCols),
                                                  isEdge,
                                                  schemaId,
                                                  isEmptyResultSet,
                                                  dedup,
                                                  std::move(orderBy),
                                                  limit,
                                                  filter);
  }

  const std::vector<IndexQueryContext>& queryContext() const {
    return contexts_;
  }

  const std::vector<std::string>& returnColumns() const {
    return returnCols_;
  }

  bool isEdge() const {
    return isEdge_;
  }

  int32_t schemaId() const {
    return schemaId_;
  }

  void setSchemaId(int32_t schema) {
    schemaId_ = schema;
  }

  bool isEmptyResultSet() const {
    return isEmptyResultSet_;
  }

  YieldColumns* yieldColumns() const {
    return yieldColumns_;
  }

  void setEmptyResultSet(bool isEmptyResultSet) {
    isEmptyResultSet_ = isEmptyResultSet;
  }

  void setIndexQueryContext(std::vector<IndexQueryContext> contexts) {
    contexts_ = std::move(contexts);
  }

  void setReturnCols(std::vector<std::string> cols) {
    returnCols_ = std::move(cols);
  }

  void setIsEdge(bool isEdge) {
    isEdge_ = isEdge;
  }

  void setYieldColumns(YieldColumns* yieldColumns) {
    yieldColumns_ = yieldColumns;
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 protected:
  friend ObjectPool;
  IndexScan(QueryContext* qctx,
            PlanNode* input,
            GraphSpaceID space,
            std::vector<IndexQueryContext>&& contexts,
            std::vector<std::string>&& returnCols,
            bool isEdge,
            int32_t schemaId,
            bool isEmptyResultSet,
            bool dedup,
            std::vector<storage::cpp2::OrderBy> orderBy,
            int64_t limit,
            Expression* filter,
            Kind kind = Kind::kIndexScan)
      : Explore(qctx, kind, input, space, dedup, limit, filter, std::move(orderBy)) {
    contexts_ = std::move(contexts);
    returnCols_ = std::move(returnCols);
    isEdge_ = isEdge;
    schemaId_ = schemaId;
    isEmptyResultSet_ = isEmptyResultSet;
  }

  void cloneMembers(const IndexScan&);

 private:
  std::vector<IndexQueryContext> contexts_;
  std::vector<std::string> returnCols_;
  bool isEdge_;
  int32_t schemaId_;

  // TODO(yee): Generate special plan for this scenario
  bool isEmptyResultSet_{false};
  YieldColumns* yieldColumns_;
};

// Scan vertices
class ScanVertices final : public Explore {
 public:
  static ScanVertices* make(QueryContext* qctx,
                            PlanNode* input,
                            GraphSpaceID space,
                            std::unique_ptr<std::vector<VertexProp>>&& props = nullptr,
                            std::unique_ptr<std::vector<Expr>>&& exprs = nullptr,
                            bool dedup = false,
                            std::vector<storage::cpp2::OrderBy> orderBy = {},
                            int64_t limit = -1,
                            Expression* filter = nullptr) {
    return qctx->objPool()->makeAndAdd<ScanVertices>(qctx,
                                                     input,
                                                     space,
                                                     std::move(props),
                                                     std::move(exprs),
                                                     dedup,
                                                     std::move(orderBy),
                                                     limit,
                                                     filter);
  }

  const std::vector<VertexProp>* props() const {
    return props_.get();
  }

  const std::vector<Expr>* exprs() const {
    return exprs_.get();
  }

  void setVertexProps(std::unique_ptr<std::vector<VertexProp>> props) {
    props_ = std::move(props);
  }

  void setExprs(std::unique_ptr<std::vector<Expr>> exprs) {
    exprs_ = std::move(exprs);
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  ScanVertices(QueryContext* qctx,
               PlanNode* input,
               GraphSpaceID space,
               std::unique_ptr<std::vector<VertexProp>>&& props,
               std::unique_ptr<std::vector<Expr>>&& exprs,
               bool dedup,
               std::vector<storage::cpp2::OrderBy> orderBy,
               int64_t limit,
               Expression* filter)
      : Explore(qctx, Kind::kScanVertices, input, space, dedup, limit, filter, std::move(orderBy)),
        props_(std::move(props)),
        exprs_(std::move(exprs)) {}

  void cloneMembers(const ScanVertices&);

 private:
  // props of the vertex
  std::unique_ptr<std::vector<VertexProp>> props_;
  // expression to get
  std::unique_ptr<std::vector<Expr>> exprs_;
};

// Scan edges
class ScanEdges final : public Explore {
 public:
  static ScanEdges* make(QueryContext* qctx,
                         PlanNode* input,
                         GraphSpaceID space,
                         std::unique_ptr<std::vector<EdgeProp>>&& props = nullptr,
                         std::unique_ptr<std::vector<Expr>>&& exprs = nullptr,
                         bool dedup = false,
                         int64_t limit = -1,
                         std::vector<storage::cpp2::OrderBy> orderBy = {},
                         Expression* filter = nullptr) {
    return qctx->objPool()->makeAndAdd<ScanEdges>(qctx,
                                                  input,
                                                  space,
                                                  std::move(props),
                                                  std::move(exprs),
                                                  dedup,
                                                  limit,
                                                  std::move(orderBy),
                                                  filter);
  }

  const std::vector<EdgeProp>* props() const {
    return props_.get();
  }

  const std::vector<Expr>* exprs() const {
    return exprs_.get();
  }

  void setEdgeProps(std::unique_ptr<std::vector<EdgeProp>> props) {
    props_ = std::move(props);
  }

  void setExprs(std::unique_ptr<std::vector<Expr>> exprs) {
    exprs_ = std::move(exprs);
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

  void accept(PlanNodeVisitor* visitor) override;

 private:
  friend ObjectPool;
  ScanEdges(QueryContext* qctx,
            PlanNode* input,
            GraphSpaceID space,
            std::unique_ptr<std::vector<EdgeProp>>&& props,
            std::unique_ptr<std::vector<Expr>>&& exprs,
            bool dedup,
            int64_t limit,
            std::vector<storage::cpp2::OrderBy> orderBy,
            Expression* filter)
      : Explore(qctx, Kind::kScanEdges, input, space, dedup, limit, filter, std::move(orderBy)),
        props_(std::move(props)),
        exprs_(std::move(exprs)) {}

  void cloneMembers(const ScanEdges&);

 private:
  // props of edge to get
  std::unique_ptr<std::vector<EdgeProp>> props_;
  // expression to show
  std::unique_ptr<std::vector<Expr>> exprs_;
};

// A Filter node helps filt some records with condition.
class Filter final : public SingleInputNode {
 public:
  static Filter* make(QueryContext* qctx,
                      PlanNode* input,
                      Expression* condition = nullptr,
                      bool needStableFilter = false) {
    return qctx->objPool()->makeAndAdd<Filter>(qctx, input, condition, needStableFilter);
  }

  Expression* condition() const {
    return condition_;
  }

  void setCondition(Expression* condition) {
    condition_ = condition;
  }

  bool needStableFilter() const {
    return needStableFilter_;
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

  void accept(PlanNodeVisitor* visitor) override;

 private:
  friend ObjectPool;
  Filter(QueryContext* qctx, PlanNode* input, Expression* condition, bool needStableFilter);
  void cloneMembers(const Filter&);

 private:
  // Remain result when true
  Expression* condition_{nullptr};
  bool needStableFilter_;
};

// Now we have three kind of set operations:
//   UNION,
//   INTERSECT,
//   MINUS
class SetOp : public BinaryInputNode {
 protected:
  SetOp(QueryContext* qctx, Kind kind, PlanNode* left, PlanNode* right)
      : BinaryInputNode(qctx, kind, left, right) {
    DCHECK(kind == Kind::kUnion || kind == Kind::kIntersect || kind == Kind::kMinus);
  }

  void cloneMembers(const SetOp&);
};

// Combine two set of records.
class Union final : public SetOp {
 public:
  static Union* make(QueryContext* qctx, PlanNode* left, PlanNode* right) {
    return qctx->objPool()->makeAndAdd<Union>(qctx, left, right);
  }

  void accept(PlanNodeVisitor* visitor) override;

  PlanNode* clone() const override;

 private:
  friend ObjectPool;
  Union(QueryContext* qctx, PlanNode* left, PlanNode* right)
      : SetOp(qctx, Kind::kUnion, left, right) {}

  void cloneMembers(const Union&);
};

// Return the intersected records between two sets.
class Intersect final : public SetOp {
 public:
  static Intersect* make(QueryContext* qctx, PlanNode* left, PlanNode* right) {
    return qctx->objPool()->makeAndAdd<Intersect>(qctx, left, right);
  }

  PlanNode* clone() const override;

 private:
  friend ObjectPool;
  Intersect(QueryContext* qctx, PlanNode* left, PlanNode* right)
      : SetOp(qctx, Kind::kIntersect, left, right) {}

  void cloneMembers(const Intersect&);
};

// Do subtraction between two sets.
class Minus final : public SetOp {
 public:
  static Minus* make(QueryContext* qctx, PlanNode* left, PlanNode* right) {
    return qctx->objPool()->makeAndAdd<Minus>(qctx, left, right);
  }

  PlanNode* clone() const override;

 private:
  friend ObjectPool;
  Minus(QueryContext* qctx, PlanNode* left, PlanNode* right)
      : SetOp(qctx, Kind::kMinus, left, right) {}

  void cloneMembers(const Minus&);
};

// Project is used to specify output vars or field.
class Project final : public SingleInputNode {
 public:
  static Project* make(QueryContext* qctx, PlanNode* input, YieldColumns* cols = nullptr) {
    return qctx->objPool()->makeAndAdd<Project>(qctx, input, cols);
  }

  const YieldColumns* columns() const {
    return cols_;
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

  void accept(PlanNodeVisitor* visitor) override;

 private:
  friend ObjectPool;
  Project(QueryContext* qctx, PlanNode* input, YieldColumns* cols);

  void cloneMembers(const Project&);

 private:
  YieldColumns* cols_{nullptr};
};

// Thansforms a list to column.
class Unwind final : public SingleInputNode {
 public:
  static Unwind* make(QueryContext* qctx,
                      PlanNode* input,
                      Expression* unwindExpr = nullptr,
                      std::string alias = "") {
    return qctx->objPool()->makeAndAdd<Unwind>(qctx, input, unwindExpr, alias);
  }

  Expression* unwindExpr() const {
    return unwindExpr_;
  }

  const std::string& alias() const {
    return alias_;
  }

  bool fromPipe() const {
    return fromPipe_;
  }

  void setFromPipe(bool fromPipe) {
    fromPipe_ = fromPipe;
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

  void accept(PlanNodeVisitor* visitor) override;

 private:
  friend ObjectPool;
  Unwind(QueryContext* qctx, PlanNode* input, Expression* unwindExpr, std::string alias)
      : SingleInputNode(qctx, Kind::kUnwind, input), unwindExpr_(unwindExpr), alias_(alias) {}

  void cloneMembers(const Unwind&);

 private:
  Expression* unwindExpr_{nullptr};
  std::string alias_;
  bool fromPipe_{false};
};

// Sort the given record set.
class Sort final : public SingleInputNode {
 public:
  static Sort* make(QueryContext* qctx,
                    PlanNode* input,
                    std::vector<std::pair<size_t, OrderFactor::OrderType>> factors = {}) {
    return qctx->objPool()->makeAndAdd<Sort>(qctx, input, std::move(factors));
  }

  const std::vector<std::pair<size_t, OrderFactor::OrderType>>& factors() const {
    return factors_;
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  Sort(QueryContext* qctx,
       PlanNode* input,
       std::vector<std::pair<size_t, OrderFactor::OrderType>> factors)
      : SingleInputNode(qctx, Kind::kSort, input) {
    factors_ = std::move(factors);
  }

  std::vector<std::pair<std::string, std::string>> factorsString() const {
    std::vector<std::pair<std::string, std::string>> result;
    result.resize(factors_.size());
    auto cols = colNames();
    auto get = [&cols](const std::pair<size_t, OrderFactor::OrderType>& factor) {
      auto colName = cols[factor.first];
      auto order = factor.second == OrderFactor::OrderType::ASCEND ? "ASCEND" : "DESCEND";
      return std::pair<std::string, std::string>{colName, order};
    };
    std::transform(factors_.begin(), factors_.end(), result.begin(), get);

    return result;
  }

  void cloneMembers(const Sort&);

 private:
  std::vector<std::pair<size_t, OrderFactor::OrderType>> factors_;
};

// Output the records with the given limitation.
class Limit final : public SingleInputNode {
 public:
  static Limit* make(QueryContext* qctx, PlanNode* input, int64_t offset = -1, int64_t count = -1) {
    return qctx->objPool()->makeAndAdd<Limit>(qctx, input, offset, count);
  }

  static Limit* make(QueryContext* qctx,
                     PlanNode* input,
                     int64_t offset = -1,
                     Expression* count = nullptr) {
    return qctx->objPool()->makeAndAdd<Limit>(qctx, input, offset, count);
  }

  int64_t offset() const {
    return offset_;
  }

  // Get constant count value
  int64_t count(QueryContext* qctx = nullptr) const;
  // Get count in runtime
  int64_t count(QueryExpressionContext& ctx) const {
    if (count_ == nullptr) {
      return -1;
    }
    auto v = count_->eval(ctx);
    auto s = v.getInt();
    DCHECK_GE(s, 0);
    return s;
  }

  const Expression* countExpr() const {
    return count_;
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  Limit(QueryContext* qctx, PlanNode* input, int64_t offset, int64_t count)
      : SingleInputNode(qctx, Kind::kLimit, input) {
    offset_ = offset;
    count_ = ConstantExpression::make(qctx_->objPool(), count);
  }

  Limit(QueryContext* qctx, PlanNode* input, int64_t offset, Expression* count)
      : SingleInputNode(qctx, Kind::kLimit, input) {
    offset_ = offset;
    count_ = count;
  }

  void cloneMembers(const Limit&);

 private:
  int64_t offset_{-1};
  Expression* count_{nullptr};
};

// Get the Top N record set.
class TopN final : public SingleInputNode {
 public:
  static TopN* make(QueryContext* qctx,
                    PlanNode* input,
                    std::vector<std::pair<size_t, OrderFactor::OrderType>> factors = {},
                    int64_t offset = 0,
                    int64_t count = 0) {
    return qctx->objPool()->makeAndAdd<TopN>(qctx, input, std::move(factors), offset, count);
  }

  const std::vector<std::pair<size_t, OrderFactor::OrderType>>& factors() const {
    return factors_;
  }

  int64_t offset() const {
    return offset_;
  }

  int64_t count() const {
    return count_;
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  TopN(QueryContext* qctx,
       PlanNode* input,
       std::vector<std::pair<size_t, OrderFactor::OrderType>> factors,
       int64_t offset,
       int64_t count)
      : SingleInputNode(qctx, Kind::kTopN, input) {
    factors_ = std::move(factors);
    DCHECK_GE(offset, 0);
    DCHECK_GE(count, 0);
    offset_ = offset;
    count_ = count;
  }

  std::vector<std::pair<std::string, std::string>> factorsString() const {
    std::vector<std::pair<std::string, std::string>> result;
    result.resize(factors_.size());
    auto cols = colNames();
    auto get = [&cols](const std::pair<size_t, OrderFactor::OrderType>& factor) {
      auto colName = cols[factor.first];
      auto order = factor.second == OrderFactor::OrderType::ASCEND ? "ASCEND" : "DESCEND";
      return std::pair<std::string, std::string>{colName, order};
    };
    std::transform(factors_.begin(), factors_.end(), result.begin(), get);

    return result;
  }

  void cloneMembers(const TopN&);

 private:
  std::vector<std::pair<size_t, OrderFactor::OrderType>> factors_;
  int64_t offset_{-1};
  int64_t count_{-1};
};

// Sample the given input data.
class Sample final : public SingleInputNode {
 public:
  static Sample* make(QueryContext* qctx, PlanNode* input, const int64_t count) {
    return qctx->objPool()->makeAndAdd<Sample>(qctx, input, count);
  }

  static Sample* make(QueryContext* qctx, PlanNode* input, Expression* count) {
    return qctx->objPool()->makeAndAdd<Sample>(qctx, input, count);
  }

  // Get constant count
  int64_t count() const;

  // Get Runtime count
  int64_t count(QueryExpressionContext& qec) const {
    auto count = count_->eval(qec).getInt();
    DCHECK_GE(count, 0);
    return count;
  }

  Expression* countExpr() const {
    return count_;
  }

  void setCount(int64_t count) {
    DCHECK_GE(count, 0);
    count_ = ConstantExpression::make(qctx_->objPool(), count);
  }

  void setCount(Expression* count) {
    count_ = DCHECK_NOTNULL(count);
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  Sample(QueryContext* qctx, PlanNode* input, int64_t count)
      : SingleInputNode(qctx, Kind::kSample, input),
        count_(ConstantExpression::make(qctx->objPool(), count)) {}

  Sample(QueryContext* qctx, PlanNode* input, Expression* count)
      : SingleInputNode(qctx, Kind::kSample, input), count_(count) {}

  void cloneMembers(const Sample&);

 private:
  Expression* count_{nullptr};
};

// Do Aggregation with the given set of records,
// such as AVG(), COUNT()...
class Aggregate final : public SingleInputNode {
 public:
  static Aggregate* make(QueryContext* qctx,
                         PlanNode* input,
                         std::vector<Expression*>&& groupKeys = {},
                         std::vector<Expression*>&& groupItems = {}) {
    return qctx->objPool()->makeAndAdd<Aggregate>(
        qctx, input, std::move(groupKeys), std::move(groupItems));
  }

  const std::vector<Expression*>& groupKeys() const {
    return groupKeys_;
  }

  const std::vector<Expression*>& groupItems() const {
    return groupItems_;
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

  void accept(PlanNodeVisitor* visitor) override;

 private:
  friend ObjectPool;
  Aggregate(QueryContext* qctx,
            PlanNode* input,
            std::vector<Expression*>&& groupKeys,
            std::vector<Expression*>&& groupItems)
      : SingleInputNode(qctx, Kind::kAggregate, input) {
    groupKeys_ = std::move(groupKeys);
    groupItems_ = std::move(groupItems);
  }

  void cloneMembers(const Aggregate&);

 private:
  std::vector<Expression*> groupKeys_;
  std::vector<Expression*> groupItems_;
};

// Change the space that specified for current session.
class SwitchSpace final : public SingleInputNode {
 public:
  static SwitchSpace* make(QueryContext* qctx, PlanNode* input, std::string spaceName) {
    return qctx->objPool()->makeAndAdd<SwitchSpace>(qctx, input, spaceName);
  }

  const std::string& getSpaceName() const {
    return spaceName_;
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  SwitchSpace(QueryContext* qctx, PlanNode* input, std::string spaceName)
      : SingleInputNode(qctx, Kind::kSwitchSpace, input) {
    spaceName_ = std::move(spaceName);
  }

  void cloneMembers(const SwitchSpace&);

 private:
  std::string spaceName_;
};

// Dedup the rows.
class Dedup final : public SingleInputNode {
 public:
  static Dedup* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<Dedup>(qctx, input);
  }

  PlanNode* clone() const override;

 private:
  friend ObjectPool;
  Dedup(QueryContext* qctx, PlanNode* input);

  void cloneMembers(const Dedup&);
};

// Collect the variable results.
class DataCollect final : public VariableDependencyNode {
 public:
  enum class DCKind : uint8_t {
    kSubgraph,
    kRowBasedMove,
    kMToN,
    kBFSShortest,
    kAllPaths,
    kMultiplePairShortest,
    kPathProp,
  };

  static DataCollect* make(QueryContext* qctx, DCKind kind) {
    return qctx->objPool()->makeAndAdd<DataCollect>(qctx, kind);
  }

  void setMToN(StepClause step) {
    step_ = std::move(step);
  }

  void setDistinct(bool distinct) {
    distinct_ = distinct;
  }

  void setInputVars(const std::vector<std::string>& vars) {
    inputVars_.clear();
    for (auto& var : vars) {
      readVariable(var);
    }
  }

  DCKind kind() const {
    return kind_;
  }

  std::vector<std::string> vars() const {
    std::vector<std::string> vars(inputVars_.size());
    std::transform(
        inputVars_.begin(), inputVars_.end(), vars.begin(), [](auto& var) { return var->name; });
    return vars;
  }

  StepClause step() const {
    return step_;
  }

  bool distinct() const {
    return distinct_;
  }

  void setColType(std::vector<Value::Type>&& colType) {
    colType_ = std::move(colType);
  }

  const std::vector<Value::Type>& colType() const {
    return colType_;
  }

  PlanNode* clone() const override;

  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  DataCollect(QueryContext* qctx, DCKind kind)
      : VariableDependencyNode(qctx, Kind::kDataCollect), kind_(kind) {}

  void cloneMembers(const DataCollect&);

 private:
  DCKind kind_;
  // using for m to n steps
  StepClause step_;
  std::vector<Value::Type> colType_;
  bool distinct_{false};
};

// Join two result set based on the keys
// We have LeftJoin and InnerJoin now.
class Join : public SingleDependencyNode {
 public:
  const std::pair<std::string, int64_t>& leftVar() const {
    return leftVar_;
  }

  const std::pair<std::string, int64_t>& rightVar() const {
    return rightVar_;
  }

  void setLeftVar(std::pair<std::string, int64_t> lvar) {
    setInputVar(lvar.first, 0);
    leftVar_ = lvar;
  }

  void setRightVar(std::pair<std::string, int64_t> rvar) {
    setInputVar(rvar.first, 1);
    rightVar_ = rvar;
  }

  const std::vector<Expression*>& hashKeys() const {
    return hashKeys_;
  }

  const std::vector<Expression*>& probeKeys() const {
    return probeKeys_;
  }

  void setHashKeys(std::vector<Expression*> newHashKeys) {
    hashKeys_ = newHashKeys;
  }

  void setProbeKeys(std::vector<Expression*> newProbeKeys) {
    probeKeys_ = newProbeKeys;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

 protected:
  Join(QueryContext* qctx,
       Kind kind,
       PlanNode* input,
       std::pair<std::string, int64_t> leftVar,
       std::pair<std::string, int64_t> rightVar,
       std::vector<Expression*> hashKeys,
       std::vector<Expression*> probeKeys);

  void cloneMembers(const Join&);

 protected:
  // var name, var version
  std::pair<std::string, int64_t> leftVar_;
  std::pair<std::string, int64_t> rightVar_;
  std::vector<Expression*> hashKeys_;
  std::vector<Expression*> probeKeys_;
};

// Left join
class LeftJoin final : public Join {
 public:
  static LeftJoin* make(QueryContext* qctx,
                        PlanNode* input,
                        std::pair<std::string, int64_t> leftVar,
                        std::pair<std::string, int64_t> rightVar,
                        std::vector<Expression*> hashKeys = {},
                        std::vector<Expression*> probeKeys = {}) {
    return qctx->objPool()->makeAndAdd<LeftJoin>(qctx,
                                                 input,
                                                 std::move(leftVar),
                                                 std::move(rightVar),
                                                 std::move(hashKeys),
                                                 std::move(probeKeys));
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  LeftJoin(QueryContext* qctx,
           PlanNode* input,
           std::pair<std::string, int64_t> leftVar,
           std::pair<std::string, int64_t> rightVar,
           std::vector<Expression*> hashKeys,
           std::vector<Expression*> probeKeys)
      : Join(qctx,
             Kind::kLeftJoin,
             input,
             std::move(leftVar),
             std::move(rightVar),
             std::move(hashKeys),
             std::move(probeKeys)) {}

  void cloneMembers(const LeftJoin&);
};

// Inner join
class InnerJoin final : public Join {
 public:
  static InnerJoin* make(QueryContext* qctx,
                         PlanNode* input,
                         std::pair<std::string, int64_t> leftVar,
                         std::pair<std::string, int64_t> rightVar,
                         std::vector<Expression*> hashKeys = {},
                         std::vector<Expression*> probeKeys = {}) {
    return qctx->objPool()->makeAndAdd<InnerJoin>(qctx,
                                                  input,
                                                  std::move(leftVar),
                                                  std::move(rightVar),
                                                  std::move(hashKeys),
                                                  std::move(probeKeys));
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  InnerJoin(QueryContext* qctx,
            PlanNode* input,
            std::pair<std::string, int64_t> leftVar,
            std::pair<std::string, int64_t> rightVar,
            std::vector<Expression*> hashKeys,
            std::vector<Expression*> probeKeys)
      : Join(qctx,
             Kind::kInnerJoin,
             input,
             std::move(leftVar),
             std::move(rightVar),
             std::move(hashKeys),
             std::move(probeKeys)) {}

  void cloneMembers(const InnerJoin&);
};

// Binding a value to a variable
class Assign final : public SingleInputNode {
 public:
  static Assign* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<Assign>(qctx, input);
  }

  const std::vector<std::pair<std::string, Expression*>>& items() const {
    return items_;
  }

  void assignVar(std::string var, Expression* value) {
    auto* varPtr = qctx_->symTable()->getVar(var);
    DCHECK(varPtr != nullptr);
    DCHECK(value != nullptr);
    items_.emplace_back(std::make_pair(std::move(var), value));
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  Assign(QueryContext* qctx, PlanNode* input) : SingleInputNode(qctx, Kind::kAssign, input) {}

  void cloneMembers(const Assign&);

 private:
  std::vector<std::pair<std::string, Expression*>> items_;
};

// Union all versions of the variable in the dependency
// The input is a single PlanNode
class UnionAllVersionVar final : public SingleInputNode {
 public:
  static UnionAllVersionVar* make(QueryContext* qctx, PlanNode* input) {
    return qctx->objPool()->makeAndAdd<UnionAllVersionVar>(qctx, input);
  }

  PlanNode* clone() const override;

 private:
  friend ObjectPool;
  UnionAllVersionVar(QueryContext* qctx, PlanNode* input)
      : SingleInputNode(qctx, Kind::kUnionAllVersionVar, input) {}

  void cloneMembers(const UnionAllVersionVar&);
};

// Traverse several steps based on condition.
class Traverse final : public GetNeighbors {
 public:
  using VertexProps = std::unique_ptr<std::vector<storage::cpp2::VertexProp>>;
  using EdgeProps = std::unique_ptr<std::vector<storage::cpp2::EdgeProp>>;
  using StatProps = std::unique_ptr<std::vector<storage::cpp2::StatProp>>;
  using Exprs = std::unique_ptr<std::vector<storage::cpp2::Expr>>;

  static Traverse* make(QueryContext* qctx, PlanNode* input, GraphSpaceID space) {
    return qctx->objPool()->makeAndAdd<Traverse>(qctx, input, space);
  }

  static Traverse* make(QueryContext* qctx,
                        PlanNode* input,
                        GraphSpaceID space,
                        Expression* src,
                        std::vector<EdgeType> edgeTypes,
                        storage::cpp2::EdgeDirection edgeDirection,
                        VertexProps&& vertexProps,
                        EdgeProps&& edgeProps,
                        StatProps&& statProps,
                        Exprs&& exprs,
                        bool dedup = false,
                        bool random = false,
                        std::vector<storage::cpp2::OrderBy> orderBy = {},
                        int64_t limit = -1,
                        Expression* filter = nullptr) {
    auto traverse = make(qctx, input, space);
    traverse->setSrc(src);
    traverse->setEdgeTypes(std::move(edgeTypes));
    traverse->setEdgeDirection(edgeDirection);
    traverse->setVertexProps(std::move(vertexProps));
    traverse->setEdgeProps(std::move(edgeProps));
    traverse->setExprs(std::move(exprs));
    traverse->setStatProps(std::move(statProps));
    traverse->setRandom(random);
    traverse->setDedup(dedup);
    traverse->setOrderBy(std::move(orderBy));
    traverse->setLimit(limit);
    traverse->setFilter(std::move(filter));
    return traverse;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  void accept(PlanNodeVisitor* visitor) override;

  Traverse* clone() const override;

  MatchStepRange* stepRange() const {
    return range_;
  }

  // Contains zero step
  bool zeroStep() const {
    return range_ != nullptr && range_->min() == 0;
  }

  Expression* vFilter() const {
    return vFilter_;
  }

  Expression* eFilter() const {
    return eFilter_;
  }

  bool trackPrevPath() const {
    return trackPrevPath_;
  }

  void setStepRange(MatchStepRange* range) {
    range_ = range;
  }

  void setVertexFilter(Expression* vFilter) {
    vFilter_ = vFilter;
  }

  void setEdgeFilter(Expression* eFilter) {
    eFilter_ = eFilter;
  }

  void setTrackPrevPath(bool track = true) {
    trackPrevPath_ = track;
  }

  Expression* firstStepFilter() const {
    return firstStepFilter_;
  }

  void setFirstStepFilter(Expression* filter) {
    firstStepFilter_ = filter;
  }

 private:
  friend ObjectPool;
  Traverse(QueryContext* qctx, PlanNode* input, GraphSpaceID space)
      : GetNeighbors(qctx, Kind::kTraverse, input, space) {
    setLimit(-1);
  }

 private:
  void cloneMembers(const Traverse& g);

  MatchStepRange* range_{nullptr};
  Expression* vFilter_{nullptr};
  Expression* eFilter_{nullptr};
  bool trackPrevPath_{true};
  // Push down filter in first step
  Expression* firstStepFilter_{nullptr};
};

// Append vertices to a path.
class AppendVertices final : public GetVertices {
 public:
  static AppendVertices* make(QueryContext* qctx, PlanNode* input, GraphSpaceID space) {
    return qctx->objPool()->makeAndAdd<AppendVertices>(qctx, input, space);
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  void accept(PlanNodeVisitor* visitor) override;

  AppendVertices* clone() const override;

  Expression* vFilter() const {
    return vFilter_;
  }

  bool trackPrevPath() const {
    return trackPrevPath_;
  }

  void setVertexFilter(Expression* vFilter) {
    vFilter_ = vFilter;
  }

  void setTrackPrevPath(bool track = true) {
    trackPrevPath_ = track;
  }

 private:
  friend ObjectPool;
  AppendVertices(QueryContext* qctx, PlanNode* input, GraphSpaceID space)
      : GetVertices(qctx,
                    Kind::kAppendVertices,
                    input,
                    space,
                    nullptr,
                    nullptr,
                    nullptr,
                    false,
                    {},
                    -1,  // means no limit
                    nullptr) {}

  void cloneMembers(const AppendVertices& a);

  Expression* vFilter_;

  bool trackPrevPath_{true};
};

// Binary Join that joins two results from two inputs.
class BiJoin : public BinaryInputNode {
 public:
  const std::vector<Expression*>& hashKeys() const {
    return hashKeys_;
  }

  const std::vector<Expression*>& probeKeys() const {
    return probeKeys_;
  }

  void setHashKeys(std::vector<Expression*> newHashKeys) {
    hashKeys_ = newHashKeys;
  }

  void setProbeKeys(std::vector<Expression*> newProbeKeys) {
    probeKeys_ = newProbeKeys;
  }

  std::unique_ptr<PlanNodeDescription> explain() const override;

  void accept(PlanNodeVisitor* visitor) override;

 protected:
  BiJoin(QueryContext* qctx,
         Kind kind,
         PlanNode* left,
         PlanNode* right,
         std::vector<Expression*> hashKeys,
         std::vector<Expression*> probeKeys);

  void cloneMembers(const BiJoin&);

 protected:
  std::vector<Expression*> hashKeys_;
  std::vector<Expression*> probeKeys_;
};

// Left join
class BiLeftJoin final : public BiJoin {
 public:
  static BiLeftJoin* make(QueryContext* qctx,
                          PlanNode* left,
                          PlanNode* right,
                          std::vector<Expression*> hashKeys = {},
                          std::vector<Expression*> probeKeys = {}) {
    return qctx->objPool()->makeAndAdd<BiLeftJoin>(
        qctx, left, right, std::move(hashKeys), std::move(probeKeys));
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  BiLeftJoin(QueryContext* qctx,
             PlanNode* left,
             PlanNode* right,
             std::vector<Expression*> hashKeys,
             std::vector<Expression*> probeKeys)
      : BiJoin(qctx, Kind::kBiLeftJoin, left, right, std::move(hashKeys), std::move(probeKeys)) {}

  void cloneMembers(const BiLeftJoin&);
};

// Inner join
class BiInnerJoin final : public BiJoin {
 public:
  static BiInnerJoin* make(QueryContext* qctx,
                           PlanNode* left,
                           PlanNode* right,
                           std::vector<Expression*> hashKeys = {},
                           std::vector<Expression*> probeKeys = {}) {
    return qctx->objPool()->makeAndAdd<BiInnerJoin>(
        qctx, left, right, std::move(hashKeys), std::move(probeKeys));
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

 private:
  friend ObjectPool;
  BiInnerJoin(QueryContext* qctx,
              PlanNode* left,
              PlanNode* right,
              std::vector<Expression*> hashKeys,
              std::vector<Expression*> probeKeys)
      : BiJoin(qctx, Kind::kBiInnerJoin, left, right, std::move(hashKeys), std::move(probeKeys)) {}

  void cloneMembers(const BiInnerJoin&);
};

// Roll Up Apply two results from two inputs.
class RollUpApply : public BinaryInputNode {
 public:
  static RollUpApply* make(QueryContext* qctx,
                           PlanNode* left,
                           PlanNode* right,
                           std::vector<Expression*> compareCols,
                           InputPropertyExpression* collectCol) {
    return qctx->objPool()->makeAndAdd<RollUpApply>(
        qctx, Kind::kRollUpApply, left, right, std::move(compareCols), collectCol);
  }

  const std::vector<Expression*>& compareCols() const {
    return compareCols_;
  }

  const InputPropertyExpression* collectCol() const {
    return collectCol_;
  }

  InputPropertyExpression* collectCol() {
    return collectCol_;
  }

  PlanNode* clone() const override;
  std::unique_ptr<PlanNodeDescription> explain() const override;

  void accept(PlanNodeVisitor* visitor) override;

 protected:
  friend ObjectPool;
  RollUpApply(QueryContext* qctx,
              Kind kind,
              PlanNode* left,
              PlanNode* right,
              std::vector<Expression*> compareCols,
              InputPropertyExpression* collectCol);

  void cloneMembers(const RollUpApply&);

 protected:
  // Collect columns when compare column equal
  std::vector<Expression*> compareCols_;
  // Collect column to List
  InputPropertyExpression* collectCol_;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_PLAN_QUERY_H_
