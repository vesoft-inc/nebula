/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_PLAN_QUERY_H_
#define PLANNER_PLAN_QUERY_H_

#include "common/expression/AggregateExpression.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "context/QueryContext.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"
#include "planner/plan/PlanNode.h"

/**
 * All query-related nodes would be put in this file,
 * and they are derived from PlanNode.
 */
namespace nebula {
namespace graph {
/**
 * Now we hava four kind of exploration nodes:
 *  GetNeighbors,
 *  GetVertices,
 *  GetEdges,
 *  IndexScan
 */
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

    int64_t limit() const {
        return limit_;
    }

    const std::string& filter() const {
        return filter_;
    }

    const std::vector<storage::cpp2::OrderBy>& orderBy() const {
        return orderBy_;
    }

    void setDedup(bool dedup = true) {
        dedup_ = dedup;
    }

    void setLimit(int64_t limit) {
        limit_ = limit;
    }

    void setFilter(std::string filter) {
        filter_ = std::move(filter);
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
            std::string filter,
            std::vector<storage::cpp2::OrderBy> orderBy)
        : SingleInputNode(qctx, kind, input),
          space_(space),
          dedup_(dedup),
          limit_(limit),
          filter_(std::move(filter)),
          orderBy_(std::move(orderBy)) {}

    Explore(QueryContext* qctx, Kind kind, PlanNode* input, GraphSpaceID space)
        : SingleInputNode(qctx, kind, input), space_(space) {}

    void cloneMembers(const Explore&);

protected:
    GraphSpaceID space_;
    bool dedup_{false};
    int64_t limit_{std::numeric_limits<int64_t>::max()};
    std::string filter_;
    std::vector<storage::cpp2::OrderBy> orderBy_;
};

using VertexProp = nebula::storage::cpp2::VertexProp;
using EdgeProp = nebula::storage::cpp2::EdgeProp;
using StatProp = nebula::storage::cpp2::StatProp;
using Expr = nebula::storage::cpp2::Expr;
using Direction = nebula::storage::cpp2::EdgeDirection;
/**
 * Get neighbors' property
 */
class GetNeighbors final : public Explore {
public:
    static GetNeighbors* make(QueryContext* qctx, PlanNode* input, GraphSpaceID space) {
        return qctx->objPool()->add(new GetNeighbors(qctx, input, space));
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
                              std::string filter = "") {
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
        gn->setFilter(std::move(filter));
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

private:
    GetNeighbors(QueryContext* qctx, PlanNode* input, GraphSpaceID space)
        : Explore(qctx, Kind::kGetNeighbors, input, space) {
        setLimit(-1);
    }

private:
    void cloneMembers(const GetNeighbors&);

    Expression*                              src_{nullptr};
    std::vector<EdgeType>                    edgeTypes_;
    storage::cpp2::EdgeDirection             edgeDirection_{Direction::OUT_EDGE};
    std::unique_ptr<std::vector<VertexProp>> vertexProps_;
    std::unique_ptr<std::vector<EdgeProp>>   edgeProps_;
    std::unique_ptr<std::vector<StatProp>>   statProps_;
    std::unique_ptr<std::vector<Expr>>       exprs_;
    bool                                     random_{false};
};

/**
 * Get property with given vertex keys.
 */
class GetVertices final : public Explore {
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
                             std::string filter = "") {
        return qctx->objPool()->add(new GetVertices(
                qctx,
                input,
                space,
                src,
                std::move(props),
                std::move(exprs),
                dedup,
                std::move(orderBy),
                limit,
                std::move(filter)));
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

private:
    GetVertices(QueryContext* qctx,
                PlanNode* input,
                GraphSpaceID space,
                Expression* src,
                std::unique_ptr<std::vector<VertexProp>>&& props,
                std::unique_ptr<std::vector<Expr>>&& exprs,
                bool dedup,
                std::vector<storage::cpp2::OrderBy> orderBy,
                int64_t limit,
                std::string filter)
        : Explore(qctx,
                  Kind::kGetVertices,
                  input,
                  space,
                  dedup,
                  limit,
                  std::move(filter),
                  std::move(orderBy)),
          src_(src),
          props_(std::move(props)),
          exprs_(std::move(exprs)) {}

    void cloneMembers(const GetVertices&);

private:
    // vertices may be parsing from runtime.
    Expression*                                src_{nullptr};
    // props of the vertex
    std::unique_ptr<std::vector<VertexProp>>   props_;
    // expression to get
    std::unique_ptr<std::vector<Expr>>         exprs_;
};

/**
 * Get property with given edge keys.
 */
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
                          std::string filter = "") {
        return qctx->objPool()->add(new GetEdges(
                qctx,
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
                std::move(filter)));
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
             std::string filter)
        : Explore(qctx,
                  Kind::kGetEdges,
                  input,
                  space,
                  dedup,
                  limit,
                  std::move(filter),
                  std::move(orderBy)),
          src_(src),
          type_(type),
          ranking_(ranking),
          dst_(dst),
          props_(std::move(props)),
          exprs_(std::move(exprs)) {}

    void cloneMembers(const GetEdges&);

private:
    // edges_ may be parsed from runtime.
    Expression*                              src_{nullptr};
    Expression*                              type_{nullptr};
    Expression*                              ranking_{nullptr};
    Expression*                              dst_{nullptr};
    // props of edge to get
    std::unique_ptr<std::vector<EdgeProp>>   props_;
    // expression to show
    std::unique_ptr<std::vector<Expr>>       exprs_;
};

/**
 * Read data through the index.
 */
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
                           std::string filter = "") {
        return qctx->objPool()->add(new IndexScan(qctx,
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
                                                  std::move(filter)));
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

    PlanNode* clone() const override;
    std::unique_ptr<PlanNodeDescription> explain() const override;

protected:
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
              std::string filter,
              Kind kind = Kind::kIndexScan)
        : Explore(qctx, kind, input, space, dedup, limit, std::move(filter), std::move(orderBy)) {
        contexts_ = std::move(contexts);
        returnCols_ = std::move(returnCols);
        isEdge_ = isEdge;
        schemaId_ = schemaId;
        isEmptyResultSet_ = isEmptyResultSet;
    }

    void cloneMembers(const IndexScan&);

private:
    std::vector<IndexQueryContext>                contexts_;
    std::vector<std::string>                      returnCols_;
    bool                                          isEdge_;
    int32_t                                       schemaId_;

    // TODO(yee): Generate special plan for this scenario
    bool isEmptyResultSet_{false};
};

/**
 * A Filter node helps filt some records with condition.
 */
class Filter final : public SingleInputNode {
public:
    static Filter* make(QueryContext* qctx,
                        PlanNode* input,
                        Expression* condition = nullptr,
                        bool      needStableFilter = false) {
        return qctx->objPool()->add(new Filter(qctx, input, condition, needStableFilter));
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

private:
    Filter(QueryContext* qctx, PlanNode* input, Expression* condition, bool needStableFilter);
    void cloneMembers(const Filter&);

private:
    // Remain result when true
    Expression*                 condition_{nullptr};
    bool                        needStableFilter_;
};

/**
 * Now we have three kind of set operations:
 *   UNION,
 *   INTERSECT,
 *   MINUS
 */
class SetOp : public BinaryInputNode {
protected:
    SetOp(QueryContext* qctx, Kind kind, PlanNode* left, PlanNode* right)
        : BinaryInputNode(qctx, kind, left, right) {
        DCHECK(kind == Kind::kUnion || kind == Kind::kIntersect || kind == Kind::kMinus);
    }

    void cloneMembers(const SetOp&);
};

/**
 * Combine two set of records.
 */
class Union final : public SetOp {
public:
    static Union* make(QueryContext *qctx, PlanNode* left, PlanNode* right) {
        return qctx->objPool()->add(new Union(qctx, left, right));
    }

    PlanNode* clone() const override;

private:
    Union(QueryContext* qctx, PlanNode* left, PlanNode* right)
        : SetOp(qctx, Kind::kUnion, left, right) {}

    void cloneMembers(const Union&);
};

/**
 * Return the intersected records between two sets.
 */
class Intersect final : public SetOp {
public:
    static Intersect* make(QueryContext* qctx, PlanNode* left, PlanNode* right) {
        return qctx->objPool()->add(new Intersect(qctx, left, right));
    }

    PlanNode* clone() const override;

private:
    Intersect(QueryContext* qctx, PlanNode* left, PlanNode* right)
        : SetOp(qctx, Kind::kIntersect, left, right) {}

    void cloneMembers(const Intersect&);
};

/**
 * Do subtraction between two sets.
 */
class Minus final : public SetOp {
public:
    static Minus* make(QueryContext* qctx, PlanNode* left, PlanNode* right) {
        return qctx->objPool()->add(new Minus(qctx, left, right));
    }

    PlanNode* clone() const override;

private:
    Minus(QueryContext* qctx, PlanNode* left, PlanNode* right)
        : SetOp(qctx, Kind::kMinus, left, right) {}

    void cloneMembers(const Minus&);
};

/**
 * Project is used to specify output vars or field.
 */
class Project final : public SingleInputNode {
public:
    static Project* make(QueryContext* qctx,
                         PlanNode* input,
                         YieldColumns* cols = nullptr) {
        return qctx->objPool()->add(new Project(qctx, input, cols));
    }

    const YieldColumns* columns() const {
        return cols_;
    }

    PlanNode* clone() const override;
    std::unique_ptr<PlanNodeDescription> explain() const override;

private:
    Project(QueryContext* qctx, PlanNode* input, YieldColumns* cols);

    void cloneMembers(const Project&);

private:
    YieldColumns*               cols_{nullptr};
};

class Unwind final : public SingleInputNode {
public:
    static Unwind* make(QueryContext* qctx,
                        PlanNode* input,
                        Expression* unwindExpr = nullptr,
                        std::string alias = "") {
        return qctx->objPool()->add(new Unwind(qctx, input, unwindExpr, alias));
    }

    Expression* unwindExpr() const {
        return unwindExpr_;
    }

    const std::string alias() const {
        return alias_;
    }

    PlanNode* clone() const override;
    std::unique_ptr<PlanNodeDescription> explain() const override;

private:
    Unwind(QueryContext* qctx, PlanNode* input, Expression* unwindExpr, std::string alias)
        : SingleInputNode(qctx, Kind::kUnwind, input), unwindExpr_(unwindExpr), alias_(alias) {}

    void cloneMembers(const Unwind&);

private:
    Expression* unwindExpr_{nullptr};
    std::string alias_;
};

/**
 * Sort the given record set.
 */
class Sort final : public SingleInputNode {
public:
    static Sort* make(QueryContext* qctx,
                      PlanNode* input,
                      std::vector<std::pair<size_t, OrderFactor::OrderType>> factors = {}) {
        return qctx->objPool()->add(
            new Sort(qctx, input, std::move(factors)));
    }

    const std::vector<std::pair<size_t, OrderFactor::OrderType>>& factors() const {
        return factors_;
    }

    PlanNode* clone() const override;
    std::unique_ptr<PlanNodeDescription> explain() const override;

private:
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
        auto get = [&cols](const std::pair<size_t, OrderFactor::OrderType> &factor) {
            auto colName = cols[factor.first];
            auto order = factor.second == OrderFactor::OrderType::ASCEND ? "ASCEND" : "DESCEND";
            return std::pair<std::string, std::string>{colName, order};
        };
        std::transform(factors_.begin(), factors_.end(), result.begin(), get);

        return result;
    }

    void cloneMembers(const Sort&);

private:
    std::vector<std::pair<size_t, OrderFactor::OrderType>>   factors_;
};

/**
 * Output the records with the given limitation.
 */
class Limit final : public SingleInputNode {
public:
    static Limit* make(QueryContext* qctx,
                       PlanNode* input,
                       int64_t offset = -1,
                       int64_t count = -1) {
        return qctx->objPool()->add(new Limit(qctx, input, offset, count));
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
    Limit(QueryContext* qctx, PlanNode* input, int64_t offset, int64_t count)
        : SingleInputNode(qctx, Kind::kLimit, input) {
        offset_ = offset;
        count_ = count;
    }

    void cloneMembers(const Limit&);

private:
    int64_t     offset_{-1};
    int64_t     count_{-1};
};

/**
 * Get the Top N record set.
 */
class TopN final : public SingleInputNode {
public:
    static TopN* make(QueryContext* qctx,
                      PlanNode* input,
                      std::vector<std::pair<size_t, OrderFactor::OrderType>> factors = {},
                      int64_t offset = -1,
                      int64_t count = -1) {
        return qctx->objPool()->add(new TopN(qctx, input, std::move(factors), offset, count));
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
        auto get = [&cols](const std::pair<size_t, OrderFactor::OrderType> &factor) {
            auto colName = cols[factor.first];
            auto order = factor.second == OrderFactor::OrderType::ASCEND ? "ASCEND" : "DESCEND";
            return std::pair<std::string, std::string>{colName, order};
        };
        std::transform(factors_.begin(), factors_.end(), result.begin(), get);

        return result;
    }

    void cloneMembers(const TopN&);

private:
    std::vector<std::pair<size_t, OrderFactor::OrderType>>   factors_;
    int64_t     offset_{-1};
    int64_t     count_{-1};
};

/**
 * Do Aggregation with the given set of records,
 * such as AVG(), COUNT()...
 */
class Aggregate final : public SingleInputNode {
public:
    static Aggregate* make(QueryContext* qctx,
                           PlanNode* input,
                           std::vector<Expression*>&& groupKeys = {},
                           std::vector<Expression*>&& groupItems = {}) {
        return qctx->objPool()->add(
            new Aggregate(qctx, input, std::move(groupKeys), std::move(groupItems)));
    }

    const std::vector<Expression*>& groupKeys() const {
        return groupKeys_;
    }

    const std::vector<Expression*>& groupItems() const {
        return groupItems_;
    }

    PlanNode* clone() const override;
    std::unique_ptr<PlanNodeDescription> explain() const override;

private:
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
    std::vector<Expression*>    groupKeys_;
    std::vector<Expression*>    groupItems_;
};

class SwitchSpace final : public SingleInputNode {
public:
    static SwitchSpace* make(QueryContext* qctx, PlanNode* input, std::string spaceName) {
        return qctx->objPool()->add(new SwitchSpace(qctx, input, spaceName));
    }

    const std::string& getSpaceName() const {
        return spaceName_;
    }

    PlanNode* clone() const override;
    std::unique_ptr<PlanNodeDescription> explain() const override;

private:
    SwitchSpace(QueryContext* qctx, PlanNode* input, std::string spaceName)
        : SingleInputNode(qctx, Kind::kSwitchSpace, input) {
        spaceName_ = std::move(spaceName);
    }

    void cloneMembers(const SwitchSpace&);

private:
    std::string     spaceName_;
};

class Dedup final : public SingleInputNode {
public:
    static Dedup* make(QueryContext* qctx,
                       PlanNode* input) {
        return qctx->objPool()->add(new Dedup(qctx, input));
    }

    PlanNode* clone() const override;

private:
    Dedup(QueryContext* qctx, PlanNode* input);

    void cloneMembers(const Dedup&);
};

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
        return qctx->objPool()->add(new DataCollect(qctx, kind));
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
        std::transform(inputVars_.begin(), inputVars_.end(), vars.begin(), [](auto& var) {
            return var->name;
        });
        return vars;
    }

    StepClause step() const {
        return step_;
    }

    bool distinct() const {
        return distinct_;
    }

    PlanNode* clone() const override;

    std::unique_ptr<PlanNodeDescription> explain() const override;

private:
    DataCollect(QueryContext* qctx, DCKind kind)
        : VariableDependencyNode(qctx, Kind::kDataCollect), kind_(kind) {}

    void cloneMembers(const DataCollect&);

private:
    DCKind          kind_;
    // using for m to n steps
    StepClause      step_;
    bool            distinct_{false};
};

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
    std::pair<std::string, int64_t>         leftVar_;
    std::pair<std::string, int64_t>         rightVar_;
    std::vector<Expression*>                hashKeys_;
    std::vector<Expression*>                probeKeys_;
};

/*
 *  left join
 */
class LeftJoin final : public Join {
public:
    static LeftJoin* make(QueryContext* qctx,
                          PlanNode* input,
                          std::pair<std::string, int64_t> leftVar,
                          std::pair<std::string, int64_t> rightVar,
                          std::vector<Expression*> hashKeys = {},
                          std::vector<Expression*> probeKeys = {}) {
        return qctx->objPool()->add(new LeftJoin(qctx,
                                                 input,
                                                 std::move(leftVar),
                                                 std::move(rightVar),
                                                 std::move(hashKeys),
                                                 std::move(probeKeys)));
    }

    PlanNode* clone() const override;
    std::unique_ptr<PlanNodeDescription> explain() const override;

private:
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

/*
 *  inner join
 */
class InnerJoin final : public Join {
public:
    static InnerJoin* make(QueryContext* qctx,
                           PlanNode* input,
                           std::pair<std::string, int64_t> leftVar,
                           std::pair<std::string, int64_t> rightVar,
                           std::vector<Expression*> hashKeys = {},
                           std::vector<Expression*> probeKeys = {}) {
        return qctx->objPool()->add(new InnerJoin(qctx,
                                                  input,
                                                  std::move(leftVar),
                                                  std::move(rightVar),
                                                  std::move(hashKeys),
                                                  std::move(probeKeys)));
    }

    PlanNode* clone() const override;
    std::unique_ptr<PlanNodeDescription> explain() const override;

private:
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

/*
 * set var = value
 */
class Assign final : public SingleInputNode {
public:
    static Assign* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new Assign(qctx, input));
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
    Assign(QueryContext* qctx, PlanNode* input) : SingleInputNode(qctx, Kind::kAssign, input) {}

    void cloneMembers(const Assign&);

private:
    std::vector<std::pair<std::string, Expression*>> items_;
};

/**
 * Union all versions of the variable in the dependency
 * The input is a single PlanNode
 */
class UnionAllVersionVar final : public SingleInputNode {
public:
    static UnionAllVersionVar* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new UnionAllVersionVar(qctx, input));
    }

    PlanNode* clone() const override;

private:
    UnionAllVersionVar(QueryContext* qctx, PlanNode* input)
        : SingleInputNode(qctx, Kind::kUnionAllVersionVar, input) {}

    void cloneMembers(const UnionAllVersionVar&);
};

}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_PLAN_QUERY_H_
