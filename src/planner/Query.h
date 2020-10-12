/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_QUERY_H_
#define PLANNER_QUERY_H_

#include "common/base/Base.h"
#include "common/function/AggregateFunction.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "context/QueryContext.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"
#include "planner/PlanNode.h"

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

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

protected:
    Explore(int64_t id,
            Kind kind,
            PlanNode* input,
            GraphSpaceID space,
            bool dedup,
            int64_t limit,
            std::string filter,
            std::vector<storage::cpp2::OrderBy> orderBy)
        : SingleInputNode(id, kind, input),
          space_(space),
          dedup_(dedup),
          limit_(limit),
          filter_(std::move(filter)),
          orderBy_(std::move(orderBy)) {}

    Explore(int64_t id, Kind kind, PlanNode* input, GraphSpaceID space)
        : SingleInputNode(id, kind, input), space_(space) {}

protected:
    GraphSpaceID space_;
    bool dedup_{false};
    int64_t limit_{std::numeric_limits<int64_t>::max()};
    std::string filter_;
    std::vector<storage::cpp2::OrderBy> orderBy_;
};

/**
 * Get neighbors' property
 */
class GetNeighbors final : public Explore {
public:
    using VertexProps = std::unique_ptr<std::vector<storage::cpp2::VertexProp>>;
    using EdgeProps = std::unique_ptr<std::vector<storage::cpp2::EdgeProp>>;
    using StatProps = std::unique_ptr<std::vector<storage::cpp2::StatProp>>;
    using Exprs = std::unique_ptr<std::vector<storage::cpp2::Expr>>;

    static GetNeighbors* make(QueryContext* qctx, PlanNode* input, GraphSpaceID space) {
        return qctx->objPool()->add(new GetNeighbors(qctx->genId(), input, space));
    }

    static GetNeighbors* make(QueryContext* qctx,
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
                              int64_t limit = std::numeric_limits<int64_t>::max(),
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

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    Expression* src() const {
        return src_;
    }

    storage::cpp2::EdgeDirection edgeDirection() const {
        return edgeDirection_;
    }

    const std::vector<EdgeType>& edgeTypes() const {
        return edgeTypes_;
    }

    const std::vector<storage::cpp2::VertexProp>* vertexProps() const {
        return vertexProps_.get();
    }

    const std::vector<storage::cpp2::EdgeProp>* edgeProps() const {
        return edgeProps_.get();
    }

    const std::vector<storage::cpp2::StatProp>* statProps() const {
        return statProps_.get();
    }

    const std::vector<storage::cpp2::Expr>* exprs() const {
        return exprs_.get();
    }

    bool random() const {
        return random_;
    }

    void setSrc(Expression* src) {
        src_ = src;
    }

    void setEdgeDirection(storage::cpp2::EdgeDirection direction) {
        edgeDirection_ = direction;
    }

    void setEdgeTypes(std::vector<EdgeType> edgeTypes) {
        edgeTypes_ = std::move(edgeTypes);
    }

    void setVertexProps(VertexProps vertexProps) {
        vertexProps_ = std::move(vertexProps);
    }

    void setEdgeProps(EdgeProps edgeProps) {
        edgeProps_ = std::move(edgeProps);
    }

    void setStatProps(StatProps statProps) {
        statProps_ = std::move(statProps);
    }

    void setExprs(Exprs exprs) {
        exprs_ = std::move(exprs);
    }

    void setRandom(bool random = true) {
        random_ = random;
    }

private:
    GetNeighbors(int64_t id, PlanNode* input, GraphSpaceID space)
        : Explore(id, Kind::kGetNeighbors, input, space) {}

private:
    Expression*                                  src_{nullptr};
    std::vector<EdgeType>                        edgeTypes_;
    storage::cpp2::EdgeDirection edgeDirection_{storage::cpp2::EdgeDirection::OUT_EDGE};
    VertexProps                                  vertexProps_;
    EdgeProps                                    edgeProps_;
    StatProps                                    statProps_;
    Exprs                                        exprs_;
    bool                                         random_;
};

/**
 * Get property with given vertex keys.
 */
class GetVertices final : public Explore {
public:
    static GetVertices* make(QueryContext* qctx,
                             PlanNode* input,
                             GraphSpaceID space,
                             Expression* src,
                             std::vector<storage::cpp2::VertexProp> props,
                             std::vector<storage::cpp2::Expr>       exprs,
                             bool dedup = false,
                             std::vector<storage::cpp2::OrderBy> orderBy = {},
                             int64_t limit = std::numeric_limits<int64_t>::max(),
                             std::string filter = "") {
        return qctx->objPool()->add(new GetVertices(
                qctx->genId(),
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

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    Expression* src() const {
        return src_;
    }

    const std::vector<storage::cpp2::VertexProp>& props() const {
        return props_;
    }

    const std::vector<storage::cpp2::Expr>& exprs() const {
        return exprs_;
    }

private:
    GetVertices(int64_t id,
                PlanNode* input,
                GraphSpaceID space,
                Expression* src,
                std::vector<storage::cpp2::VertexProp> props,
                std::vector<storage::cpp2::Expr>       exprs,
                bool dedup,
                std::vector<storage::cpp2::OrderBy> orderBy,
                int64_t limit,
                std::string filter)
        : Explore(id,
                  Kind::kGetVertices,
                  input,
                  space,
                  dedup,
                  limit,
                  std::move(filter),
                  std::move(orderBy)),
          src_(src),
          props_(std::move(props)),
          exprs_(std::move(exprs)) { }

private:
    // vertices may be parsing from runtime.
    Expression*                              src_{nullptr};
    // props of the vertex
    std::vector<storage::cpp2::VertexProp>   props_;
    // expression to get
    std::vector<storage::cpp2::Expr>         exprs_;
};

/**
 * Get property with given edge keys.
 */
class GetEdges final : public Explore {
public:
    static GetEdges* make(QueryContext* qctx,
                          PlanNode* input,
                          GraphSpaceID space,
                          Expression* src,
                          Expression* type,
                          Expression* ranking,
                          Expression* dst,
                          std::vector<storage::cpp2::EdgeProp> props,
                          std::vector<storage::cpp2::Expr> exprs,
                          bool dedup = false,
                          int64_t limit = std::numeric_limits<int64_t>::max(),
                          std::vector<storage::cpp2::OrderBy> orderBy = {},
                          std::string filter = "") {
        return qctx->objPool()->add(new GetEdges(
                qctx->genId(),
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

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

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

    const std::vector<storage::cpp2::EdgeProp>& props() const {
        return props_;
    }

    const std::vector<storage::cpp2::Expr>& exprs() const {
        return exprs_;
    }

private:
    GetEdges(int64_t id,
             PlanNode* input,
             GraphSpaceID space,
             Expression* src,
             Expression* type,
             Expression* ranking,
             Expression* dst,
             std::vector<storage::cpp2::EdgeProp> props,
             std::vector<storage::cpp2::Expr>     exprs,
             bool dedup,
             int64_t limit,
             std::vector<storage::cpp2::OrderBy> orderBy,
             std::string filter)
        : Explore(id,
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
          exprs_(std::move(exprs)) { }

private:
    // edges_ may be parsed from runtime.
    Expression*                              src_{nullptr};
    Expression*                              type_{nullptr};
    Expression*                              ranking_{nullptr};
    Expression*                              dst_{nullptr};
    // props of edge to get
    std::vector<storage::cpp2::EdgeProp>     props_;
    // expression to show
    std::vector<storage::cpp2::Expr>         exprs_;
};

/**
 * Read data through the index.
 */
class IndexScan final : public Explore {
public:
    using IndexQueryCtx = std::unique_ptr<std::vector<storage::cpp2::IndexQueryContext>>;
    using IndexReturnCols = std::unique_ptr<std::vector<std::string>>;

    static IndexScan* make(QueryContext* qctx,
                           PlanNode* input,
                           GraphSpaceID space,
                           IndexQueryCtx&& contexts,
                           IndexReturnCols&& returnCols,
                           bool isEdge,
                           int32_t schemaId,
                           bool dedup = false,
                           std::vector<storage::cpp2::OrderBy> orderBy = {},
                           int64_t limit = std::numeric_limits<int64_t>::max(),
                           std::string filter = "") {
        return qctx->objPool()->add(new IndexScan(qctx->genId(),
                                                  input,
                                                  space,
                                                  std::move(contexts),
                                                  std::move(returnCols),
                                                  isEdge,
                                                  schemaId,
                                                  dedup,
                                                  std::move(orderBy),
                                                  limit,
                                                  std::move(filter)));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const std::vector<storage::cpp2::IndexQueryContext>* queryContext() const {
        return contexts_.get();
    }

    const std::vector<std::string>* returnColumns() const {
        return returnCols_.get();
    }

    bool isEdge() const {
        return isEdge_;
    }

    int32_t schemaId() const {
        return schemaId_;
    }

    void setIndexQueryContext(IndexQueryCtx contexts) {
        contexts_ = std::move(contexts);
    }

    void setReturnCols(IndexReturnCols cols) {
        returnCols_ = std::move(cols);
    }

    void setIsEdge(bool isEdge) {
        isEdge_ = isEdge;
    }

    void setSchemaId(int32_t schema) {
        schemaId_ = schema;
    }

private:
    IndexScan(int64_t id,
              PlanNode* input,
              GraphSpaceID space,
              IndexQueryCtx&& contexts,
              IndexReturnCols&& returnCols,
              bool isEdge,
              int32_t schemaId,
              bool dedup,
              std::vector<storage::cpp2::OrderBy> orderBy,
              int64_t limit,
              std::string filter)
    : Explore(id,
              Kind::kIndexScan,
              input,
              space,
              dedup,
              limit,
              std::move(filter),
              std::move(orderBy)) {
        contexts_ = std::move(contexts);
        returnCols_ = std::move(returnCols);
        isEdge_ = isEdge;
        schemaId_ = schemaId;
    }

private:
    IndexQueryCtx                                 contexts_;
    IndexReturnCols                               returnCols_;
    bool                                          isEdge_;
    int32_t                                       schemaId_;
};

/**
 * A Filter node helps filt some records with condition.
 */
class Filter final : public SingleInputNode {
public:
    static Filter* make(QueryContext* qctx,
                        PlanNode* input,
                        Expression* condition) {
        return qctx->objPool()->add(new Filter(qctx->genId(), input, condition));
    }

    Expression* condition() const {
        return condition_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

private:
    Filter(int64_t id, PlanNode* input, Expression* condition)
      : SingleInputNode(id, Kind::kFilter, input) {
        condition_ = condition;
    }

private:
    Expression*                 condition_{nullptr};
};

/**
 * Now we have three kind of set operations:
 *   UNION,
 *   INTERSECT,
 *   MINUS
 */
class SetOp : public BiInputNode {
protected:
    SetOp(int64_t id, Kind kind, PlanNode* left, PlanNode* right)
        : BiInputNode(id, kind, left, right) {
        DCHECK(kind == Kind::kUnion || kind == Kind::kIntersect || kind == Kind::kMinus);
    }
};

/**
 * Combine two set of records.
 */
class Union final : public SetOp {
public:
    static Union* make(QueryContext *qctx, PlanNode* left, PlanNode* right) {
        return qctx->objPool()->add(new Union(qctx->genId(), left, right));
    }

private:
    Union(int64_t id, PlanNode* left, PlanNode* right)
        : SetOp(id, Kind::kUnion, left, right) {}
};

/**
 * Return the intersected records between two sets.
 */
class Intersect final : public SetOp {
public:
    static Intersect* make(QueryContext* qctx, PlanNode* left, PlanNode* right) {
        return qctx->objPool()->add(new Intersect(qctx->genId(), left, right));
    }

private:
    Intersect(int64_t id, PlanNode* left, PlanNode* right)
        : SetOp(id, Kind::kIntersect, left, right) {}
};

/**
 * Do subtraction between two sets.
 */
class Minus final : public SetOp {
public:
    static Minus* make(QueryContext* qctx, PlanNode* left, PlanNode* right) {
        return qctx->objPool()->add(new Minus(qctx->genId(), left, right));
    }

private:
    Minus(int64_t id, PlanNode* left, PlanNode* right)
        : SetOp(id, Kind::kMinus, left, right) {}
};

/**
 * Project is used to specify output vars or field.
 */
class Project final : public SingleInputNode {
public:
    static Project* make(QueryContext* qctx,
                         PlanNode* input,
                         YieldColumns* cols) {
        return qctx->objPool()->add(new Project(qctx->genId(), input, cols));
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

    const YieldColumns* columns() const {
        return cols_;
    }

private:
    Project(int64_t id, PlanNode* input, YieldColumns* cols)
      : SingleInputNode(id, Kind::kProject, input), cols_(cols) { }

private:
    YieldColumns*               cols_{nullptr};
};

/**
 * Sort the given record set.
 */
class Sort final : public SingleInputNode {
public:
    static Sort* make(QueryContext* qctx,
                      PlanNode* input,
                      std::vector<std::pair<size_t, OrderFactor::OrderType>> factors) {
        return qctx->objPool()->add(new Sort(qctx->genId(), input, std::move(factors)));
    }

    const std::vector<std::pair<size_t, OrderFactor::OrderType>>& factors() const {
        return factors_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

private:
    Sort(int64_t id,
         PlanNode* input,
         std::vector<std::pair<size_t, OrderFactor::OrderType>> factors)
        : SingleInputNode(id, Kind::kSort, input) {
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
                       int64_t offset,
                       int64_t count) {
        return qctx->objPool()->add(new Limit(qctx->genId(), input, offset, count));
    }

    int64_t offset() const {
        return offset_;
    }

    int64_t count() const {
        return count_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

private:
    Limit(int64_t id, PlanNode* input, int64_t offset, int64_t count)
        : SingleInputNode(id, Kind::kLimit, input) {
        offset_ = offset;
        count_ = count;
    }

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
                      std::vector<std::pair<size_t, OrderFactor::OrderType>> factors,
                      int64_t offset,
                      int64_t count) {
        return qctx->objPool()->add(new TopN(qctx->genId(),
            input, std::move(factors), offset, count));
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

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

private:
    TopN(int64_t id,
         PlanNode* input,
         std::vector<std::pair<size_t, OrderFactor::OrderType>> factors,
         int64_t offset,
         int64_t count)
        : SingleInputNode(id, Kind::kTopN, input) {
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
    struct GroupItem {
        GroupItem(Expression* e, AggFun::Function f, bool d)
            : expr(e), func(f), distinct(d) {}
        Expression* expr;
        AggFun::Function func;
        bool distinct = false;
    };
    static Aggregate* make(QueryContext* qctx,
                           PlanNode* input,
                           std::vector<Expression*>&& groupKeys,
                           std::vector<GroupItem>&& groupItems) {
        return qctx->objPool()->add(
            new Aggregate(qctx->genId(), input, std::move(groupKeys), std::move(groupItems)));
    }

    const std::vector<Expression*>& groupKeys() const {
        return groupKeys_;
    }

    const std::vector<GroupItem>& groupItems() const {
        return groupItems_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

private:
    Aggregate(int64_t id,
              PlanNode* input,
              std::vector<Expression*>&& groupKeys,
              std::vector<GroupItem>&& groupItems)
        : SingleInputNode(id, Kind::kAggregate, input) {
        groupKeys_ = std::move(groupKeys);
        groupItems_ = std::move(groupItems);
    }

private:
    std::vector<Expression*>    groupKeys_;
    std::vector<GroupItem>      groupItems_;
};

class SwitchSpace final : public SingleInputNode {
public:
    static SwitchSpace* make(QueryContext* qctx,
                             PlanNode* input,
                             std::string spaceName) {
        return qctx->objPool()->add(new SwitchSpace(qctx->genId(), input, spaceName));
    }

    const std::string& getSpaceName() const {
        return spaceName_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

private:
    SwitchSpace(int64_t id,
                PlanNode* input,
                std::string spaceName)
        : SingleInputNode(id, Kind::kSwitchSpace, input) {
        spaceName_ = std::move(spaceName);
    }

private:
    std::string     spaceName_;
};

class Dedup final : public SingleInputNode {
public:
    static Dedup* make(QueryContext* qctx,
                       PlanNode* input) {
        return qctx->objPool()->add(new Dedup(qctx->genId(), input));
    }

private:
    Dedup(int64_t id,
          PlanNode* input)
        : SingleInputNode(id, Kind::kDedup, input) {
    }
};

class DataCollect final : public SingleDependencyNode {
public:
    enum class CollectKind : uint8_t {
        kSubgraph,
        kRowBasedMove,
        kMToN,
        kBFSShortest,
    };

    static DataCollect* make(QueryContext* qctx,
                             PlanNode* input,
                             CollectKind collectKind,
                             std::vector<std::string> vars) {
        return qctx->objPool()->add(
            new DataCollect(qctx->genId(), input, collectKind, std::move(vars)));
    }

    void setMToN(StepClause::MToN* mToN) {
        mToN_ = mToN;
    }

    void setDistinct(bool distinct) {
        distinct_ = distinct;
    }

    CollectKind collectKind() const {
        return collectKind_;
    }

    const std::vector<std::string>& vars() const {
        return inputVars_;
    }

    StepClause::MToN* mToN() const {
        return mToN_;
    }

    bool distinct() const {
        return distinct_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

private:
    DataCollect(int64_t id,
                PlanNode* input,
                CollectKind collectKind,
                std::vector<std::string> vars)
        : SingleDependencyNode(id, Kind::kDataCollect, input) {
        collectKind_ = collectKind;
        inputVars_ = std::move(vars);
    }

private:
    CollectKind                 collectKind_;
    // using for m to n steps
    StepClause::MToN*           mToN_{nullptr};
    bool                        distinct_{false};
};

/**
 * An implementation of inner join which join two given variable.
 */
class DataJoin final : public SingleDependencyNode {
public:
    static DataJoin* make(QueryContext* qctx,
                          PlanNode* input,
                          std::pair<std::string, int64_t> leftVar,
                          std::pair<std::string, int64_t> rightVar,
                          std::vector<Expression*> hashKeys,
                          std::vector<Expression*> probeKeys) {
        return qctx->objPool()->add(new DataJoin(qctx->genId(),
                                                 input,
                                                 std::move(leftVar),
                                                 std::move(rightVar),
                                                 std::move(hashKeys),
                                                 std::move(probeKeys)));
    }

    const std::pair<std::string, int64_t>& leftVar() const {
        return leftVar_;
    }

    const std::pair<std::string, int64_t>& rightVar() const {
        return rightVar_;
    }

    const std::vector<Expression*>& hashKeys() const {
        return hashKeys_;
    }

    const std::vector<Expression*>& probeKeys() const {
        return probeKeys_;
    }

    std::unique_ptr<cpp2::PlanNodeDescription> explain() const override;

private:
    DataJoin(int64_t id, PlanNode* input,
             std::pair<std::string, int64_t> leftVar,
             std::pair<std::string, int64_t> rightVar,
             std::vector<Expression*> hashKeys, std::vector<Expression*> probeKeys)
        : SingleDependencyNode(id, Kind::kDataJoin, input),
        leftVar_(std::move(leftVar)),
        rightVar_(std::move(rightVar)),
        hashKeys_(std::move(hashKeys)),
        probeKeys_(std::move(probeKeys)) {}

private:
    // var name, var version
    std::pair<std::string, int64_t>         leftVar_;
    std::pair<std::string, int64_t>         rightVar_;
    std::vector<Expression*>                hashKeys_;
    std::vector<Expression*>                probeKeys_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_QUERY_H_
