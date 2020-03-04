/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_QUERY_H_
#define PLANNER_QUERY_H_

#include "base/Base.h"
#include "PlanNode.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"
#include "interface/gen-cpp2/storage_types.h"

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
 *  ReadIndex
 */
class Explore : public PlanNode {
public:
    explicit Explore(GraphSpaceID space) {
        space_ = space;
    }

    Explore(GraphSpaceID space,
            std::vector<std::string>&& colNames,
            StateTransition&& stateTrans) : PlanNode(std::move(colNames), std::move(stateTrans)) {
        space_ = space;
    }

    GraphSpaceID space() const {
        return space_;
    }

private:
    GraphSpaceID        space_;
};

/**
 * Get neighbors' property
 */
class GetNeighbors final : public Explore {
public:
    explicit GetNeighbors(GraphSpaceID space) : Explore(space) {
        kind_ = PlanNode::Kind::kGetNeighbors;
    }

    GetNeighbors(GraphSpaceID space,
                 std::vector<std::string>&& colNames,
                 StateTransition&& stateTrans)
        : Explore(space, std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kGetNeighbors;
    }

    std::string explain() const override;

private:
    std::vector<VertexID>                        vertices_;
    std::vector<EdgeType>                        edgeTypes_;
    std::vector<storage::cpp2::VertexProp>       vertexProps_;
    std::vector<storage::cpp2::EdgeProp>         edgeProps_;
    std::vector<storage::cpp2::StatProp>         statProps_;
    std::string                                  filter_;
};

/**
 * Get property with given vertex keys.
 */
class GetVertices final : public Explore {
public:
    explicit GetVertices(GraphSpaceID space) : Explore(space) {
        kind_ = PlanNode::Kind::kGetVertices;
    }

    GetVertices(GraphSpaceID space,
                std::vector<std::string>&& colNames,
                StateTransition&& stateTrans)
        : Explore(space, std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kGetVertices;
    }

    std::string explain() const override;

private:
    std::vector<VertexID>                    vertices_;
    std::vector<storage::cpp2::VertexProp>   props_;
    std::string                              filter_;
};

/**
 * Get property with given edge keys.
 */
class GetEdges final : public Explore {
public:
    explicit GetEdges(GraphSpaceID space) : Explore(space) {
        kind_ = PlanNode::Kind::kGetEdges;
    }

    GetEdges(GraphSpaceID space,
             std::vector<std::string>&& colNames,
             StateTransition&& stateTrans)
        : Explore(space, std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kGetEdges;
    }

    std::string explain() const override;

private:
    std::vector<storage::cpp2::EdgeKey>      edges_;
    std::vector<storage::cpp2::EdgeProp>     props_;
    std::string                              filter_;
};

/**
 * Read data through the index.
 */
class ReadIndex final : public Explore {
public:
    explicit ReadIndex(GraphSpaceID space) : Explore(space) {
        kind_ = PlanNode::Kind::kReadIndex;
    }

    ReadIndex(GraphSpaceID space,
              std::vector<std::string>&& colNames,
              StateTransition&& stateTrans)
        : Explore(space, std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kReadIndex;
    }

    std::string explain() const override;
};

/**
 * A Filter node helps filt some records with condition.
 */
class Filter final : public PlanNode {
public:
    explicit Filter(Expression* condition) {
        kind_ = PlanNode::Kind::kFilter;
        condition_ = condition;
    }

    Filter(Expression* condition,
           std::vector<std::string>&& colNames,
           StateTransition&& stateTrans) : PlanNode(std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kFilter;
        condition_ = condition;
    }

    const Expression* condition() const {
        return condition_;
    }

    std::string explain() const override;

private:
    Expression*         condition_;
};

/**
 * Now we have three kind of set operations:
 *   UNION,
 *   INTERSECT,
 *   MINUS
 */
class SetOp : public PlanNode {
public:
    SetOp() = default;

    SetOp(std::vector<std::string>&& colNames,
          StateTransition&& stateTrans) : PlanNode(std::move(colNames), std::move(stateTrans)) {}
};

/**
 * Combine two set of records.
 */
class Union final : public SetOp {
public:
    explicit Union(bool distinct) {
        kind_ = PlanNode::Kind::kUnion;
        distinct_ = distinct;
    }

    Union(bool distinct,
          std::vector<std::string>&& colNames,
          StateTransition&& stateTrans) : SetOp(std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kUnion;
        distinct_ = distinct;
    }

    bool distinct() {
        return distinct_;
    }

    std::string explain() const override;

private:
    bool    distinct_;
};

/**
 * Return the intersected records between two sets.
 */
class Intersect final : public SetOp {
public:
    Intersect() {
        kind_ = PlanNode::Kind::kIntersect;
    }

    Intersect(std::vector<std::string>&& colNames,
              StateTransition&& stateTrans) : SetOp(std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kIntersect;
    }

    std::string explain() const override;
};

/**
 * Do subtraction between two sets.
 */
class Minus final : public SetOp {
public:
    Minus() {
        kind_ = PlanNode::Kind::kMinus;
    }

    Minus(std::vector<std::string>&& colNames,
          StateTransition&& stateTrans) : SetOp(std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kMinus;
    }

    std::string explain() const override;
};

/**
 * Project is used to specify the final output.
 */
class Project final : public PlanNode {
public:
    Project(YieldColumns* cols, bool distinct) {
        kind_ = PlanNode::Kind::kProject;
        cols_ = cols;
        distinct_ = distinct;
    }

    Project(YieldColumns* cols,
            bool distinct,
            std::vector<std::string>&& colNames,
            StateTransition&& stateTrans) : PlanNode(std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kProject;
        cols_ = cols;
        distinct_ = distinct;
    }

    const YieldColumns* cols() const {
        return cols_;
    }

    bool distinct() const {
        return distinct_;
    }

    std::string explain() const override;

private:
    YieldColumns*       cols_;
    bool                distinct_;
};

/**
 * Sort the given record set.
 */
class Sort final : public PlanNode {
public:
    explicit Sort(OrderFactors* factors) {
        kind_ = PlanNode::Kind::kSort;
        factors_ = factors;
    }

    Sort(OrderFactors* factors,
         std::vector<std::string>&& colNames,
         StateTransition&& stateTrans) : PlanNode(std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kSort;
        factors_ = factors;
    }

    const OrderFactors* factors() {
        return factors_;
    }

    std::string explain() const override;

private:
    OrderFactors*   factors_;
};

/**
 * Output the records with the given limitation.
 */
class Limit final : public PlanNode {
public:
    Limit(int64_t offset, int64_t count) {
        kind_ = PlanNode::Kind::kLimit;
        offset_ = offset;
        count_ = count;
    }

    Limit(int64_t offset,
          int64_t count,
          std::vector<std::string>&& colNames,
          StateTransition&& stateTrans) : PlanNode(std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kLimit;
        offset_ = offset;
        count_ = count;
    }

    int64_t offset() const {
        return offset_;
    }

    int64_t count() const {
        return count_;
    }

    std::string explain() const override;

private:
    int64_t     offset_{-1};
    int64_t     count_{-1};
};

/**
 * Do Aggregation with the given set of records,
 * such as AVG(), COUNT()...
 */
class Aggregate : public PlanNode {
public:
    Aggregate(YieldColumns* yieldCols,
              YieldColumns* groupCols) {
        kind_ = PlanNode::Kind::kAggregate;
        yieldCols_ = yieldCols;
        groupCols_ = groupCols;
    }

    Aggregate(YieldColumns* yieldCols,
              YieldColumns* groupCols,
              std::vector<std::string>&& colNames,
              StateTransition&& stateTrans) : PlanNode(std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kAggregate;
        yieldCols_ = yieldCols;
        groupCols_ = groupCols;
    }

    const YieldColumns* yields() const {
        return yieldCols_;
    }

    const YieldColumns* groups() const {
        return groupCols_;
    }

    std::string explain() const override;

private:
    YieldColumns*                               yieldCols_;
    YieldColumns*                               groupCols_;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_QUERY_H_
