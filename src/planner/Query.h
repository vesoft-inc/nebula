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

/**
 *  All query-related nodes would be put in this file,
 *  and they are derived from PlanNode.
 */
namespace nebula {
namespace graph {
class Explore : public PlanNode {
public:
    Explore(GraphSpaceID space,
            std::vector<std::string>&& colNames,
            StateTransition&& stateTrans) : PlanNode(std::move(colNames), std::move(stateTrans)) {
        space_ = space;
    }

    const GraphSpaceID space() const {
        return space_;
    }

private:
    GraphSpaceID        space_;
};

/**
 *  Get neighbors' property
 */
class GetNeighbors final : public Explore {
public:
    /*
    GetNeighbors(nebula::cpp2::GetNeighborsRequest&& req,
                 GraphSpaceID space,
                 std::vector<std::string>&& colNames,
                 StateTransition&& stateTrans)
        : Explore(space, std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kGetNeighbors;
        req_ = std::move(req);
    }
    */

    std::string explain() const override;

private:
    //nebula::cpp2::GetNeighborsRequest req_;
};

/**
 *  Get property with given vertex keys.
 */
class GetVertices final : public Explore {
public:
    /*
    GetVertices(nebula::cpp2::VertexPropRequest&& req,
                GraphSpaceID space,
                std::vector<std::string>&& colNames,
                StateTransition&& stateTrans)
        : Explore(space, std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kGetVertices;
        req_ = std::move(req);
    }
    */

    std::string explain() const override;

private:
    //nebula::cpp2::VertexPropRequest req_;
};

/**
 *  Get property with given edge keys.
 */
class GetEdges final : public Explore {
public:
    /*
    GetEdges(nebula::cpp2::EdgePropsRequest&& req,
                GraphSpaceID space,
                std::vector<std::string>&& colNames,
                StateTransition&& stateTrans)
        : Explore(space, std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kGetEdges;
        req_ = std::move(req);
    }
    */

    std::string explain() const override;

private:
    //nebula::cpp2::EdgePropRequest req_;
};

/**
 *  Read data through the index.
 */
class ReadIndex final : public Explore {
public:
    ReadIndex(GraphSpaceID space,
              std::vector<std::string>&& colNames,
              StateTransition&& stateTrans)
        : Explore(space, std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kReadIndex;
    }

    std::string explain() const override;
};

class Filter final : public PlanNode {
public:
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

class SetOp : public PlanNode {
public:
    SetOp(std::vector<std::string>&& colNames,
          StateTransition&& stateTrans) : PlanNode(std::move(colNames), std::move(stateTrans)) {};
};

class Union final : public SetOp {
public:
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

class Intersect final : public SetOp {
public:
    Intersect(std::vector<std::string>&& colNames,
              StateTransition&& stateTrans) : SetOp(std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kIntersect;
    }

    std::string explain() const override;

};

class Minus final : public SetOp {
public:
    Minus(std::vector<std::string>&& colNames,
          StateTransition&& stateTrans) : SetOp(std::move(colNames), std::move(stateTrans)) {
        kind_ = PlanNode::Kind::kMinus;
    }

    std::string explain() const override;

};

/**
 *  Project is used to specify the final output.
 */
class Project final : public PlanNode {
public:
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

class Sort final : public PlanNode {
public:
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

class Limit final : public PlanNode {
public:
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

class Aggregate : public PlanNode {
public:
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
