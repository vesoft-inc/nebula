/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_QUERY_H_
#define PLANNER_QUERY_H_


#include "base/Base.h"
#include "planner/PlanNode.h"
#include "planner/ExecutionPlan.h"
#include "parser/Clauses.h"
#include "parser/TraverseSentences.h"
#include "interface/gen-cpp2/storage_types.h"

/**
 * All query-related nodes would be put in this file,
 * and they are derived from PlanNode.
 */
namespace nebula {
namespace graph {

class StartNode final : public PlanNode {
public:
    StartNode() {
        kind_ = PlanNode::Kind::kStart;
    }

    std::string explain() const override {
        return "Start";
    }
};

class SingleInputNode : public PlanNode {
public:
    PlanNode* input() const {
        return input_;
    }

    void setInput(PlanNode* input) {
        input_ = input;
    }

    std::string explain() const override {
        return "";
    }

protected:
    PlanNode* input_;
};

class BiInputNode : public PlanNode {
public:
    PlanNode* left() const {
        return left_;
    }

    PlanNode* right() const {
        return right_;
    }

    std::string explain() const override {
        return "";
    }

protected:
    PlanNode* left_;
    PlanNode* right_;
};

class EndNode final : public SingleInputNode {
public:
    explicit EndNode(PlanNode* input) {
        kind_ = PlanNode::Kind::kEnd;
        input_ = input;
    }

    static PlanNode* make(PlanNode* input, ExecutionPlan* plan) {
        auto end = std::make_unique<EndNode>(input);
        return plan->addPlanNode(std::move(end));
    }

    std::string explain() const override {
        return "End";
    }
};

/**
 * Now we hava four kind of exploration nodes:
 *  GetNeighbors,
 *  GetVertices,
 *  GetEdges,
 *  ReadIndex
 */
class Explore : public SingleInputNode {
public:
    GraphSpaceID space() const {
        return space_;
    }

protected:
    GraphSpaceID        space_;
};

/**
 * Get neighbors' property
 */
class GetNeighbors final : public Explore {
public:
    GetNeighbors(PlanNode* input,
                 GraphSpaceID space,
                 std::vector<VertexID>&& vertices,
                 std::unique_ptr<Expression>&& src,
                 std::vector<EdgeType>&& edgeTypes,
                 std::vector<storage::cpp2::VertexProp>&& vertexProps,
                 std::vector<storage::cpp2::EdgeProp>&& edgeProps,
                 std::vector<storage::cpp2::StatProp>&& statProps,
                 std::string&& filter) {
        kind_ = PlanNode::Kind::kGetNeighbors;
        input_ = input;
        space_ = space;
        vertices_ = std::move(vertices);
        src_ = std::move(src);
        edgeTypes_ = std::move(edgeTypes);
        vertexProps_ = std::move(vertexProps);
        edgeProps_ = std::move(edgeProps);
        statProps_ = std::move(statProps);
        filter_ = std::move(filter);
    }

    static PlanNode* make(PlanNode* input,
                          GraphSpaceID space,
                          std::vector<VertexID>&& vertices,
                          std::unique_ptr<Expression>&& src,
                          std::vector<EdgeType>&& edgeTypes,
                          std::vector<storage::cpp2::VertexProp>&& vertexProps,
                          std::vector<storage::cpp2::EdgeProp>&& edgeProps,
                          std::vector<storage::cpp2::StatProp>&& statProps,
                          std::string&& filter,
                          ExecutionPlan* plan) {
        auto node = std::make_unique<GetNeighbors>(
                input,
                space,
                std::move(vertices),
                std::move(src),
                std::move(edgeTypes),
                std::move(vertexProps),
                std::move(edgeProps),
                std::move(statProps),
                std::move(filter));
        return plan->addPlanNode(std::move(node));
    }

    std::string explain() const override;

private:
    // vertices are parsing from query.
    std::vector<VertexID>                        vertices_;
    // vertices may be parsing from runtime.
    std::unique_ptr<Expression>                  src_;
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
    GetVertices(PlanNode* input,
                GraphSpaceID space,
                std::vector<VertexID>&& vertices,
                std::unique_ptr<Expression>&& src,
                std::vector<storage::cpp2::VertexProp>&& props,
                std::string&& filter) {
        kind_ = PlanNode::Kind::kGetVertices;
        input_ = input;
        space_ = space;
        vertices_ = std::move(vertices);
        src_ = std::move(src);
        props_ = std::move(props);
        filter = std::move(filter);
    }

    static PlanNode* make(PlanNode* input,
                          GraphSpaceID space,
                          std::vector<VertexID>&& vertices,
                          std::unique_ptr<Expression>&& src,
                          std::vector<storage::cpp2::VertexProp>&& props,
                          std::string&& filter,
                          ExecutionPlan* plan) {
        auto node = std::make_unique<GetVertices>(
                input,
                space,
                std::move(vertices),
                std::move(src),
                std::move(props),
                std::move(filter));
        return plan->addPlanNode(std::move(node));
    }

    std::string explain() const override;

private:
    // vertices are parsing from query.
    std::vector<VertexID>                    vertices_;
    // vertices may be parsing from runtime.
    std::unique_ptr<Expression>              src_;
    // props and filter are parsing from query.
    std::vector<storage::cpp2::VertexProp>   props_;
    std::string                              filter_;
};

/**
 * Get property with given edge keys.
 */
class GetEdges final : public Explore {
public:
    GetEdges(PlanNode* input,
             GraphSpaceID space,
             std::vector<storage::cpp2::EdgeKey> edges,
             std::unique_ptr<Expression> src,
             std::unique_ptr<Expression> ranking,
             std::unique_ptr<Expression> dst,
             std::vector<storage::cpp2::EdgeProp> props,
             std::string filter) {
        kind_ = PlanNode::Kind::kGetEdges;
        input_ = input;
        space_ = space;
        edges_ = std::move(edges);
        src_ = std::move(src);
        ranking_ = std::move(ranking);
        dst_ = std::move(dst);
        props_ = std::move(props);
        filter_ = std::move(filter);
    }

    static PlanNode* make(PlanNode* input,
                          GraphSpaceID space,
                          std::vector<storage::cpp2::EdgeKey> edges,
                          std::unique_ptr<Expression> src,
                          std::unique_ptr<Expression> ranking,
                          std::unique_ptr<Expression> dst,
                          std::vector<storage::cpp2::EdgeProp> props,
                          std::string filter,
                          ExecutionPlan* plan) {
        auto node = std::make_unique<GetEdges>(
                input,
                space,
                std::move(edges),
                std::move(src),
                std::move(ranking),
                std::move(dst),
                std::move(props),
                std::move(filter));
        return plan->addPlanNode(std::move(node));
    }

    std::string explain() const override;

private:
    // edges_ are parsing from the query.
    std::vector<storage::cpp2::EdgeKey>      edges_;
    // edges_ may be parsed from runtime.
    std::unique_ptr<Expression>              src_;
    std::unique_ptr<Expression>              ranking_;
    std::unique_ptr<Expression>              dst_;
    // props and filter are parsing from query.
    std::vector<storage::cpp2::EdgeProp>     props_;
    std::string                              filter_;
};

/**
 * Read data through the index.
 */
class ReadIndex final : public Explore {
public:
    explicit ReadIndex(GraphSpaceID space) {
        kind_ = PlanNode::Kind::kReadIndex;
        space_ = space;
    }

    std::string explain() const override;
};

/**
 * A Filter node helps filt some records with condition.
 */
class Filter final : public SingleInputNode {
public:
    Filter(PlanNode* input, Expression* condition) {
        kind_ = PlanNode::Kind::kFilter;
        input_ = std::move(input);
        condition_ = condition;
    }

    static PlanNode* make(PlanNode* input,
                          Expression* condition,
                          ExecutionPlan* plan) {
        auto filter = std::make_unique<Filter>(input, condition);
        return plan->addPlanNode(std::move(filter));
    }

    void setInput(PlanNode* input) {
        input_ = input;
    }

    const Expression* condition() const {
        return condition_;
    }

    std::string explain() const override;

private:
    Expression*                 condition_;
};

/**
 * Now we have three kind of set operations:
 *   UNION,
 *   INTERSECT,
 *   MINUS
 */
class SetOp : public BiInputNode {
public:
    SetOp() = default;

    SetOp(PlanNode* left, PlanNode* right) {
        left_ = left;
        right_ = right;
    }
};

/**
 * Combine two set of records.
 */
class Union final : public SetOp {
public:
    Union(PlanNode* left, PlanNode* right) : SetOp(left, right) {
        kind_ = PlanNode::Kind::kUnion;
    }

    static PlanNode* make(PlanNode* left, PlanNode* right, ExecutionPlan* plan) {
        auto unionNode = std::make_unique<Union>(left, right);
        return plan->addPlanNode(std::move(unionNode));
    }

    std::string explain() const override;
};

/**
 * Return the intersected records between two sets.
 */
class Intersect final : public SetOp {
public:
    Intersect(PlanNode* left, PlanNode* right) : SetOp(left, right) {
        kind_ = PlanNode::Kind::kIntersect;
    }

    static PlanNode* make(PlanNode* left, PlanNode* right, ExecutionPlan* plan) {
        auto intersect = std::make_unique<Intersect>(left, right);
        return plan->addPlanNode(std::move(intersect));
    }

    std::string explain() const override;
};

/**
 * Do subtraction between two sets.
 */
class Minus final : public SetOp {
public:
    Minus(PlanNode* left, PlanNode* right) : SetOp(left, right) {
        kind_ = PlanNode::Kind::kMinus;
    }

    static PlanNode* make(PlanNode* left, PlanNode* right, ExecutionPlan* plan) {
        auto minus = std::make_unique<Minus>(left, right);
        return plan->addPlanNode(std::move(minus));
    }

    std::string explain() const override;
};

/**
 * Project is used to specify output vars or field.
 */
class Project final : public SingleInputNode {
public:
    Project(PlanNode* input, YieldColumns* cols) {
        kind_ = PlanNode::Kind::kProject;
        input = std::move(input);
        cols_ = cols;
    }

    static PlanNode* make(PlanNode* input,
                          YieldColumns* cols,
                          ExecutionPlan* plan) {
        auto project = std::make_unique<Project>(input, cols);
        return plan->addPlanNode(std::move(project));
    }

    void setInput(PlanNode* input) {
        input_ = input;
    }

    std::string explain() const override;

private:
    YieldColumns*               cols_;
};

/**
 * Sort the given record set.
 */
class Sort final : public SingleInputNode {
public:
    Sort(PlanNode* input, OrderFactors* factors) {
        kind_ = PlanNode::Kind::kSort;
        input_ = input;
        factors_ = factors;
    }

    static PlanNode* make(PlanNode* input,
                          OrderFactors* factors,
                          ExecutionPlan* plan) {
        auto sort = std::make_unique<Sort>(input, factors);
        return plan->addPlanNode(std::move(sort));
    }

    const OrderFactors* factors() {
        return factors_;
    }

    std::string explain() const override;

private:
    PlanNode*       input_;
    OrderFactors*   factors_;
};

/**
 * Output the records with the given limitation.
 */
class Limit final : public SingleInputNode {
public:
    Limit(int64_t offset, int64_t count) {
        kind_ = PlanNode::Kind::kLimit;
        offset_ = offset;
        count_ = count;
    }

    Limit(PlanNode* input, int64_t offset, int64_t count) {
        kind_ = PlanNode::Kind::kLimit;
        input_ = input;
        offset_ = offset;
        count_ = count;
    }

    static PlanNode* make(PlanNode* input,
                          int64_t offset,
                          int64_t count,
                          ExecutionPlan* plan) {
        auto limit = std::make_unique<Limit>(input, offset, count);
        return plan->addPlanNode(std::move(limit));
    }

    void setInput(PlanNode* input) {
        input_ = input;
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
class Aggregate : public SingleInputNode {
public:
    explicit Aggregate(YieldColumns* groupCols) {
        kind_ = PlanNode::Kind::kAggregate;
        groupCols_ = groupCols;
    }

    explicit Aggregate(PlanNode* input,
                       YieldColumns* groupCols) {
        kind_ = PlanNode::Kind::kAggregate;
        input_ = input;
        groupCols_ = groupCols;
    }

    static PlanNode* make(PlanNode* input,
                          YieldColumns* groupCols,
                          ExecutionPlan* plan) {
        auto agg = std::make_unique<Aggregate>(input, groupCols);
        return plan->addPlanNode(std::move(agg));
    }

    void setInput(PlanNode* input) {
        input_ = input;
    }

    const YieldColumns* groups() const {
        return groupCols_;
    }

    std::string explain() const override;

private:
    YieldColumns*   groupCols_;
};

class BinarySelect : public SingleInputNode {
public:
    explicit BinarySelect(Expression* condition) {
        condition_ = condition;
    }

private:
    Expression*  condition_;
};

class Selector : public BinarySelect {
public:
    explicit Selector(Expression* condition)
        : BinarySelect(condition) {
        kind_ = PlanNode::Kind::kSelector;
    }

    Selector(PlanNode* input,
             PlanNode* ifBranch,
             PlanNode* elseBranch,
             Expression* condition)
        : BinarySelect(condition) {
        kind_ = PlanNode::Kind::kSelector;
        input_ = input;
        if_ = ifBranch;
        else_ = elseBranch;
    }

    static PlanNode* make(PlanNode* input,
                          PlanNode* ifBranch,
                          PlanNode* elseBranch,
                          Expression* condition,
                          ExecutionPlan* plan) {
        auto selector = std::make_unique<Selector>(input, ifBranch, elseBranch, condition);
        return plan->addPlanNode(std::move(selector));
    }

    std::string explain() const override;

private:
    PlanNode*   if_;
    PlanNode*   else_;
};

class Loop : public BinarySelect {
public:
    explicit Loop(Expression* condition)
        : BinarySelect(condition) {
        kind_ = PlanNode::Kind::kLoop;
    }

    Loop(PlanNode* input, PlanNode* body, Expression* condition)
        : BinarySelect(condition) {
        kind_ = PlanNode::Kind::kLoop;
        input_ = input;
        body_ = body;
    }

    static PlanNode* make(PlanNode* input,
                          PlanNode* body,
                          Expression* condition,
                          ExecutionPlan* plan) {
        auto loop = std::make_unique<Loop>(input, body, condition);
        return plan->addPlanNode(std::move(loop));
    }

    std::string explain() const override;

private:
    PlanNode*   body_;
};

class RegisterSpaceToSession : public SingleInputNode {
public:
    explicit RegisterSpaceToSession(GraphSpaceID space) {
        kind_ = PlanNode::Kind::kRegisterSpaceToSession;
        space_ = space;
    }

    RegisterSpaceToSession(PlanNode* input,
            GraphSpaceID space) {
        kind_ = PlanNode::Kind::kRegisterSpaceToSession;
        input_ = std::move(input);
        space_ = space;
    }

    static PlanNode* make(PlanNode* input,
                          GraphSpaceID space,
                          ExecutionPlan* plan) {
        auto regSpace = std::make_unique<RegisterSpaceToSession>(input, space);
        return plan->addPlanNode(std::move(regSpace));
    }

    std::string explain() const override;

private:
    GraphSpaceID    space_;
};

class Dedup : public SingleInputNode {
public:
    Dedup(PlanNode* input, std::string&& var) {
        kind_ = PlanNode::Kind::kDedup;
        input_ = input;
        var_ = std::move(var);
    }

    static PlanNode* make(PlanNode* input,
                          std::string&& var,
                          ExecutionPlan* plan) {
        auto dedup = std::make_unique<Dedup>(input, std::move(var));
        return plan->addPlanNode(std::move(dedup));
    }

    std::string explain() const override;

private:
    std::string     var_;
};

class ProduceSemiShortestPath : public PlanNode {
};

class ConjunctPath : public PlanNode {
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_QUERY_H_
