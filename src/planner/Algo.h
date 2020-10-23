/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_ALGO_H_
#define PLANNER_ALGO_H_

#include "common/base/Base.h"
#include "context/QueryContext.h"
#include "planner/PlanNode.h"

namespace nebula {
namespace graph {
class ProduceSemiShortestPath : public SingleInputNode {
public:
    static ProduceSemiShortestPath* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new ProduceSemiShortestPath(qctx, input));
    }
    void setStartsVid(std::vector<Value> starts);

    std::vector<Value> getStartsVid() const {
        return starts_;
    }

private:
    ProduceSemiShortestPath(QueryContext* qctx, PlanNode* input)
        : SingleInputNode(qctx, Kind::kProduceSemiShortestPath, input) {}

    std::vector<Value> starts_;
};

class BFSShortestPath : public SingleInputNode {
public:
    static BFSShortestPath* make(QueryContext* qctx, PlanNode* input) {
        return qctx->objPool()->add(new BFSShortestPath(qctx, input));
    }

private:
    BFSShortestPath(QueryContext* qctx, PlanNode* input)
        : SingleInputNode(qctx, Kind::kBFSShortest, input) {}
};

class ConjunctPath : public BiInputNode {
public:
    enum class PathKind : uint8_t {
        kBiBFS,
        kBiDijkstra,
        kFloyd,
        kAllPath,
    };

    static ConjunctPath* make(QueryContext* qctx,
                              PlanNode* left,
                              PlanNode* right,
                              PathKind pathKind) {
        return qctx->objPool()->add(new ConjunctPath(qctx, left, right, pathKind));
    }

    PathKind pathKind() const {
        return pathKind_;
    }

private:
    ConjunctPath(QueryContext* qctx, PlanNode* left, PlanNode* right, PathKind pathKind)
        : BiInputNode(qctx, Kind::kConjunctPath, left, right) {
        pathKind_ = pathKind;
    }

    PathKind pathKind_;
};

}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_ALGO_H_
