/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ALGO_PRODUCESEMISHORTESTPATHEXECUTOR_H_
#define EXECUTOR_ALGO_PRODUCESEMISHORTESTPATHEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {
class ProduceSemiShortestPathExecutor final : public Executor {
public:
    ProduceSemiShortestPathExecutor(const PlanNode* node, QueryContext* qctx)
        : Executor("ProduceSemiShortestPath", node, qctx) {}

    folly::Future<Status> execute() override;

    struct CostPaths {
        double cost_;
        std::vector<Path> paths_;
        CostPaths() = default;
        CostPaths(double cost, std::vector<Path>& paths) : cost_(cost) {
            paths_.swap(paths);
        }
        CostPaths(double cost, std::vector<Path>&& paths) : cost_(cost) {
            paths_.swap(paths);
        }
    };

    struct CostPathsPtr {
        CostPathsPtr() = default;
        CostPathsPtr(double cost, std::vector<const Path*>& paths) : cost_(cost) {
            paths_.swap(paths);
        }
        double cost_;
        std::vector<const Path*> paths_;
    };

    using CostPathMapType = std::unordered_map<Value, std::unordered_map<Value, CostPaths>>;
    using CostPathMapPtr = std::unordered_map<Value, std::unordered_map<Value, CostPathsPtr>>;

private:
    void dstNotInHistory(const Edge& edge, CostPathMapType&);

    void dstInHistory(const Edge& edge, CostPathMapType&);

    void dstInCurrent(const Edge& edge, CostPathMapType&);

    void updateHistory(const Value& dst, const Value& src, double cost, Value& paths);

    std::vector<Path> createPaths(const std::vector<const Path*>& paths, const Edge& edge);

    void removeSamePath(std::vector<Path>& paths, std::vector<const Path*> &historyPaths);

private:
    // dst : {src : <cost, {Path*}>}
    CostPathMapPtr historyCostPathMap_;

    // std::unique_ptr<IWeight> weight_;
};

}   // namespace graph
}   // namespace nebula
#endif  // EXECUTOR_ALGO_PRODUCESEMISHORTESTPATHEXECUTOR_H_
