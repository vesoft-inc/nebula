/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/algo/BFSShortestPathExecutor.h"

#include "planner/plan/Algo.h"

namespace nebula {
namespace graph {
folly::Future<Status> BFSShortestPathExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* bfs = asNode<BFSShortestPath>(node());
    auto iter = ectx_->getResult(bfs->inputVar()).iter();
    VLOG(1) << "current: " << node()->outputVar();
    VLOG(1) << "input: " << bfs->inputVar();
    DCHECK(!!iter);

    DataSet ds;
    ds.colNames = node()->colNames();
    std::unordered_multimap<Value, Value> interim;

    for (; iter->valid(); iter->next()) {
        auto edgeVal = iter->getEdge();
        if (!edgeVal.isEdge()) {
            continue;
        }
        auto& edge = edgeVal.getEdge();
        auto visited = visited_.find(edge.dst) != visited_.end();
        if (visited) {
            continue;
        }

        // save the starts.
        visited_.emplace(edge.src);
        VLOG(1) << "dst: " << edge.dst << " edge: " << edge;
        interim.emplace(edge.dst, std::move(edgeVal));
    }
    for (auto& kv : interim) {
        auto dst = std::move(kv.first);
        auto edge = std::move(kv.second);
        Row row;
        row.values.emplace_back(dst);
        row.values.emplace_back(std::move(edge));
        ds.rows.emplace_back(std::move(row));
        visited_.emplace(dst);
    }
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}
}  // namespace graph
}  // namespace nebula
