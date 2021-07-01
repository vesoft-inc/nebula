/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_OPTCONTEXT_H_
#define OPTIMIZER_OPTCONTEXT_H_

#include <memory>
#include <unordered_map>

#include "common/cpp/helpers.h"

namespace nebula {

class ObjectPool;

namespace graph {
class QueryContext;
}   // namespace graph

namespace opt {

class OptGroupNode;

class OptContext final : private cpp::NonCopyable, private cpp::NonMovable {
public:
    explicit OptContext(graph::QueryContext *qctx);

    graph::QueryContext *qctx() const {
        return qctx_;
    }

    ObjectPool *objPool() const {
        return objPool_.get();
    }

    bool changed() const {
        return changed_;
    }

    void setChanged(bool changed) {
        changed_ = changed;
    }

    void addPlanNodeAndOptGroupNode(int64_t planNodeId, const OptGroupNode *optGroupNode);
    const OptGroupNode *findOptGroupNodeByPlanNodeId(int64_t planNodeId) const;

private:
    bool changed_{true};
    graph::QueryContext *qctx_{nullptr};
    std::unique_ptr<ObjectPool> objPool_;
    std::unordered_map<int64_t, const OptGroupNode *> planNodeToOptGroupNodeMap_;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_OPTCONTEXT_H_
