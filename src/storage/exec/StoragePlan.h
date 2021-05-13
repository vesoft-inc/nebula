/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_STORAGEPLAN_H_
#define STORAGE_EXEC_STORAGEPLAN_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

/*
Origined from folly::FutureDAG, not thread-safe.

The StoragePlan contains a set of RelNode, all you need to do is define a RelNode, add it
to plan by calling addNode, which will return the index of the RelNode in this plan.
The denpendencies between different nodes is defined by calling addDependency in RelNode.

To run the plan, call the go method, you could get the final result.

For simplicity, StoragePlan has not detect if has cycle in it for now, user must make sure no cycle
dependency in it.

For this version, if there are more than one node depends on the same node, that node will be
executed **more than once**. If you want to make sure each node would be executed exactly once,
StoragePlan would be inappropriate. In that case, please refer to the previous implement,
FutureDAG in StorageDAGBenchmark.cpp
*/
template<typename T>
class StoragePlan {
public:
    nebula::cpp2::ErrorCode go(PartitionID partId, const T& input) {
        // find all leaf nodes, and a dummy output node depends on all leaf node.
        if (firstLoop_) {
            auto output = std::make_unique<RelNode<T>>();
            for (const auto& node : nodes_) {
                if (!node->hasDependents_) {
                    // add dependency of output node
                    output->addDependency(node.get());
                }
            }
            outputIdx_ = addNode(std::move(output));
            firstLoop_ = false;
        }
        CHECK_GE(outputIdx_, 0);
        CHECK_LT(outputIdx_, nodes_.size());
        return nodes_[outputIdx_]->execute(partId, input);
    }

    nebula::cpp2::ErrorCode go(PartitionID partId) {
        // find all leaf nodes, and a dummy output node depends on all leaf node.
        if (firstLoop_) {
            auto output = std::make_unique<RelNode<T>>();
            for (const auto& node : nodes_) {
                if (!node->hasDependents_) {
                    // add dependency of output node
                    output->addDependency(node.get());
                }
            }
            outputIdx_ = addNode(std::move(output));
            firstLoop_ = false;
        }
        CHECK_GE(outputIdx_, 0);
        CHECK_LT(outputIdx_, nodes_.size());
        return nodes_[outputIdx_]->execute(partId);
    }

    int32_t addNode(std::unique_ptr<RelNode<T>> node) {
        nodes_.emplace_back(std::move(node));
        return nodes_.size() - 1;
    }

    RelNode<T>* getNode(size_t idx) {
        CHECK_LT(idx, nodes_.size());
        return nodes_[idx].get();
    }

private:
    bool firstLoop_ = true;
    int32_t outputIdx_ = -1;
    std::vector<std::unique_ptr<RelNode<T>>> nodes_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_STORAGEPLAN_H_
