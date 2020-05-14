/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_SIMPLEDAG_H_
#define STORAGE_EXEC_SIMPLEDAG_H_

#include "base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

/*
The StorageDAG defines a simple dag, all you need to do is define a RelNode, add it to dag by
calling addNode, which will return the index of the RelNode in this dag. The denpendencies
between different nodes is defined by calling addDependency in RelNode.

To run the dag, run the go method, you could get the Future of result. 

For simplicity, StorageDAG has not detect if has cycle in it for now, user must make sure no
cycle in it. And re-run StorageDAG is meaningless.
*/
class StorageDAG {
public:
    folly::Future<kvstore::ResultCode> go() {
        // find the root nodes and leaf nodes. All root nodes depends on a dummy input node,
        // and a dummy output node depends on all leaf node.
        auto output = std::make_unique<RelNode>();
        for (const auto& node : nodes_) {
            if (node->dependencies_.empty()) {
                // add dependency of root node
                node->addDependency(&input_);
            }
            if (!node->hasDependents_) {
                // add dependency of output node
                output->addDependency(node.get());
            }
        }
        addNode(std::move(output));

        for (size_t i = 0; i < nodes_.size(); i++) {
            auto* node = nodes_[i].get();
            std::vector<folly::Future<kvstore::ResultCode>> futures;
            for (auto dep : node->dependencies_) {
                futures.emplace_back(dep->promise_.getFuture());
            }

            folly::collectAll(futures)
                .via(workers_[i])
                .thenTry([node] (auto&& t) {
                    CHECK(!t.hasException());
                    for (const auto& codeTry : t.value()) {
                        if (codeTry.hasException()) {
                            node->promise_.setException(std::move(codeTry.exception()));
                            return;
                        } else if (codeTry.value() != kvstore::ResultCode::SUCCEEDED) {
                            node->promise_.setValue(codeTry.value());
                            return;
                        }
                    }
                    node->execute().thenValue([node] (auto&& code) {
                        node->promise_.setValue(code);
                    }).thenError([node] (auto&& ex) {
                        LOG(ERROR) << "Exception occurs, perhaps should not reach here: "
                                    << ex.what();
                        node->promise_.setException(std::move(ex));
                    });
                });
        }

        input_.promise_.setValue(kvstore::ResultCode::SUCCEEDED);
        return nodes_.back()->promise_.getFuture();
    }

    size_t addNode(std::unique_ptr<RelNode> node, folly::Executor* executor = nullptr) {
        workers_.emplace_back(executor);
        nodes_.emplace_back(std::move(node));
        return nodes_.size() - 1;
    }

    RelNode* getNode(size_t idx) {
        CHECK_LT(idx, nodes_.size());
        return nodes_[idx].get();
    }

private:
    RelNode input_;
    std::vector<folly::Executor*> workers_;
    std::vector<std::unique_ptr<RelNode>> nodes_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_SIMPLEDAG_H_
