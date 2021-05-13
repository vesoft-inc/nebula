
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "storage/exec/StoragePlan.h"
#include <folly/Benchmark.h>

namespace nebula {
namespace storage {

template<typename T> class FutureDAG;

template<typename T>
class FutureNode {
    friend class FutureDAG<T>;
public:
    folly::Future<nebula::cpp2::ErrorCode> execute(PartitionID, const T&) {
        VLOG(1) << name_;
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    void addDependency(FutureNode<T>* dep) {
        dependencies_.emplace_back(dep);
        dep->hasDependents_ = true;
    }

    FutureNode() = default;
    FutureNode(const FutureNode&) = delete;
    FutureNode(FutureNode&&) = default;
    explicit FutureNode(const std::string& name): name_(name) {}

private:
    std::string name_;
    folly::SharedPromise<nebula::cpp2::ErrorCode> promise_;
    std::vector<FutureNode<T>*> dependencies_;
    bool hasDependents_ = false;
};

template<typename T>
class FutureDAG {
public:
    folly::Future<nebula::cpp2::ErrorCode> go(PartitionID partId, const T& vId) {
        // find the root nodes and leaf nodes. All root nodes depends on a dummy input node,
        // and a dummy output node depends on all leaf node.
        if (firstLoop_) {
            auto output = std::make_unique<FutureNode<T>>();
            auto input = std::make_unique<FutureNode<T>>();
            for (const auto& node : nodes_) {
                if (node->dependencies_.empty()) {
                    // add dependency of root node
                    node->addDependency(input.get());
                }
                if (!node->hasDependents_) {
                    // add dependency of output node
                    output->addDependency(node.get());
                }
            }
            outputIdx_ = addNode(std::move(output));
            inputIdx_ = addNode(std::move(input));
            firstLoop_ = false;
        }

        for (size_t i = 0; i < nodes_.size() - 1; i++) {
            auto* node = nodes_[i].get();
            std::vector<folly::Future<nebula::cpp2::ErrorCode>> futures;
            for (auto dep : node->dependencies_) {
                futures.emplace_back(dep->promise_.getFuture());
            }

            folly::collectAll(futures)
                .via(workers_[i])
                .thenTry([node, partId, &vId] (auto&& t) {
                    CHECK(!t.hasException());
                    for (const auto& codeTry : t.value()) {
                        if (codeTry.hasException()) {
                            node->promise_.setException(std::move(codeTry.exception()));
                            return;
                        } else if (codeTry.value() != nebula::cpp2::ErrorCode::SUCCEEDED) {
                            node->promise_.setValue(codeTry.value());
                            return;
                        }
                    }
                    node->execute(partId, vId).thenValue([node] (auto&& code) {
                        node->promise_.setValue(code);
                    }).thenError([node] (auto&& ex) {
                        LOG(ERROR) << "Exception occurs, perhaps should not reach here: "
                                    << ex.what();
                        node->promise_.setException(std::move(ex));
                    });
                });
        }

        getNode(inputIdx_)->promise_.setValue(nebula::cpp2::ErrorCode::SUCCEEDED);
        return getNode(outputIdx_)->promise_.getFuture()
            .thenTry([this](auto&& t) -> folly::Future<nebula::cpp2::ErrorCode> {
                CHECK(!t.hasException());
                reset();
                return t.value();
            });
    }

    size_t addNode(std::unique_ptr<FutureNode<T>> node, folly::Executor* executor = nullptr) {
        workers_.emplace_back(executor);
        nodes_.emplace_back(std::move(node));
        return nodes_.size() - 1;
    }

    FutureNode<T>* getNode(size_t idx) {
        CHECK_LT(idx, nodes_.size());
        return nodes_[idx].get();
    }

private:
    // reset all promise after dag has run
    void reset() {
        for (auto& node : nodes_) {
            node->promise_ = folly::SharedPromise<nebula::cpp2::ErrorCode>();
        }
    }

    bool firstLoop_ = true;
    size_t inputIdx_;
    size_t outputIdx_;
    std::vector<folly::Executor*> workers_;
    std::vector<std::unique_ptr<FutureNode<T>>> nodes_;
};

FutureDAG<std::string> fanoutFutureDAG() {
    FutureDAG<std::string> dag;
    auto out = std::make_unique<FutureNode<std::string>>("leaf");
    for (size_t i = 0; i < 10; i++) {
        auto node = std::make_unique<FutureNode<std::string>>(folly::to<std::string>(i));
        auto idx = dag.addNode(std::move(node));
        out->addDependency(dag.getNode(idx));
    }
    dag.addNode(std::move(out));
    return dag;
}

StoragePlan<std::string> fanoutStorageDAG() {
    StoragePlan<std::string> dag;
    auto out = std::make_unique<RelNode<std::string>>("leaf");
    for (size_t i = 0; i < 10; i++) {
        auto node = std::make_unique<RelNode<std::string>>(folly::to<std::string>(i));
        auto idx = dag.addNode(std::move(node));
        out->addDependency(dag.getNode(idx));
    }
    dag.addNode(std::move(out));
    return dag;
}

FutureDAG<std::string> chainFutureDAG() {
    FutureDAG<std::string> dag;
    size_t lastIdx = 0;
    for (size_t i = 0; i < 10; i++) {
        auto node = std::make_unique<FutureNode<std::string>>(folly::to<std::string>(i));
        if (i != 0) {
            node->addDependency(dag.getNode(lastIdx));
        }
        lastIdx = dag.addNode(std::move(node));
    }
    auto out = std::make_unique<FutureNode<std::string>>("leaf");
    out->addDependency(dag.getNode(lastIdx));
    dag.addNode(std::move(out));
    return dag;
}

StoragePlan<std::string> chainStorageDAG() {
    StoragePlan<std::string> dag;
    size_t lastIdx = 0;
    for (size_t i = 0; i < 10; i++) {
        auto node = std::make_unique<RelNode<std::string>>(folly::to<std::string>(i));
        if (i != 0) {
            node->addDependency(dag.getNode(lastIdx));
        }
        lastIdx = dag.addNode(std::move(node));
    }
    auto out = std::make_unique<RelNode<std::string>>("leaf");
    out->addDependency(dag.getNode(lastIdx));
    dag.addNode(std::move(out));
    return dag;
}

BENCHMARK(future_fanout, iters) {
    FutureDAG<std::string> dag;
    BENCHMARK_SUSPEND {
        dag = fanoutFutureDAG();
    }
    for (size_t i = 0; i < iters; i++) {
        dag.go(0, "fanoutFutureDAG");
    }
}

BENCHMARK_RELATIVE(recursive_fanout, iters) {
    StoragePlan<std::string> dag;
    BENCHMARK_SUSPEND {
        dag = fanoutStorageDAG();
    }
    for (size_t i = 0; i < iters; i++) {
        dag.go(0, "fanoutStorageDAG");
    }
}

BENCHMARK_DRAW_LINE();

BENCHMARK(future_chain, iters) {
    FutureDAG<std::string> dag;
    BENCHMARK_SUSPEND {
        dag = chainFutureDAG();
    }
    for (size_t i = 0; i < iters; i++) {
        dag.go(0, "chainFutureDAG");
    }
}

BENCHMARK_RELATIVE(recursive_chain, iters) {
    StoragePlan<std::string> dag;
    BENCHMARK_SUSPEND {
        dag = chainStorageDAG();
    }
    for (size_t i = 0; i < iters; i++) {
        dag.go(0, "chainStorageDAG");
    }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}
/*
40 processors, Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz.
============================================================================
StorageDAGBenchmark.cpprelative                            time/iter  iters/s
============================================================================
future_fanout                                              150.59us    6.64K
recursive_fanout                                34326.27%   438.69ns    2.28M
----------------------------------------------------------------------------
future_chain                                               128.58us    7.78K
recursive_chain                                 27904.03%   460.81ns    2.17M
============================================================================
*/
