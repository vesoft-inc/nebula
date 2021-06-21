/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_EXECUTOR_H_
#define EXECUTOR_EXECUTOR_H_

#include <set>
#include <string>
#include <vector>

#include <folly/futures/Future.h>

#include "common/base/Status.h"
#include "common/cpp/helpers.h"
#include "common/datatypes/Value.h"
#include "common/time/Duration.h"
#include "context/ExecutionContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

class PlanNode;
class QueryContext;

class Executor : private cpp::NonCopyable, private cpp::NonMovable {
public:
    // Create executor according to plan node
    static Executor *create(const PlanNode *node, QueryContext *qctx);

    virtual ~Executor();

    // Prepare or initialize executor before each execution
    virtual Status open();

    // Each executor inherited from this class should get input values from ExecutionContext,
    // evaluate expressions and save output result back to ExecutionContext by `finish'
    virtual folly::Future<Status> execute() = 0;

    // Cleanup or reset executor some states after each execution
    virtual Status close();

    QueryContext *qctx() const {
        return qctx_;
    }

    int64_t id() const {
        return id_;
    }

    const std::string &name() const {
        return name_;
    }

    const PlanNode *node() const {
        return node_;
    }

    const std::set<Executor *> &depends() const {
        return depends_;
    }

    const std::set<Executor *> &successors() const {
        return successors_;
    }

    Executor *dependsOn(Executor *dep) {
        depends_.emplace(dep);
        dep->successors_.emplace(this);
        return this;
    }

    template <typename T>
    static std::enable_if_t<std::is_base_of<PlanNode, T>::value, const T *> asNode(
        const PlanNode *node) {
        return static_cast<const T *>(node);
    }

    // Throw runtime error to stop whole execution early
    folly::Future<Status> error(Status status) const;

protected:
    static Executor *makeExecutor(const PlanNode *node,
                                  QueryContext *qctx,
                                  std::unordered_map<int64_t, Executor *> *visited);

    static Executor *makeExecutor(QueryContext *qctx, const PlanNode *node);

    // Only allow derived executor to construct
    Executor(const std::string &name, const PlanNode *node, QueryContext *qctx);

    // Start a future chain and bind it to thread pool
    folly::Future<Status> start(Status status = Status::OK()) const;

    folly::Executor *runner() const;

    void drop();

    // Store the result of this executor to execution context
    Status finish(Result &&result);
    // Store the default result which not used for later executor
    Status finish(Value &&value);

    int64_t id_;

    // Executor name
    std::string name_;

    // Relative Plan Node
    const PlanNode *node_;

    QueryContext *qctx_;
    // Execution context for saving some execution data
    ExecutionContext *ectx_;

    // Topology
    std::set<Executor *> depends_;
    std::set<Executor *> successors_;

    // profiling data
    uint64_t numRows_{0};
    uint64_t execTime_{0};
    time::Duration totalDuration_;
    std::unordered_map<std::string, std::string> otherStats_;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_EXECUTOR_H_
