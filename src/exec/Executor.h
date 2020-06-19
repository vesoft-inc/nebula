/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_EXECUTOR_H_
#define EXEC_EXECUTOR_H_

#include <memory>
#include <set>
#include <string>
#include <vector>
#include <folly/futures/Future.h>

#include "common/base/Status.h"
#include "common/cpp/helpers.h"
#include "common/datatypes/Value.h"

#include "context/ExecutionContext.h"

namespace nebula {
namespace graph {

class PlanNode;
class QueryContext;

class Executor : private cpp::NonCopyable, private cpp::NonMovable {
public:
    // Create executor according to plan node
    static Executor *makeExecutor(const PlanNode *node, QueryContext *qctx);

    virtual ~Executor() {}

    // Implementation interface of operation logic
    virtual folly::Future<Status> execute() = 0;

    QueryContext *qctx() const {
        return qctx_;
    }

    int64_t id() const;

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

    Executor *addDependent(Executor *dep) {
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

    // Only allow derived executor to construct
    Executor(const std::string &name, const PlanNode *node, QueryContext *qctx);

    // Start a future chain and bind it to thread pool
    folly::Future<Status> start(Status status = Status::OK()) const;

    folly::Executor *runner() const;

    // Store the result of this executor to execution context
    Status finish(nebula::Value &&value);
    Status finish(ExecResult &&result);

    // Dump some execution logging messages
    void dumpLog() const;

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

    // TODO: Some statistics
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_EXECUTOR_H_
