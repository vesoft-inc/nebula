/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SERVICE_QUERYINSTANCE_H_
#define SERVICE_QUERYINSTANCE_H_

#include "base/Base.h"
#include "base/Status.h"
#include "cpp/helpers.h"
#include "parser/GQLParser.h"
#include "validator/ASTValidator.h"
#include "service/ExecutionContext.h"

/**
 * QueryInstance coordinates the execution process,
 * i.e. parse a query into a parsing tree, analyze the tree,
 * initiate and finalize the execution.
 */

namespace nebula {
namespace graph {

class QueryInstance final : public cpp::NonCopyable, public cpp::NonMovable {
public:
    explicit QueryInstance(std::unique_ptr<ExecutionContext> ectx) {
        ectx_ = std::move(ectx);
    }

    ~QueryInstance() = default;

    void execute();

    /**
     * If the whole execution was done, `onFinish' would be invoked.
     * All `onFinish' should do is to ask `executor_' to fill the `ectx()->rctx()->resp()',
     * which in turn asks the last sub-executor to do the actual job.
     */
    void onFinish();

    /**
     * If any error occurred during the execution, `onError' should
     * be invoked with a status to indicate the reason.
     */
    void onError(Status);

    ExecutionContext* ectx() const {
        return ectx_.get();
    }

private:
    std::unique_ptr<SequentialSentences>        sentences_;
    std::unique_ptr<ExecutionContext>           ectx_;
    std::unique_ptr<ASTValidator>               validator_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_QueryInstance_H_
