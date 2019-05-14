/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_YIELDEXECUTOR_H_
#define GRAPH_YIELDEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

// For now, YIELD is used only for evaluating instant expressions
class YieldExecutor final : public Executor {
public:
    YieldExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "YieldExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    std::vector<std::string> getResultColumnNames() const;

private:
    YieldSentence                              *sentence_;
    std::vector<YieldColumn*>                   yields_;
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_YIELDEXECUTOR_H_
