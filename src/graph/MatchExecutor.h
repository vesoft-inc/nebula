/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_MATCHEXECUTOR_H_
#define GRAPH_MATCHEXECUTOR_H_

#include "base/Base.h"
#include "graph/TraverseExecutor.h"

namespace nebula {
namespace graph {

class MatchExecutor final : public TraverseExecutor {
public:
    MatchExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "UseExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void feedResult(std::unique_ptr<InterimResult> result) override {
        UNUSED(result);
    }

    void execute() override {}

private:
    MatchSentence                                *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_MATCHEXECUTOR_H_
