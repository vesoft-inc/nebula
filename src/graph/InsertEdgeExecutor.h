/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_INSERTEDGEEXECUTOR_H_
#define GRAPH_INSERTEDGEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class InsertEdgeExecutor final : public Executor {
public:
    InsertEdgeExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "InsertEdgeExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    InsertEdgeSentence                         *sentence_{nullptr};
    bool                                        overwritable_{true};
    int64_t                                     srcid_{0};
    int64_t                                     dstid_{0};
    int64_t                                     rank_{0};
    std::string                                *edge_{nullptr};
    std::vector<std::string*>                   properties_;
    std::vector<Expression*>                    values_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_INSERTEDGEEXECUTOR_H_
