/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_DELETEEDGEEXECUTOR_H_
#define GRAPH_DELETEEDGEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class DeleteEdgeExecutor final : public Executor {
public:
    DeleteEdgeExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DeleteEdgeExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    DeleteEdgeSentence                          *sentence_{nullptr};
    EdgeType                                    edgeType_{0};
    Expression                                  *filter_{nullptr};
    std::vector<EdgeKeyItem*>                   edgeKeys_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_DELETEEDGEEXECUTOR_H_
