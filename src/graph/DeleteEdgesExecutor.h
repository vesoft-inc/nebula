/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_DELETEEDGESEXECUTOR_H_
#define GRAPH_DELETEEDGESEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class DeleteEdgesExecutor final : public Executor {
public:
    DeleteEdgesExecutor(Sentence *sentence, ExecutionContext *ectx);

     const char* name() const override {
        return "DeleteEdgesExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    Status setupEdgeKeys(EdgeType edgeType);

private:
    DeleteEdgesSentence                         *sentence_{nullptr};
    std::vector<storage::cpp2::EdgeKey>         edgeKeys_;
    std::unique_ptr<ExpressionContext>          expCtx_;
};

}  // namespace graph
}  // namespace nebula

 #endif  // GRAPH_DELETEEDGESEXECUTOR_H_
