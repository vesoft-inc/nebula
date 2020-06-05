/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
    Status check();
    StatusOr<std::vector<storage::cpp2::Edge>> prepareEdges();

private:
    using EdgeSchema = std::shared_ptr<const meta::SchemaProviderIf>;

    InsertEdgeSentence                               *sentence_{nullptr};
    std::unique_ptr<ExpressionContext>                expCtx_;
    bool                                              overwritable_{true};
    EdgeType                                          edgeType_{0};
    EdgeSchema                                        schema_;
    std::vector<std::string*>                         props_;
    std::vector<EdgeRowItem*>                         rows_;
    GraphSpaceID                                      spaceId_{-1};
    std::unordered_map<std::string, VariantType>      defaultValues_;
    std::unordered_map<std::string, int32_t>          propsPosition_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_INSERTEDGEEXECUTOR_H_
