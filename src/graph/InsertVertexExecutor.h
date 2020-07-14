/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_INSERTVERTEXEXECUTOR_H_
#define GRAPH_INSERTVERTEXEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class InsertVertexExecutor final : public Executor {
public:
    InsertVertexExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "InsertVertexExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    Status check();
    StatusOr<std::vector<storage::cpp2::Vertex>> prepareVertices();

private:
    using TagSchema = std::shared_ptr<const meta::SchemaProviderIf>;

    InsertVertexSentence                                  *sentence_{nullptr};
    std::unique_ptr<ExpressionContext>                     expCtx_;
    bool                                                   overwritable_{true};
    std::vector<VertexRowItem*>                            rows_;
    std::vector<TagID>                                     tagIds_;
    std::vector<TagSchema>                                 schemas_;
    std::vector<std::vector<std::string*>>                 tagProps_;
    std::vector<std::unordered_map<std::string, int32_t>>  propsPositions_;
    GraphSpaceID                                           spaceId_{-1};
    std::unordered_map<std::string, VariantType>           defaultValues_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_INSERTVERTEXEXECUTOR_H_
