/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_INSERTVERTEXEXECUTOR_H_
#define GRAPH_INSERTVERTEXEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"
#include "meta/SchemaManager.h"

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
    using TagSchema = std::shared_ptr<const meta::SchemaProviderIf>;
    InsertVertexSentence                       *sentence_{nullptr};
    bool                                        overwritable_{true};
    TagID                                       tagId_{0};
    TagSchema                                   schema_;
    std::vector<std::string*>                   properties_;
    std::vector<VertexRowItem*>                 rows_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_INSERTVERTEXEXECUTOR_H_
