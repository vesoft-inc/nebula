/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_INSERTEDGEEXECUTOR_H_
#define GRAPH_INSERTEDGEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"
#include "meta/SchemaManager.h"

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
    using EdgeSchema = std::shared_ptr<const meta::SchemaProviderIf>;
    InsertEdgeSentence                         *sentence_{nullptr};
    bool                                        overwritable_{true};
    EdgeType                                    edgeType_{0};
    EdgeSchema                                  schema_;
    std::vector<std::string*>                   properties_;
    std::vector<EdgeRowItem*>                   rows_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_INSERTEDGEEXECUTOR_H_
