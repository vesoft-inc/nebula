/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_ALTEREDGEEXECUTOR_H_
#define GRAPH_ALTEREDGEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class AlterEdgeExecutor final : public Executor {
public:
    AlterEdgeExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "AlterEdgeExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    Status getSchema();

private:
    AlterEdgeSentence                                  *sentence_{nullptr};
    std::vector<nebula::meta::cpp2::AlterSchemaItem>    options_;
    nebula::cpp2::SchemaProp                            schemaProp_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_ALTEREDGEEXECUTOR_H_
