/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_ALTERTAGEXECUTOR_H_
#define GRAPH_ALTERTAGEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class AlterTagExecutor final : public Executor {
public:
    AlterTagExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "AlterTagExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    Status getSchema();

private:
    AlterTagSentence                                   *sentence_{nullptr};
    std::vector<nebula::meta::cpp2::AlterSchemaItem>    options_;
    nebula::cpp2::SchemaProp                            schemaProp_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_ALTERTAGEXECUTOR_H_
