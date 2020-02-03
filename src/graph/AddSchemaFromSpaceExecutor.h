/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef GRAPH_ADDSCHEMAFROMSPACEEXECUTOR_H_
#define GRAPH_ADDSCHEMAFROMSPACEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class AddSchemaFromSpaceExecutor final : public Executor {
public:
    AddSchemaFromSpaceExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "AddSchemaFromSpaceExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    AddSchemaFromSpaceSentence                  *sentence_{nullptr};
    const std::string                           *spaceName_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_ADDSCHEMAFROMSPACEEXECUTOR_H_

