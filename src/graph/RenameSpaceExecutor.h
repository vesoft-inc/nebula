/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_RENAMESPACEEXECUTOR_H_
#define GRAPH_RENAMESPACEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class RenameSpaceExecutor final : public Executor {
public:
    RenameSpaceExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "RenameSpaceExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    RenameSpaceSentence            *sentence_{nullptr};
    const std::string              *fromSpaceName_{nullptr};
    const std::string              *toSpaceName_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_RENAMESPACEEXECUTOR_H_

