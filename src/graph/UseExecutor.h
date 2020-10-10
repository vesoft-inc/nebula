/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_USEEXECUTOR_H_
#define GRAPH_USEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"
#include "graph/GraphFlags.h"
#include "common/permission/PermissionManager.h"

namespace nebula {
namespace graph {

class UseExecutor final : public Executor {
public:
    UseExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "UseExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    UseSentence                                *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_USEEXECUTOR_H_
