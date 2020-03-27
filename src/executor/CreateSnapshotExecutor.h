/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_CREATESNAPSHOT_H_
#define GRAPH_CREATESNAPSHOT_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class CreateSnapshotExecutor final : public Executor {
public:
    CreateSnapshotExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "CreateSnapshotExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    CreateSnapshotSentence                    *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>  resp_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_CREATESNAPSHOT_H_

