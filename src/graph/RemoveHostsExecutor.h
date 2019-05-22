/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_REMOVEHOSTSEXECUTOR_H_
#define GRAPH_REMOVEHOSTSEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {


class RemoveHostsExecutor final : public Executor {
public:
    RemoveHostsExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "RemoveHostsExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    RemoveHostsSentence     *sentence_{nullptr};
    std::vector<HostAddr>    host_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_REMOVEHOSTSEXECUTOR_H_

