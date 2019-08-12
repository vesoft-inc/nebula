/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_ADMIN_ADDHOSTSEXECUTOR_H_
#define GRAPH_ADMIN_ADDHOSTSEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class AddHostsExecutor final : public Executor {
public:
    AddHostsExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "AddHostsExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    AddHostsSentence        *sentence_{nullptr};
    std::vector<HostAddr>    hosts_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_ADMIN_ADDHOSTSEXECUTOR_H_
