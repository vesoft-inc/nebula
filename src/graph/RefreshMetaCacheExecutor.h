/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_REFRESHMETACACHEEXECUTOR_H_
#define GRAPH_REFRESHMETACACHEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class RefreshMetaCacheExecutor final : public Executor {
public:
    RefreshMetaCacheExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "RefreshMetaCacheExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    RefreshMetaCacheSentence            *sentence_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_REFRESHMETACACHEEXECUTOR_H_
