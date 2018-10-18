/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_ALTEREDGEEXECUTOR_H_
#define VGRAPHD_ALTEREDGEEXECUTOR_H_

#include "base/Base.h"
#include "vgraphd/Executor.h"

namespace vesoft {
namespace vgraph {

class AlterEdgeExecutor final : public Executor {
public:
    AlterEdgeExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "AlterEdgeExecutor";
    }

    Status VE_MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    AlterEdgeSentence                          *sentence_{nullptr};
};

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_ALTEREDGEEXECUTOR_H_
