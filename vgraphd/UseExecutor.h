/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_USEEXECUTOR_H_
#define VGRAPHD_USEEXECUTOR_H_

#include "base/Base.h"
#include "vgraphd/Executor.h"

namespace vesoft {
namespace vgraph {

class UseExecutor final : public Executor {
public:
    UseExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "UseExecutor";
    }

    Status VE_MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    UseSentence                                *sentence_{nullptr};
};

}   // namespace vgraph
}   // namespace vesoft


#endif  // VGRAPHD_USEEXECUTOR_H_
