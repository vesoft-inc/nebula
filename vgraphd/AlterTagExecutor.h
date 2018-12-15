/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_ALTERTAGEXECUTOR_H_
#define VGRAPHD_ALTERTAGEXECUTOR_H_

#include "base/Base.h"
#include "vgraphd/Executor.h"

namespace vesoft {
namespace vgraph {

class AlterTagExecutor final : public Executor {
public:
    AlterTagExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "AlterTagExecutor";
    }

    Status VE_MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    AlterTagSentence                           *sentence_{nullptr};
};

}   // namespace vgraph
}   // namespace vesoft


#endif  // VGRAPHD_ALTERTAGEXECUTOR_H_
