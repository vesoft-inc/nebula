/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_COMPOUNDEXECUTOR_H_
#define VGRAPHD_COMPOUNDEXECUTOR_H_

#include "base/Base.h"
#include "vgraphd/Executor.h"


namespace vesoft {
namespace vgraph {

class CompoundExecutor final : public Executor {
public:
    CompoundExecutor(CompoundSentence *compound, ExecutionContext *ectx);

    const char* name() const override {
        return "CompoundExecutor";
    }

    Status VE_MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    CompoundSentence                           *compound_{nullptr};
    std::vector<std::unique_ptr<Executor>>      executors_;
};


}   // namespace vgraph
}   // namespace vesoft


#endif  // VGRAPHD_COMPOUNDEXECUTOR_H_
