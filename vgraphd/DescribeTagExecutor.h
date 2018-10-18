/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_DESCRIBETAGEXECUTOR_H_
#define VGRAPHD_DESCRIBETAGEXECUTOR_H_

#include "base/Base.h"
#include "vgraphd/Executor.h"

namespace vesoft {
namespace vgraph {

class DescribeTagExecutor final : public Executor {
public:
    DescribeTagExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "DescribeTagExecutor";
    }

    Status VE_MUST_USE_RESULT prepare() override;

    void execute() override;

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    DescribeTagSentence                        *sentence_{nullptr};
    std::string                                *tag_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>    resp_;
};

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_DESCRIBETAGEXECUTOR_H_
