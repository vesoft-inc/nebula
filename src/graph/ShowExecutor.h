/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_SHOWEXECUTOR_H_
#define GRAPH_SHOWEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class ShowExecutor final : public Executor {
public:
    ShowExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "ShowExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;
    void showHosts();
    void showSpaces();
    void showTags();
    void showEdges();

    void setupResponse(cpp2::ExecutionResponse &resp) override;

private:
    ShowSentence                             *sentence_{nullptr};
    std::unique_ptr<cpp2::ExecutionResponse>  resp_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_SHOWEXECUTOR_H_
