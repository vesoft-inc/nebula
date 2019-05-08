/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_ASSIGNMENTEXECUTOR_H_
#define GRAPH_ASSIGNMENTEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class TraverseExecutor;
class AssignmentExecutor final : public Executor {
public:
    AssignmentExecutor(Sentence *sentence, ExecutionContext *ectx);

     const char* name() const override {
        return "AssignmentExecutor";
    }

     Status MUST_USE_RESULT prepare() override;

     void execute() override;

private:
    AssignmentSentence                         *sentence_{nullptr};
    std::unique_ptr<TraverseExecutor>           executor_;
    const std::string                          *var_;
};


}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_ASSIGNMENTEXECUTOR_H_
