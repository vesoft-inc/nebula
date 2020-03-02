/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_TRUNCATESPACEEXECUTOR_H_
#define GRAPH_TRUNCATESPACEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class TruncateSpaceExecutor final : public Executor {
public:
    TruncateSpaceExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "TruncateSpaceExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    void truncateSpace();
    void createSpace(const meta::SpaceDesc &spaceDesc);
    void copySpace();
    void renameSpace();
    void dropSpace(const std::string &spaceName, bool isIfExists);

private:
    TruncateSpaceSentence            *sentence_{nullptr};
    const std::string                *spaceName_{nullptr};
    std::string                       tempSpaceName_;
    GraphSpaceID                      fromSpaceId_{-1};
    GraphSpaceID                      newSpaceId_{-1};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_TRUNCATESPACEEXECUTOR_H_
