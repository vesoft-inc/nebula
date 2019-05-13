/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_ALTERTAGEXECUTOR_H_
#define GRAPH_ALTERTAGEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class AlterTagExecutor final : public Executor {
public:
    AlterTagExecutor(Sentence *sentence, ExecutionContext *ectx);

    const char* name() const override {
        return "AlterTagExecutor";
    }

    Status MUST_USE_RESULT prepare() override;

    void execute() override;

private:
    AlterTagSentence                           *sentence_{nullptr};

    nebula::meta::cpp2::AlterSchemaOp
    getTagOpType(const AlterTagOptItem::OptionType type);
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_ALTERTAGEXECUTOR_H_
