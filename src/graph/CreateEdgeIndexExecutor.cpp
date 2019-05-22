/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/CreateEdgeIndexExecutor.h"

namespace nebula {
namespace graph {

CreateEdgeIndexExecutor::CreateEdgeIndexExecutor(Sentence *sentence,
                                                 ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<CreateEdgeIndexSentence*>(sentence);
}

Status CreateEdgeIndexExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}

void CreateEdgeIndexExecutor::execute() {
}

}   // namespace graph
}   // namespace nebula

