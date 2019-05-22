/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DropEdgeIndexExecutor.h"

namespace nebula {
namespace graph {

DropEdgeIndexExecutor::DropEdgeIndexExecutor(Sentence *sentence,
                                             ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DropEdgeIndexSentence*>(sentence);
}

Status DropEdgeIndexExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}

void DropEdgeIndexExecutor::execute() {
}

}   // namespace graph
}   // namespace nebula

