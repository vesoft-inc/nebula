/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DropTagIndexExecutor.h"

namespace nebula {
namespace graph {

DropTagIndexExecutor::DropTagIndexExecutor(Sentence *sentence,
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DropTagIndexSentence*>(sentence);
}

Status DropTagIndexExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}

void DropTagIndexExecutor::execute() {
}

}   // namespace graph
}   // namespace nebula

