/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/CreateTagIndexExecutor.h"

namespace nebula {
namespace graph {

CreateTagIndexExecutor::CreateTagIndexExecutor(Sentence *sentence,
                                               ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<CreateTagIndexSentence*>(sentence);
}

Status CreateTagIndexExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}

void CreateTagIndexExecutor::execute() {
}

}   // namespace graph
}   // namespace nebula

