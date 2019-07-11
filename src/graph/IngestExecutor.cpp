/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/IngestExecutor.h"

namespace nebula {
namespace graph {

IngestExecutor::IngestExecutor(Sentence *sentence,
                               ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<IngestSentence*>(sentence);
}

Status IngestExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}

void IngestExecutor::execute() {
}

}   // namespace graph
}   // namespace nebula
