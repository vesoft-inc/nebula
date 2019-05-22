/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DescribeTagIndexExecutor.h"

namespace nebula {
namespace graph {

DescribeTagIndexExecutor::DescribeTagIndexExecutor(Sentence *sentence,
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DescribeTagIndexSentence*>(sentence);
}

Status DescribeTagIndexExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}


void DescribeTagIndexExecutor::execute() {
}


void DescribeTagIndexExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}


}   // namespace graph
}   // namespace nebula

