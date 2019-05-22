/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/DescribeEdgeIndexExecutor.h"

namespace nebula {
namespace graph {

DescribeEdgeIndexExecutor::DescribeEdgeIndexExecutor(Sentence *sentence,
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<DescribeEdgeIndexSentence*>(sentence);
}

Status DescribeEdgeIndexExecutor::prepare() {
    return checkIfGraphSpaceChosen();
}


void DescribeEdgeIndexExecutor::execute() {
}


void DescribeEdgeIndexExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula

