/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/TraverseExecutor.h"
#include "parser/TraverseSentences.h"
#include "graph/GoExecutor.h"
#include "graph/PipeExecutor.h"

namespace nebula {
namespace graph {

std::unique_ptr<TraverseExecutor> TraverseExecutor::makeTraverseExecutor(Sentence *sentence) {
    return makeTraverseExecutor(sentence, ectx());
}


// static
std::unique_ptr<TraverseExecutor>
TraverseExecutor::makeTraverseExecutor(Sentence *sentence, ExecutionContext *ectx) {
    auto kind = sentence->kind();
    std::unique_ptr<TraverseExecutor> executor;
    switch (kind) {
        case Sentence::Kind::kGo:
            executor = std::make_unique<GoExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kPipe:
            executor = std::make_unique<PipeExecutor>(sentence, ectx);
            break;
        case Sentence::Kind::kUnknown:
            LOG(FATAL) << "Sentence kind unknown";
            break;
        default:
            LOG(FATAL) << "Sentence kind illegal: " << kind;
            break;
    }
    return executor;
}

}   // namespace graph
}   // namespace nebula
