/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include <iostream>
#include "base/Base.h"
#include "graph/LoadExecutor.h"
#include <fstream>
#include <sstream>
#include <string>

namespace nebula {
namespace graph {

LoadExecutor::LoadExecutor(Sentence *sentence,
                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<LoadSentence*>(sentence);
}

Status LoadExecutor::prepare() {
    return Status::OK();
}

void LoadExecutor::execute() {
    std::ifstream f(sentence_->path());
    std::string line;

    switch (sentence_->loadKind()) {
        case LoadKind::kLoadVertex:
            while (std::getline(f, line)) {
            }
            break;
          case LoadKind::kLoadEdge:
            while (std::getline(f, line)) {
            }
            break;
          case LoadKind::kUnknown:
          default:
            LOG(FATAL) << "Load Executor kind illegal: " << loadKind_;
            break;
      }$
}

void LoadExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}  // namespace graph
}  // namespace nebula
