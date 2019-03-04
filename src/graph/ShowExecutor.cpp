/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/ShowExecutor.h"
#include "storage/StorageServiceHandler.h"


namespace nebula {
namespace graph {


ShowExecutor::ShowExecutor(Sentence *sentence,
                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<ShowSentence*>(sentence);
}


Status ShowExecutor::prepare() {
    return Status::OK();
}


void ShowExecutor::execute() {
    // TODO(YT) when StorageClient fininshed, then implement this interface
    auto show_kind = sentence_->showKind();
    switch (show_kind) {
        case (ShowKind::kShowHosts):
            DCHECK(onFinish_);
            onFinish_();
            break;
        default:
            LOG(FATAL) << "Show Sentence kind illegal: " <<show_kind;
            break;
    }
}


void ShowExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
