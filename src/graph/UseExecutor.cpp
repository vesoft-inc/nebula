/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/UseExecutor.h"
#include "meta/SchemaManager.h"

namespace nebula {
namespace graph {

UseExecutor::UseExecutor(Sentence *sentence, ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<UseSentence*>(sentence);
}


Status UseExecutor::prepare() {
    return Status::OK();
}


void UseExecutor::execute() {
    auto *session = ectx()->rctx()->session();

    auto spaceName = *sentence_->space();
    // Check from the cache, if space not exists, schemas also not exist
    auto status = ectx()->schemaManager()->checkSpaceExist(spaceName);
    if (!status.ok()) {
        DCHECK(onError_);
        onError_(Status::Error("Space not found for `%s'", spaceName.c_str()));
        return;
    }

    auto spaceId = ectx()->schemaManager()->toGraphSpaceID(spaceName);
    session->setSpace(*sentence_->space(), spaceId);
    FLOG_INFO("Graph space switched to `%s', space id: %d", sentence_->space()->c_str(), spaceId);

    onFinish_();
}

}   // namespace graph
}   // namespace nebula
