/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/UseValidator.h"
#include "parser/TraverseSentences.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {
Status UseValidator::validateImpl() {
    auto useSentence = static_cast<UseSentence*>(sentence_);
    spaceName_ = useSentence->space();
    SpaceInfo spaceInfo;
    spaceInfo.name = *spaceName_;
    // firstly get from validate context
    if (!vctx_->hasSpace(*spaceName_)) {
        // secondly get from cache
        auto spaceId = qctx_->schemaMng()->toGraphSpaceID(*spaceName_);
        if (!spaceId.ok()) {
            LOG(ERROR) << "Unknown space: " << *spaceName_;
            return spaceId.status();
        }
        auto spaceDesc = qctx_->getMetaClient()->getSpaceDesc(spaceId.value());
        if (!spaceDesc.ok()) {
            return spaceDesc.status();
        }
        spaceInfo.id = spaceId.value();
        spaceInfo.spaceDesc = std::move(spaceDesc).value();
        vctx_->switchToSpace(std::move(spaceInfo));
        return Status::OK();
    }

    spaceInfo.id = -1;
    vctx_->switchToSpace(std::move(spaceInfo));
    return Status::OK();
}

Status UseValidator::toPlan() {
    auto reg = SwitchSpace::make(qctx_, nullptr, *spaceName_);
    root_ = reg;
    tail_ = root_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
