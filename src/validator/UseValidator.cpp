/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/UseValidator.h"
#include "parser/TraverseSentences.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
Status UseValidator::validateImpl() {
    auto useSentence = static_cast<UseSentence*>(sentence_);
    auto* spaceName = useSentence->space();
    auto ret = qctx_->schemaMng()->toGraphSpaceID(*spaceName);
    if (!ret.ok()) {
        LOG(ERROR) << "Unkown space: " << *spaceName;
        return ret.status();
    }

    vctx_->switchToSpace(*spaceName, ret.value());
    return Status::OK();
}

Status UseValidator::toPlan() {
    auto space = vctx_->whichSpace();
    // The input will be set by father validator later.
    auto plan = qctx_->plan();
    auto *start = StartNode::make(plan);
    auto reg = SwitchSpace::make(plan, start, space.name, space.id);
    root_ = reg;
    tail_ = root_;
    return Status::OK();
}
}  // namespace graph
}  // namespace nebula
