/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/base/Base.h"

#include "planner/plan/Admin.h"
#include "validator/IngestValidator.h"
#include "parser/MutateSentences.h"

namespace nebula {
namespace graph {

Status IngestValidator::toPlan() {
    auto *doNode = Ingest::make(qctx_, nullptr);
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
