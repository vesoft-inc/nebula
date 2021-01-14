/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "common/base/Base.h"

#include "planner/Admin.h"
#include "validator/DownloadValidator.h"
#include "parser/MutateSentences.h"

namespace nebula {
namespace graph {

Status DownloadValidator::toPlan() {
    auto sentence = static_cast<DownloadSentence*>(sentence_);
    auto *doNode = Download::make(qctx_,
                                  nullptr,
                                  *sentence->host(),
                                  sentence->port(),
                                  *sentence->path());
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
