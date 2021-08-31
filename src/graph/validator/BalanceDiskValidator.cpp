/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/validator/BalanceDiskValidator.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

Status BalanceDiskValidator::toPlan() {
  PlanNode *current = nullptr;
  BalanceDiskSentence *sentence = static_cast<BalanceDiskSentence *>(sentence_);
  switch (sentence->subType()) {
    case BalanceDiskSentence::SubType::kDiskAttach:
      current =
          BalanceDiskAttach::make(qctx_, nullptr, *sentence->host(), sentence->paths()->paths());
      break;
    case BalanceDiskSentence::SubType::kDiskRemove:
      current =
          BalanceDiskRemove::make(qctx_, nullptr, *sentence->host(), sentence->paths()->paths());
      break;
    default:
      DLOG(FATAL) << "Unknown balance disk kind " << sentence->kind();
      return Status::NotSupported("Unknown balance disk kind %d",
                                  static_cast<int>(sentence->kind()));
  }
  root_ = current;
  tail_ = root_;
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
