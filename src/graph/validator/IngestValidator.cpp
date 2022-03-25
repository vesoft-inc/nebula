/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/IngestValidator.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

// Plan to ingest SST file in server side
Status IngestValidator::toPlan() {
  auto *doNode = Ingest::make(qctx_, nullptr);
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
