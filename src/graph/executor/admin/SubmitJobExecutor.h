/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_SUBMIT_JOB_EXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SUBMIT_JOB_EXECUTOR_H_

#include <folly/futures/Future.h>  // for Future
#include <gtest/gtest_prod.h>      // for FRIEND_TEST
#include <stdint.h>                // for int64_t

#include <vector>  // for allocator, vector

#include "common/base/StatusOr.h"           // for StatusOr
#include "common/datatypes/DataSet.h"       // for DataSet
#include "common/datatypes/Value.h"         // for Value
#include "graph/executor/Executor.h"        // for Executor
#include "interface/gen-cpp2/meta_types.h"  // for AdminJobOp, AdminJobResul...

namespace nebula {
class Status;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph

class Status;

namespace graph {
class PlanNode;
class QueryContext;

class SubmitJobExecutor final : public Executor {
 public:
  SubmitJobExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("SubmitJobExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  FRIEND_TEST(JobTest, JobFinishTime);
  StatusOr<DataSet> buildResult(meta::cpp2::AdminJobOp jobOp, meta::cpp2::AdminJobResult &&resp);
  Value convertJobTimestampToDateTime(int64_t timestamp);
  nebula::DataSet buildShowResultData(const nebula::meta::cpp2::JobDesc &jd,
                                      const std::vector<nebula::meta::cpp2::TaskDesc> &td);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SUBMIT_JOB_EXECUTOR_H_
