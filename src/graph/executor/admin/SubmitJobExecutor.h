/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_SUBMIT_JOB_EXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SUBMIT_JOB_EXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class SubmitJobExecutor final : public Executor {
 public:
  SubmitJobExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("SubmitJobExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  FRIEND_TEST(JobTest, JobFinishTime);
  StatusOr<DataSet> buildResult(meta::cpp2::AdminJobOp jobOp, meta::cpp2::AdminJobResult &&resp);
  Value convertJobTimestampToDateTime(int64_t timestamp);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SUBMIT_JOB_EXECUTOR_H_
