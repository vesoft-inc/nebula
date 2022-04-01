// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

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
  StatusOr<DataSet> buildResult(meta::cpp2::JobOp jobOp, meta::cpp2::AdminJobResult &&resp);
  Value convertJobTimestampToDateTime(int64_t timestamp);
  nebula::DataSet buildShowResultData(const nebula::meta::cpp2::JobDesc &jd,
                                      const std::vector<nebula::meta::cpp2::TaskDesc> &td);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SUBMIT_JOB_EXECUTOR_H_
