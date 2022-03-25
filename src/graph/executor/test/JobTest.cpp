/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/time/TimeUtils.h"
#include "graph/context/QueryContext.h"
#include "graph/executor/admin/SubmitJobExecutor.h"
#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {
class JobTest : public testing::Test {};

TEST_F(JobTest, JobFinishTime) {
  {
    meta::cpp2::AdminJobResult resp;
    resp.job_id_ref() = 0;
    meta::cpp2::JobDesc jobDesc;
    jobDesc.job_id_ref() = 0;
    jobDesc.start_time_ref() = 123;
    jobDesc.stop_time_ref() = 0;
    resp.job_desc_ref() = {std::move(jobDesc)};
    meta::cpp2::TaskDesc taskDesc;
    taskDesc.start_time_ref() = 456;
    taskDesc.stop_time_ref() = 0;
    resp.task_desc_ref() = {std::move(taskDesc)};

    auto qctx = std::make_unique<QueryContext>();
    auto submitJob = SubmitJob::make(
        qctx.get(), nullptr, meta::cpp2::JobOp::SHOW, meta::cpp2::JobType::UNKNOWN, {});
    auto submitJobExe = std::make_unique<SubmitJobExecutor>(submitJob, qctx.get());

    auto status = submitJobExe->buildResult(meta::cpp2::JobOp::SHOW, std::move(resp));
    EXPECT_TRUE(status.ok());
    auto result = std::move(status).value();
    EXPECT_EQ(result.rows.size(), 2);
    EXPECT_EQ(result.rows[0][3], Value(time::TimeConversion::unixSecondsToDateTime(123)));
    EXPECT_EQ(result.rows[0][4], Value::kEmpty);
    EXPECT_EQ(result.rows[1][3], Value(time::TimeConversion::unixSecondsToDateTime(456)));
    EXPECT_EQ(result.rows[1][4], Value::kEmpty);
  }
  {
    meta::cpp2::AdminJobResult resp;
    resp.job_id_ref() = 0;
    meta::cpp2::JobDesc jobDesc;
    jobDesc.job_id_ref() = 0;
    jobDesc.start_time_ref() = 123;
    jobDesc.stop_time_ref() = 0;
    resp.job_desc_ref() = {std::move(jobDesc)};

    auto qctx = std::make_unique<QueryContext>();
    auto submitJob = SubmitJob::make(
        qctx.get(), nullptr, meta::cpp2::JobOp::SHOW_All, meta::cpp2::JobType::UNKNOWN, {});
    auto submitJobExe = std::make_unique<SubmitJobExecutor>(submitJob, qctx.get());

    auto status = submitJobExe->buildResult(meta::cpp2::JobOp::SHOW_All, std::move(resp));
    EXPECT_TRUE(status.ok());
    auto result = std::move(status).value();
    EXPECT_EQ(result.rows.size(), 1);
    EXPECT_EQ(result.rows[0][3], Value(time::TimeConversion::unixSecondsToDateTime(123)));
    EXPECT_EQ(result.rows[0][4], Value::kEmpty);
  }
}
}  // namespace graph
}  // namespace nebula
