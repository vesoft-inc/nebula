/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>               // for TestPartResult
#include <gtest/gtest.h>               // for Message
#include <gtest/gtest.h>               // for TestPartResult
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref, optio...

#include <memory>       // for make_unique, uni...
#include <string>       // for string, basic_st...
#include <type_traits>  // for remove_reference...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/StatusOr.h"                    // for StatusOr
#include "common/datatypes/DataSet.h"                // for DataSet
#include "common/datatypes/List.h"                   // for List
#include "common/datatypes/Value.h"                  // for Value, Value::kE...
#include "common/time/TimeConversion.h"              // for TimeConversion
#include "graph/context/QueryContext.h"              // for QueryContext
#include "graph/executor/admin/SubmitJobExecutor.h"  // for SubmitJobExecutor
#include "graph/planner/plan/Admin.h"                // for SubmitJob
#include "interface/gen-cpp2/meta_types.h"           // for JobDesc, AdminJo...

namespace nebula {
namespace graph {
class JobTest : public testing::Test {};

TEST_F(JobTest, JobFinishTime) {
  {
    meta::cpp2::AdminJobResult resp;
    resp.job_id_ref() = 0;
    meta::cpp2::JobDesc jobDesc;
    jobDesc.id_ref() = 0;
    jobDesc.start_time_ref() = 123;
    jobDesc.stop_time_ref() = 0;
    resp.job_desc_ref() = {std::move(jobDesc)};
    meta::cpp2::TaskDesc taskDesc;
    taskDesc.start_time_ref() = 456;
    taskDesc.stop_time_ref() = 0;
    resp.task_desc_ref() = {std::move(taskDesc)};

    auto qctx = std::make_unique<QueryContext>();
    auto submitJob = SubmitJob::make(
        qctx.get(), nullptr, meta::cpp2::AdminJobOp::SHOW, meta::cpp2::AdminCmd::UNKNOWN, {});
    auto submitJobExe = std::make_unique<SubmitJobExecutor>(submitJob, qctx.get());

    auto status = submitJobExe->buildResult(meta::cpp2::AdminJobOp::SHOW, std::move(resp));
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
    jobDesc.id_ref() = 0;
    jobDesc.start_time_ref() = 123;
    jobDesc.stop_time_ref() = 0;
    resp.job_desc_ref() = {std::move(jobDesc)};

    auto qctx = std::make_unique<QueryContext>();
    auto submitJob = SubmitJob::make(
        qctx.get(), nullptr, meta::cpp2::AdminJobOp::SHOW_All, meta::cpp2::AdminCmd::UNKNOWN, {});
    auto submitJobExe = std::make_unique<SubmitJobExecutor>(submitJob, qctx.get());

    auto status = submitJobExe->buildResult(meta::cpp2::AdminJobOp::SHOW_All, std::move(resp));
    EXPECT_TRUE(status.ok());
    auto result = std::move(status).value();
    EXPECT_EQ(result.rows.size(), 1);
    EXPECT_EQ(result.rows[0][3], Value(time::TimeConversion::unixSecondsToDateTime(123)));
    EXPECT_EQ(result.rows[0][4], Value::kEmpty);
  }
}
}  // namespace graph
}  // namespace nebula
