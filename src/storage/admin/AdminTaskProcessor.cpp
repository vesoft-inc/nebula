/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "storage/admin/AdminTaskProcessor.h"
#include "storage/admin/AdminTaskManager.h"
#include "common/interface/gen-cpp2/common_types.h"

namespace nebula {
namespace storage {

void AdminTaskProcessor::process(const cpp2::AddAdminTaskRequest& req) {
    auto taskManager = AdminTaskManager::instance();

    auto cb = [env = env_,
               jobId = req.get_job_id(), taskId = req.get_task_id()](
                  nebula::cpp2::ErrorCode errCode,
                  nebula::meta::cpp2::StatisItem& result) {
        meta::cpp2::StatisItem* pStatis = nullptr;
        if (errCode == nebula::cpp2::ErrorCode::SUCCEEDED &&
            *result.status_ref() == nebula::meta::cpp2::JobStatus::FINISHED) {
            pStatis = &result;
        }

        LOG(INFO) << folly::sformat("reportTaskFinish(), job={}, task={}, rc={}",
                                    jobId,
                                    taskId,
                                    apache::thrift::util::enumNameSafe(errCode));
        auto maxRetry = 5;
        auto retry = 0;
        while (retry++ < maxRetry) {
            auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;
            auto fut = env->metaClient_->reportTaskFinish(jobId, taskId, errCode, pStatis);
            fut.wait();
            if (!fut.hasValue()) {
                LOG(INFO) << folly::sformat(
                    "reportTaskFinish() got rpc error:, job={}, task={}",
                    jobId,
                    taskId);
                continue;
            }
            if (!fut.value().ok()) {
                LOG(INFO) << folly::sformat(
                    "reportTaskFinish() has bad status:, job={}, task={}, rc={}",
                    jobId,
                    taskId,
                    fut.value().status().toString());
                break;
            }
            rc = fut.value().value();
            LOG(INFO) << folly::sformat("reportTaskFinish(), job={}, task={}, rc={}",
                                        jobId,
                                        taskId,
                                        apache::thrift::util::enumNameSafe(rc));
            if (rc == nebula::cpp2::ErrorCode::E_LEADER_CHANGED ||
                rc == nebula::cpp2::ErrorCode::E_STORE_FAILURE) {
                continue;
            } else {
                break;
            }
        }
    };

    TaskContext ctx(req, std::move(cb));
    auto task = AdminTaskFactory::createAdminTask(env_, std::move(ctx));
    if (task) {
        taskManager->addAsyncTask(task);
    } else {
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(nebula::cpp2::ErrorCode::E_INVALID_TASK_PARA);
        codes_.emplace_back(std::move(thriftRet));
    }
    onFinished();
}

void AdminTaskProcessor::onProcessFinished(nebula::meta::cpp2::StatisItem& result) {
    resp_.set_statis(std::move(result));
}

}  // namespace storage
}  // namespace nebula

