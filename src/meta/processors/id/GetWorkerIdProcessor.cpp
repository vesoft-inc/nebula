/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 */

#include "meta/processors/id/GetWorkerIdProcessor.h"

namespace nebula {
namespace meta {

void GetWorkerIdProcessor::process(const cpp2::GetWorkerIdReq& req) {
  LOG(INFO) << "workerId GetWorkerIdProcessor::process";
  const string& ipAddr = req.get_host();
  auto result = doGet(ipAddr);
  LOG(INFO) << "workerId doGet ipAddr: " << ipAddr;
  if (nebula::ok(result)) {
    LOG(INFO) << "workerId Get directly";
    string workerIdStr = std::move(nebula::value(result));
    int32_t workerIdInt32 = std::stoi(workerIdStr);

    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.set_workerid(std::move(workerIdInt32));
    onFinished();
    return;
  }

  LOG(INFO) << "workerId Get and set";
  folly::SharedMutex::WriteHolder wHolder(LockUtils::workerIdLock());
  auto newResult = doGet(idKey);
  if (!nebula::ok(newResult)) {
    LOG(ERROR) << "Get idKey worker id failed";
    // TODO handleErrorCode(nebula::cpp2::ErrorCode::E_GET_WORKER_ID_FAILED);
    onFinished();
    return;
  }

  string workerIdStr = std::move(nebula::value(newResult));
  int32_t workerIdInt32 = std::stoi(workerIdStr);
  LOG(INFO) << "workerId Get and set workerIdInt32: " << workerIdInt32;

  int32_t newWorkerId = workerIdInt32 + 1;
  doPut(std::vector<kvstore::KV>{{ipAddr, std::to_string(newWorkerId)}});

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.set_workerid(std::move(workerIdInt32));
  onFinished();
}

void GetWorkerIdProcessor::doPut(std::vector<kvstore::KV> data) {
  folly::Baton<true, std::atomic> baton;
  kvstore_->asyncMultiPut(kDefaultSpaceId,
                          kDefaultPartId,
                          std::move(data),
                          [this, &baton](nebula::cpp2::ErrorCode code) {
                            this->handleErrorCode(code);
                            baton.post();
                          });
  baton.wait();
}

}  // namespace meta
}  // namespace nebula
