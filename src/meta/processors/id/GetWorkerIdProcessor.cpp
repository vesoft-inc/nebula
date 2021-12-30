/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/id/GetWorkerIdProcessor.h"

namespace nebula {
namespace meta {

void GetWorkerIdProcessor::process(const cpp2::GetWorkerIdReq& req) {
  const string& ipAddr = req.get_host();
  auto result = doGet(ipAddr);
  if (nebula::ok(result)) {
    int64_t workerId = std::stoi(std::move(nebula::value(result)));

    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.workerid_ref() = workerId;
    onFinished();
    return;
  }

  folly::SharedMutex::WriteHolder wHolder(LockUtils::workerIdLock());
  auto newResult = doGet(idKey);
  if (!nebula::ok(newResult)) {
    handleErrorCode(nebula::cpp2::ErrorCode::E_WORKER_ID_FAILED);
    onFinished();
    return;
  }

  string workerIdStr = std::move(nebula::value(newResult));
  int64_t workerIdInt32 = std::stoi(workerIdStr);

  doPut(std::vector<kvstore::KV>{{ipAddr, std::to_string(workerIdInt32 + 1)}});

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.workerid_ref() = workerIdInt32;
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
