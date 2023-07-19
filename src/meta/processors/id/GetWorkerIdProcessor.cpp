/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/id/GetWorkerIdProcessor.h"

namespace nebula {
namespace meta {

void GetWorkerIdProcessor::process(const cpp2::GetWorkerIdReq& req) {
  const std::string& ipAddr = req.get_host();
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto result = doGet(ipAddr);
  if (nebula::ok(result)) {
    int64_t workerId = std::stoi(std::move(nebula::value(result)));

    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.workerid_ref() = workerId;
    onFinished();
    return;
  }

  auto newResult = doGet(kIdKey);
  if (!nebula::ok(newResult)) {
    LOG(ERROR) << "Get worker kIdKey failed during get worker id";
    handleErrorCode(nebula::cpp2::ErrorCode::E_ID_FAILED);
    onFinished();
    return;
  }

  int64_t workerId = std::stoi(std::move(nebula::value(newResult)));
  // TODO: (jackwener) limit worker, add LOG ERROR
  auto code = doSyncPut(std::vector<kvstore::KV>{{ipAddr, std::to_string(workerId + 1)}});
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Put worker ipAddr failed during get worker id";
    handleErrorCode(nebula::cpp2::ErrorCode::E_ID_FAILED);
    onFinished();
    return;
  }
  code = doSyncPut(std::vector<kvstore::KV>{{kIdKey, std::to_string(workerId + 1)}});
  handleErrorCode(code);
  resp_.workerid_ref() = workerId;
  onFinished();
  return;
}
}  // namespace meta
}  // namespace nebula
