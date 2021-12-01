/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/id/GetWorkerIdProcessor.h"

namespace nebula {
namespace meta {

void GetWorkerIdProcessor::process(const cpp2::GetWorkerIdReq& req) {
  const string& mac_address = req.get_mac_address();
  auto result = doGet(mac_address);
  if (nebula::ok(result)) {
    string worker_id_str = std::move(nebula::value(result));
    int32_t worker_id_int32 = std::stoi(worker_id_str);

    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.set_workerid(std::move(worker_id_int32));
    return;
  }

  auto new_result = doGet(id_key);
  if (!nebula::ok(new_result)) {
    LOG(ERROR) << "Get id_key worker id failed";
    handleErrorCode(nebula::cpp2::ErrorCode::E_GET_WORKER_ID_FAILED);
    return;
  }

  // 加锁
  string worker_id_str = std::move(nebula::value(new_result));
  int32_t worker_id_int32 = std::stoi(worker_id_str);

  int32_t new_worker_id = worker_id_int32 + 1;
  doPut(std::vector<kvstore::KV>{{id_key, std::to_string(new_worker_id)}});

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.set_workerid(std::move(worker_id_int32));
}

}  // namespace meta
}  // namespace nebula
