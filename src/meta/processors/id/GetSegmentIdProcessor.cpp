/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/id/GetSegmentIdProcessor.h"

namespace nebula {
namespace meta {

void GetSegmentIdProcessor::process(const cpp2::GetSegmentIdReq& req) {
  auto stepResult = doGet(stepKey);
  auto curIdResult = doGet(idKey);
  if (!nebula::ok(stepResult) || !nebula::ok(curIdResult)) {
    LOG(ERROR) << "Get step or current id failed during get segment id";
  }

  std::lock_guard<std::mutex> lck(lock_);

  string step = std::move(nebula::value(stepResult));
  string curId = std::move(nebula::value(curIdResult));
  int64_t stepInt = std::stoi(step);
  int64_t curIdInt = std::stoi(curId);
  doPut(std::vector<kvstore::KV>{{idKey, std::to_string(curIdInt + 1)}});

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.set_start_id(curIdInt);
  resp_.set_length(stepInt);
}

}  // namespace meta
}  // namespace nebula
