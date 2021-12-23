/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/id/GetSegmentIdProcessor.h"

namespace nebula {
namespace meta {

void GetSegmentIdProcessor::process(MAYBE_UNUSED const cpp2::GetSegmentIdReq& req) {
  int64_t length = req.get_length();

  folly::SharedMutex::WriteHolder wHolder(LockUtils::segmentIdLock());

  auto curIdResult = doGet(idKey);
  if (!nebula::ok(curIdResult)) {
    LOG(ERROR) << "Get step or current id failed during get segment id";
  }

  int64_t curIdInt = std::stoi(std::move(nebula::value(curIdResult)));

  doPut(std::vector<kvstore::KV>{{idKey, std::to_string(curIdInt + length)}});

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.set_segment_id(curIdInt);
}

}  // namespace meta
}  // namespace nebula
