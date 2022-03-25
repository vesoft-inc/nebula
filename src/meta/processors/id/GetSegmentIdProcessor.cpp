/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/id/GetSegmentIdProcessor.h"

#include "common/base/Logging.h"

namespace nebula {
namespace meta {

void GetSegmentIdProcessor::process(MAYBE_UNUSED const cpp2::GetSegmentIdReq& req) {
  int64_t length = req.get_length();
  folly::SharedMutex::WriteHolder wHolder(LockUtils::idLock());

  auto curIdResult = doGet(kIdKey);
  if (!nebula::ok(curIdResult)) {
    LOG(ERROR) << "Get step or current id failed during get segment id";
    handleErrorCode(nebula::cpp2::ErrorCode::E_ID_FAILED);
    resp_.segment_id_ref() = -1;
    onFinished();
    return;
  }

  int64_t curId = std::stoi(std::move(nebula::value(curIdResult)));

  auto code = doSyncPut(std::vector<kvstore::KV>{{kIdKey, std::to_string(curId + length)}});

  handleErrorCode(code);
  resp_.segment_id_ref() = curId;
  onFinished();
  return;
}

}  // namespace meta
}  // namespace nebula
