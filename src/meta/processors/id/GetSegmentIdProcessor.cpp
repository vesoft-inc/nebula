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
  folly::SharedMutex::WriteHolder wHolder(LockUtils::segmentIdLock());

  auto curIdResult = doGet(kIdKey);
  if (!nebula::ok(curIdResult)) {
    LOG(ERROR) << "Get step or current id failed during get segment id";
    resp_.segment_id_ref() = -1;
    onFinished();
  }

  int64_t curId = std::stoi(std::move(nebula::value(curIdResult)));

  doPut(std::vector<kvstore::KV>{{kIdKey, std::to_string(curId + length)}});

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.segment_id_ref() = curId;
  onFinished();
}

void GetSegmentIdProcessor::doPut(std::vector<kvstore::KV> data) {
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
