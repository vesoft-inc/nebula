/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/GetStatsProcessor.h"

namespace nebula {
namespace meta {

void GetStatsProcessor::process(const cpp2::GetStatsReq& req) {
  auto spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);

  auto statsKey = MetaKeyUtils::statsKey(spaceId);
  std::string val;
  auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, statsKey, &val);

  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      ret = nebula::cpp2::ErrorCode::E_STATS_NOT_FOUND;
      LOG(ERROR) << "SpaceId " << spaceId
                 << " no stats info, please execute `submit job stats' under space firstly.";
    } else {
      LOG(ERROR) << "Show stats failed, error " << apache::thrift::util::enumNameSafe(ret);
    }

    handleErrorCode(ret);
    onFinished();
    return;
  }
  auto statsItem = MetaKeyUtils::parseStatsVal(val);
  auto statsJobStatus = statsItem.get_status();
  if (statsJobStatus != cpp2::JobStatus::FINISHED) {
    LOG(ERROR) << "SpaceId " << spaceId
               << " stats job is running or failed, please execute `show jobs' firstly.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_JOB_NOT_FINISHED);
    onFinished();
    return;
  }

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.set_stats(std::move(statsItem));
  onFinished();
}

}  // namespace meta
}  // namespace nebula
