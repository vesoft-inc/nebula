/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/job/GetStatsProcessor.h"

namespace nebula {
namespace meta {

void GetStatsProcessor::process(const cpp2::GetStatsReq& req) {
  auto spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);

  auto statsKey = MetaServiceUtils::statsKey(spaceId);
  std::string val;
  auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, statsKey, &val);

  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "SpaceId " << spaceId << " no statis data, please submit job statis under space.";
    handleErrorCode(ret);
    onFinished();
    return;
  }
  auto statsItem = MetaServiceUtils::parseStatsVal(val);
  auto statisJobStatus = statsItem.get_status();
  if (statisJobStatus != cpp2::JobStatus::FINISHED) {
    LOG(ERROR) << "SpaceId " << spaceId << " statis job is running or failed, please show jobs.";
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
