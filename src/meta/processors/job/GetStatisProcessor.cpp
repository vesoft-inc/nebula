/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/GetStatisProcessor.h"

namespace nebula {
namespace meta {

void GetStatisProcessor::process(const cpp2::GetStatisReq& req) {
    auto spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);

    auto statisKey = MetaServiceUtils::statisKey(spaceId);
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, statisKey, &val);

    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "SpaceId " << spaceId << " no statis data, "
                   << "please submit job statis under space.";
        handleErrorCode(ret);
        onFinished();
        return;
    }
    auto statisItem = MetaServiceUtils::parseStatisVal(val);
    auto statisJobStatus = statisItem.get_status();
    if (statisJobStatus != cpp2::JobStatus::FINISHED) {
        LOG(ERROR) << "SpaceId " << spaceId << " statis job is running or failed, "
                   << "please show jobs.";
        handleErrorCode(nebula::cpp2::ErrorCode::E_JOB_NOT_FINISHED);
        onFinished();
        return;
    }

    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.set_statis(std::move(statisItem));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

