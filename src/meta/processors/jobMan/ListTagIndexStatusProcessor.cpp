/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/ListTagIndexStatusProcessor.h"

namespace nebula {
namespace meta {

void ListTagIndexStatusProcessor::process(const cpp2::ListIndexStatusReq& req) {
    auto curSpaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(curSpaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto rc = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, JobUtil::jobPrefix(), &iter);
    if (rc != nebula::kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }

    if (!iter->valid()) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    decltype(resp_.statuses) statuses;
    for (; iter->valid(); iter->next()) {
        if (JobDescription::isJobKey(iter->key())) {
            auto jobDesc = JobDescription::makeJobDescription(iter->key(), iter->val());
            if (jobDesc != folly::none &&
                jobDesc->getCmd() == meta::cpp2::AdminCmd::REBUILD_TAG_INDEX) {
                auto paras = jobDesc->getParas();
                DCHECK_EQ(paras.size(), 2);
                auto indexName = paras[0];
                auto spaceName = paras[1];
                auto ret = getSpaceId(spaceName);
                if (!ret.ok()) {
                    handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
                    onFinished();
                    return;
                }
                auto spaceId = ret.value();
                if (spaceId != curSpaceId) {
                    continue;
                }
                cpp2::IndexStatus status;
                status.set_name(std::move(indexName));
                status.set_status(meta::cpp2::_JobStatus_VALUES_TO_NAMES.at(jobDesc->getStatus()));
                statuses.emplace_back(std::move(status));
            }
        }
    }
    resp_.set_statuses(std::move(statuses));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}   // namespace meta
}   // namespace nebula
