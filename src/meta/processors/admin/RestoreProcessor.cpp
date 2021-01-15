/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/RestoreProcessor.h"
#include "common/fs/FileUtils.h"

namespace nebula {
namespace meta {

void RestoreProcessor::process(const cpp2::RestoreMetaReq& req) {
    auto files = req.get_files();
    if (files.empty()) {
        LOG(ERROR) << "restore must contain the sst file.";
        handleErrorCode(cpp2::ErrorCode::E_RESTORE_FAILURE);
        onFinished();
        return;
    }

    auto ret = kvstore_->restoreFromFiles(kDefaultSpaceId, files);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Failed to restore file";
        handleErrorCode(cpp2::ErrorCode::E_RESTORE_FAILURE);
        onFinished();
        return;
    }

    auto replaceHosts = req.get_hosts();
    if (!replaceHosts.empty()) {
        for (auto h : replaceHosts) {
            auto result = MetaServiceUtils::replaceHostInPartition(
                kvstore_, h.get_from_host(), h.get_to_host(), true);
            if (!result) {
                LOG(ERROR) << "replaceHost in partition fails when recovered";
                handleErrorCode(cpp2::ErrorCode::E_RESTORE_FAILURE);
                onFinished();
                return;
            }

            result = MetaServiceUtils::replaceHostInZone(
                kvstore_, h.get_from_host(), h.get_to_host(), true);
            if (!result) {
                LOG(ERROR) << "replacehost in zone fails when recovered";
                handleErrorCode(cpp2::ErrorCode::E_RESTORE_FAILURE);
                onFinished();
                return;
            }
        }
    }

    for (auto &f : files) {
        unlink(f.c_str());
    }

    onFinished();
}
}   // namespace meta
}   // namespace nebula
