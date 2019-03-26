/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/DropSpaceProcessor.h"

namespace nebula {
namespace meta {

void DropSpaceProcessor::process(const cpp2::DropSpaceReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    auto spaceRet = getSpaceId(req.get_space_name());

    if (!spaceRet.ok()) {
        if (spaceRet.status() == Status::SpaceNotFound()) {
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        } else {
            resp_.set_code(cpp2::ErrorCode::E_UNKNOWN);
        }
        onFinished();
        return;;
    }

    auto spaceId = spaceRet.value();
    VLOG(3) << "Drop space " << req.get_space_name() << ", id " << spaceId;

    std::string start = MetaUtils::partKey(spaceId, 0x00000000);
    std::string end = MetaUtils::partKey(spaceId, 0x7FFFFFFF);
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);

    kvstore_->asyncRemoveRange(kDefaultSpaceId_, kDefaultPartId_, start, end,
                               [this] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(leader);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            this->resp_.set_code(to(code));
            return;
        }
    });

    if (resp_.get_code() != cpp2::ErrorCode::SUCCEEDED) {
        this->onFinished();
        return;
    }

    auto spaceIndexKey = MetaUtils::indexKey(EntryType::SPACE, req.get_space_name());
    kvstore_->asyncRemove(kDefaultSpaceId_, kDefaultPartId_, spaceIndexKey,
                          [this] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(leader);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            this->resp_.set_code(to(code));
            return;
        }
    });

    if (resp_.get_code() != cpp2::ErrorCode::SUCCEEDED) {
        this->onFinished();
        return;
    }

    auto spaceKey = MetaUtils::spaceKey(spaceId);
    kvstore_->asyncRemove(kDefaultSpaceId_, kDefaultPartId_, spaceKey,
                          [this] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(leader);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            this->resp_.set_code(to(code));
            return;
        }
    });

    onFinished();
    // TODO(YT) delete part files of the space
}

}  // namespace meta
}  // namespace nebula
