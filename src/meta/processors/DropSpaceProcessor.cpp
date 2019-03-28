/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/DropSpaceProcessor.h"

namespace nebula {
namespace meta {

void DropSpaceProcessor::process(const cpp2::DropSpaceReq& req) {
    guard_ = std::make_unique<std::lock_guard<std::mutex>>(
                                    BaseProcessor<cpp2::ExecResp>::lock_);
    auto spaceRet = spaceExist(req.get_space_name());

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

    auto partPrefix = MetaUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, partPrefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(to(ret));
        onFinished();
        return;
    }

    while (iter->valid()) {
        auto key = iter->key();
        resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        kvstore_->asyncRemove(kDefaultSpaceId_, kDefaultPartId_, key.toString(),
                             [this] (kvstore::ResultCode code, HostAddr leader) {
            UNUSED(leader);
            if (code != kvstore::ResultCode::SUCCEEDED) {
                this->resp_.set_code(to(code));
                this->onFinished();
                return;
            }
        });

        if (resp_.get_code() != cpp2::ErrorCode::SUCCEEDED) {
            return;
        }
        iter->next();
    }

    auto spaceKey = MetaUtils::spaceKey(spaceId);
    kvstore_->asyncRemove(kDefaultSpaceId_, kDefaultPartId_, spaceKey,
                         [this] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(leader);
        this->resp_.set_code(to(code));
        this->onFinished();
    });

    // TODO(YT) delete part files of the space
}

}  // namespace meta
}  // namespace nebula
