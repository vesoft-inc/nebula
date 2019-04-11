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
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);

    std::vector<folly::Future<kvstore::ResultCode>> removeResults;

    std::string start = MetaUtils::partKey(spaceId, 0x00000000);
    std::string end = MetaUtils::partKey(spaceId, 0x7FFFFFFF);
    auto spaceIndexKey = MetaUtils::indexKey(EntryType::SPACE, req.get_space_name());
    auto spaceKey = MetaUtils::spaceKey(spaceId);

    auto callFunc = [&removeResults] (kvstore::ResultCode code, HostAddr leader) {
         UNUSED(leader);
         folly::Promise<kvstore::ResultCode> pro;
         auto f = pro.getFuture();
         pro.setValue(code);
         removeResults.emplace_back(std::move(f));
         return;
    };

    kvstore_->asyncRemoveRange(kDefaultSpaceId_, kDefaultPartId_, start, end, callFunc);
    kvstore_->asyncRemove(kDefaultSpaceId_, kDefaultPartId_, spaceIndexKey, callFunc);
    kvstore_->asyncRemove(kDefaultSpaceId_, kDefaultPartId_, spaceKey, callFunc);

    while (removeResults.size() != 3)
        usleep(10);

    folly::collectAll(removeResults).thenTry([this] (auto&& t) {
        CHECK(!t.hasException()) << "Should not happen! msg:" << t.exception().what();
        CHECK_GT(t.value().size(), 0);
        for (const auto& entry : t.value()) {
            auto retVal = entry.value();
            if (retVal == kvstore::ResultCode::SUCCEEDED) {
                continue;
            } else {
                this->resp_.set_code(to(retVal));
                this->onFinished();
                return;
            }
        }
        this->resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        this->onFinished();
        return;
    });
    // TODO(YT) delete part files of the space
}

}  // namespace meta
}  // namespace nebula
