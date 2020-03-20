/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METACOMMON_H_
#define META_METACOMMON_H_

#include "base/Base.h"
#include "base/Status.h"
#include "kvstore/KVStore.h"
#include "meta/processors/Common.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

class MetaCommon final {
public:
    MetaCommon() = delete;

    static bool checkSegment(const std::string& segment) {
        static const std::regex pattern("^[0-9a-zA-Z]+$");
        if (!segment.empty() && std::regex_match(segment, pattern)) {
            return true;
        }
        return false;
    }

    static cpp2::ErrorCode to(kvstore::ResultCode code) {
        switch (code) {
        case kvstore::ResultCode::SUCCEEDED:
            return cpp2::ErrorCode::SUCCEEDED;
        case kvstore::ResultCode::ERR_KEY_NOT_FOUND:
            return cpp2::ErrorCode::E_NOT_FOUND;
        case kvstore::ResultCode::ERR_LEADER_CHANGED:
            return cpp2::ErrorCode::E_LEADER_CHANGED;
        case kvstore::ResultCode::ERR_CHECKPOINT_ERROR:
            return cpp2::ErrorCode::E_SNAPSHOT_FAILURE;
        default:
            return cpp2::ErrorCode::E_UNKNOWN;
        }
    }

    static cpp2::ErrorCode to(const Status& status) {
        switch (status.code()) {
        case Status::kOk:
            return cpp2::ErrorCode::SUCCEEDED;
        case Status::kSpaceNotFound:
        case Status::kHostNotFound:
        case Status::kTagNotFound:
        case Status::kUserNotFound:
            return cpp2::ErrorCode::E_NOT_FOUND;
        default:
            return cpp2::ErrorCode::E_UNKNOWN;
        }
    }

    static bool saveRebuildStatus(kvstore::KVStore* kvstore,
                                  std::string statusKey,
                                  std::string&& statusValue) {
        std::vector<kvstore::KV> status{std::make_pair(std::move(statusKey),
                                                       std::forward<std::string>(statusValue))};
        folly::Baton<true, std::atomic> baton;
        auto ret = kvstore::ResultCode::SUCCEEDED;
        kvstore->asyncMultiPut(kDefaultSpaceId,
                               kDefaultPartId,
                               std::move(status),
                               [&ret, &baton] (kvstore::ResultCode code) {
                                   if (kvstore::ResultCode::SUCCEEDED != code) {
                                       ret = code;
                                       LOG(INFO) << "Put data error on meta server";
                                   }
                                   baton.post();
                               });
        baton.wait();
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Save Status Failed";
            return false;
        }
        return true;
    }
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METACOMMON_H_
