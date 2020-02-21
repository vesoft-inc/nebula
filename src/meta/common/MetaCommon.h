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
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METACOMMON_H_
