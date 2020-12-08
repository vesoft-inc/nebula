/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/CommonUtils.h"
#include "common/time/WallClock.h"

namespace nebula {
namespace storage {

cpp2::ErrorCode CommonUtils::to(const Status& status) {
    switch (status.code()) {
        case Status::kOk:
            return cpp2::ErrorCode::SUCCEEDED;
        case Status::kSpaceNotFound:
            return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
        case Status::kPartNotFound:
            return cpp2::ErrorCode::E_PART_NOT_FOUND;
        default:
            return cpp2::ErrorCode::E_UNKNOWN;
    }
}


cpp2::ErrorCode CommonUtils::to(kvstore::ResultCode rc) {
    switch (rc) {
        case kvstore::ResultCode::SUCCEEDED :
            return cpp2::ErrorCode::SUCCEEDED;
        case kvstore::ResultCode::ERR_LEADER_CHANGED :
            return cpp2::ErrorCode::E_LEADER_CHANGED;
        default:
            LOG(ERROR) << "unknown ResultCode: " << static_cast<int>(rc);
            return cpp2::ErrorCode::E_UNKNOWN;
    }
}

kvstore::ResultCode CommonUtils::to(cpp2::ErrorCode code) {
    switch (code) {
        case cpp2::ErrorCode::SUCCEEDED:
            return kvstore::ResultCode::SUCCEEDED;
        case cpp2::ErrorCode::E_LEADER_CHANGED:
            return kvstore::ResultCode::ERR_LEADER_CHANGED;
        default:
            LOG(ERROR) << "unknown ErrorCode: " << static_cast<int>(code);
            return kvstore::ResultCode::ERR_UNKNOWN;
    }
}


bool CommonUtils::checkDataExpiredForTTL(const meta::SchemaProviderIf* schema,
                                         RowReader* reader,
                                         const std::string& ttlCol,
                                         int64_t ttlDuration) {
    const auto& ftype = schema->getFieldType(ttlCol);
    // could not support VID type anymore
    // todo: could support DateTime later
    if (ftype != meta::cpp2::PropertyType::TIMESTAMP && ftype != meta::cpp2::PropertyType::INT64) {
        return false;
    }
    auto now = time::WallClock::fastNowInSec();
    auto v = reader->getValueByName(ttlCol);
    if (now > (v.getInt() + ttlDuration)) {
        VLOG(2) << "ttl expired";
        return true;
    }
    return false;
}

}  // namespace storage
}  // namespace nebula
