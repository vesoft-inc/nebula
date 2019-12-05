/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "GetTimezoneProcessor.h"

namespace nebula {
namespace meta {

void GetTimezoneProcessor::process(const cpp2::GetTimezoneReq&) {
    std::string timezoneKey = MetaServiceUtils::timezoneKey();
    StatusOr<std::string> ret;
    {
        folly::SharedMutex::ReadHolder rHolder(LockUtils::timezoneLock());
        ret = doGet(timezoneKey);
    }

    if (!ret.ok()) {
        LOG(INFO) << "Timezone is inexistence, create it";
        std::vector<kvstore::KV> data;
        cpp2::Timezone timezone;
        std::string timezoneValue = MetaServiceUtils::timezoneValue(timezone);
        data.emplace_back(std::move(timezoneKey), std::move(timezoneValue));

        folly::SharedMutex::WriteHolder wHolder(LockUtils::timezoneLock());
        doPut(std::move(data));
        return;
    }
    auto timezone = MetaServiceUtils::parseTimezone(ret.value());
    VLOG(3) << "Get Timezone: eastern " << timezone.get_eastern()
            << ", hour " << timezone.get_hour()
            << ", minute " << timezone.get_minute();

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_timezone(std::move(timezone));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

