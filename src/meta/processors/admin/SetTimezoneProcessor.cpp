/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "SetTimezoneProcessor.h"

namespace nebula {
namespace meta {

void SetTimezoneProcessor::process(const cpp2::SetTimezoneReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::timezoneLock());

    std::string timezoneKey = MetaServiceUtils::timezoneKey();
    std::vector<kvstore::KV> data;
    std::string timezoneValue = MetaServiceUtils::timezoneValue(req.get_timezone());
    data.emplace_back(std::move(timezoneKey), std::move(timezoneValue));

    doPut(std::move(data));
    LastUpdateTimeMan::update(kvstore_, time::WallClock::fastNowInMilliSec());
    return;
}

}  // namespace meta
}  // namespace nebula
