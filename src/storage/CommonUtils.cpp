/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/CommonUtils.h"
#include "common/time/WallClock.h"

namespace nebula {
namespace storage {

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
