/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

bool checkDataExpiredForTTL(const meta::SchemaProviderIf* schema,
                            RowReader* reader,
                            const std::string& ttlCol,
                            int64_t ttlDuration) {
    auto now = time::WallClock::fastNowInSec();
    const auto& ftype = schema->getFieldType(ttlCol);

    int64_t v = 0;
    switch (ftype.type) {
        case nebula::cpp2::SupportedType::TIMESTAMP:
        case nebula::cpp2::SupportedType::INT: {
            auto ret = reader->getInt<int64_t>(ttlCol, v);
            if (ret != ResultType::SUCCEEDED) {
                // Reading wrong data should not be deleted
                return false;
            }
            break;
        }
        case nebula::cpp2::SupportedType::VID: {
            auto ret = reader->getVid(ttlCol, v);
            if (ret != ResultType::SUCCEEDED) {
                // Reading wrong data should not be deleted
                return false;
            }
            break;
        }
        default: {
            VLOG(1) << "Unsupport TTL column type";
            return false;
        }
    }

    if (now > (v + ttlDuration)) {
        return true;
    }
    return false;
}


}  // namespace storage
}  // namespace nebula
