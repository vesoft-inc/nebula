/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "TimezoneSentence.h"

namespace nebula {

std::string TimezoneSentence::toString() const {
    switch (type_) {
        case TimezoneType::kGetTimezone:
            return folly::stringPrintf("GET TIME_ZONE");
        case TimezoneType::kSetTimezone:
            return folly::stringPrintf("SET TIME_ZONE %s", timezone_->c_str());
        default:
            return "Unknown sentence";
    }
}
}  // namespace nebula

