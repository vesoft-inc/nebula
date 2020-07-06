/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "utils/ConvertTimeType.h"

namespace nebula {

StatusOr<int64_t> ConvertTimeType::toTimestamp(const VariantType &value) {
    if (value.which() != VAR_INT64 && value.which() != VAR_STR) {
        return Status::Error("Invalid value type");
    }

    int64_t timestamp;
    if (value.which() == VAR_STR) {
        static const std::regex reg("^([1-9]\\d{3})-"
                                    "(0[1-9]|1[0-2]|\\d)-"
                                    "(0[1-9]|[1-2][0-9]|3[0-1]|\\d)\\s+"
                                    "(20|21|22|23|[0-1]\\d|\\d):"
                                    "([0-5]\\d|\\d):"
                                    "([0-5]\\d|\\d)$");
        std::smatch result;
        if (!std::regex_match(boost::get<std::string>(value), result, reg)) {
            return Status::Error("Invalid timestamp type");
        }
        struct tm time;
        memset(&time, 0, sizeof(time));
        time.tm_year = atoi(result[1].str().c_str()) - 1900;
        time.tm_mon = atoi(result[2].str().c_str()) - 1;
        time.tm_mday = atoi(result[3].str().c_str());
        time.tm_hour = atoi(result[4].str().c_str());
        time.tm_min = atoi(result[5].str().c_str());
        time.tm_sec = atoi(result[6].str().c_str());
        timestamp = mktime(&time);
    } else {
        timestamp = boost::get<int64_t>(value);
    }

    // The mainstream Linux kernel's implementation constrains this
    static const int64_t maxTimestamp = std::numeric_limits<int64_t>::max() / 1000000000;
    if (timestamp < 0 || (timestamp > maxTimestamp)) {
        return Status::Error("Invalid timestamp type");
    }
    return timestamp;
}

}  // namespace nebula
