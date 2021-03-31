/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <sstream>
#include <folly/String.h>

#include "common/datatypes/Set.h"

namespace nebula {

std::string Set::toString() const {
    std::vector<std::string> value(values.size());
    std::transform(
        values.begin(), values.end(), value.begin(), [](const auto& v) -> std::string {
            return v.toString();
        });
    std::stringstream os;
    os << "{" << folly::join(",", value) << "}";
    return os.str();
}

}  // namespace nebula
