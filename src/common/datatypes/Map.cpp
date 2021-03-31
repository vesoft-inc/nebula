/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/String.h>
#include <sstream>

#include "common/datatypes/Map.h"

namespace nebula {

std::string Map::toString() const {
    std::vector<std::string> value(kvs.size());
    std::transform(kvs.begin(), kvs.end(), value.begin(), [](const auto &iter) -> std::string {
        std::stringstream out;
        out << iter.first << ":" << iter.second;
        return out.str();
    });

    std::stringstream os;
    os << "{" << folly::join(",", value) << "}";
    return os.str();
}

}  // namespace nebula
