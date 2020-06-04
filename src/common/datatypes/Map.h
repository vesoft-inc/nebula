/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_MAP_H_
#define COMMON_DATATYPES_MAP_H_

#include "common/base/Base.h"
#include "common/datatypes/Value.h"

namespace nebula {

struct Map {
    std::unordered_map<std::string, Value> kvs;

    Map() = default;
    Map(const Map&) = default;
    Map(Map&&) = default;

    Map& operator=(const Map& rhs) {
        if (this == &rhs) { return *this; }
        kvs = rhs.kvs;
        return *this;
    }
    Map& operator=(Map&& rhs) {
        if (this == &rhs) { return *this; }
        kvs = std::move(rhs.kvs);
        return *this;
    }

    void clear() {
        kvs.clear();
    }

    bool operator==(const Map& rhs) const {
        return kvs == rhs.kvs;
    }
};

}  // namespace nebula
#endif  // COMMON_DATATYPES_MAP_H_
