/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_SET_H_
#define COMMON_DATATYPES_SET_H_

#include "common/base/Base.h"
#include "common/datatypes/Value.h"

namespace nebula {

struct Set {
    std::unordered_set<Value> values;

    Set() = default;
    Set(const Set&) = default;
    Set(Set&&) = default;

    void clear() {
        values.clear();
    }

    std::string toString() const {
        std::stringstream os;
        os << "{";
        for (const auto &v : values) {
            os << v << ",";
        }
        os << "}";
        return os.str();
    }

    Set& operator=(const Set& rhs) {
        if (this == &rhs) { return *this; }
        values = rhs.values;
        return *this;
    }
    Set& operator=(Set&& rhs) {
        if (this == &rhs) { return *this; }
        values = std::move(rhs.values);
        return *this;
    }

    bool operator==(const Set& rhs) const {
        return values == rhs.values;
    }
};

inline std::ostream &operator<<(std::ostream& os, const Set& s) {
    return os << s.toString();
}

}  // namespace nebula
#endif  // COMMON_DATATYPES_SET_H_
