/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_SET_H_
#define COMMON_DATATYPES_SET_H_

#include <unordered_set>

#include "common/datatypes/Value.h"

namespace nebula {

struct Set {
    std::unordered_set<Value> values;

    Set() = default;
    Set(const Set&) = default;
    Set(Set&&) noexcept = default;
    explicit Set(std::unordered_set<Value> value) {
        values = std::move(value);
    }

    void clear() {
        values.clear();
    }

    void __clear() {
        clear();
    }

    std::string toString() const;

    Set& operator=(const Set& rhs) {
        if (this == &rhs) { return *this; }
        values = rhs.values;
        return *this;
    }

    Set& operator=(Set&& rhs) noexcept {
        if (this == &rhs) { return *this; }
        values = std::move(rhs.values);
        return *this;
    }

    bool operator==(const Set& rhs) const {
        return values == rhs.values;
    }

    bool contains(const Value &value) const {
        return values.count(value) != 0;
    }

    size_t size() const {
        return values.size();
    }
};

inline std::ostream &operator<<(std::ostream& os, const Set& s) {
    return os << s.toString();
}

}  // namespace nebula
#endif  // COMMON_DATATYPES_SET_H_
