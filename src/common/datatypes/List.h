/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_LIST_H_
#define COMMON_DATATYPES_LIST_H_

#include "common/base/Base.h"
#include "common/datatypes/Value.h"

namespace nebula {

struct List {
    std::vector<Value> values;

    List() = default;
    List(const List&) = default;
    List(List&&) = default;
    explicit List(std::vector<Value>&& vals) {
        values = std::move(vals);
    }

    void clear() {
        values.clear();
    }

    List& operator=(const List& rhs) {
        if (this == &rhs) { return *this; }
        values = rhs.values;
        return *this;
    }
    List& operator=(List&& rhs) {
        if (this == &rhs) { return *this; }
        values = std::move(rhs.values);
        return *this;
    }

    bool operator==(const List& rhs) const {
        return values == rhs.values;
    }

    const Value& operator[](size_t i) const {
        return values[i];
    }

    size_t size() const {
        return values.size();
    }
};

}  // namespace nebula
#endif  // COMMON_DATATYPES_LIST_H_
