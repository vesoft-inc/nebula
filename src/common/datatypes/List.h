/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_LIST_H_
#define COMMON_DATATYPES_LIST_H_

#include <vector>
#include <algorithm>

#include "common/datatypes/Value.h"

namespace nebula {

struct List {
    std::vector<Value> values;

    List() = default;
    List(const List&) = default;
    List(List&&) noexcept = default;
    explicit List(std::vector<Value>&& vals) {
        values = std::move(vals);
    }
    explicit List(const std::vector<Value> &l) : values(l) {}

    bool empty() const {
        return values.empty();
    }

    void reserve(std::size_t n) {
        values.reserve(n);
    }

    template <typename T,
              typename = typename std::enable_if<std::is_convertible<T, Value>::value>::type>
    void emplace_back(T &&v) {
        values.emplace_back(std::forward<T>(v));
    }

    void clear() {
        values.clear();
    }

    void __clear() {
        clear();
    }

    List& operator=(const List& rhs) {
        if (this == &rhs) { return *this; }
        values = rhs.values;
        return *this;
    }
    List& operator=(List&& rhs) noexcept {
        if (this == &rhs) { return *this; }
        values = std::move(rhs.values);
        return *this;
    }

    bool operator==(const List& rhs) const {
        return values == rhs.values;
    }

    bool operator<(const List& rhs) const {
        return values < rhs.values;
    }

    const Value& operator[](size_t i) const {
        return values[i];
    }

    bool contains(const Value &value) const {
        return std::find(values.begin(), values.end(), value) != values.end();
    }

    size_t size() const {
        return values.size();
    }

    std::string toString() const;
};

inline std::ostream &operator<<(std::ostream& os, const List& l) {
    return os << l.toString();
}

}  // namespace nebula

namespace std {
template<>
struct hash<nebula::List> {
    std::size_t operator()(const nebula::List& h) const noexcept {
        size_t seed = 0;
        for (auto& v : h.values) {
            seed ^= hash<nebula::Value>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }
};
}  // namespace std
#endif  // COMMON_DATATYPES_LIST_H_
