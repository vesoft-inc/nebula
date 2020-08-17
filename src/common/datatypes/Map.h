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
    Map(Map&&) noexcept = default;
    explicit Map(std::unordered_map<std::string, Value> values) {
        kvs = std::move(values);
    }

    Map& operator=(const Map& rhs) {
        if (this == &rhs) { return *this; }
        kvs = rhs.kvs;
        return *this;
    }
    Map& operator=(Map&& rhs) noexcept {
        if (this == &rhs) { return *this; }
        kvs = std::move(rhs.kvs);
        return *this;
    }

    void clear() {
        kvs.clear();
    }

    // the configs of rocksdb will use the interface, so the value need modify to string
    std::string toString() const {
        std::vector<std::string> value(kvs.size());
        std::transform(kvs.begin(), kvs.end(), value.begin(), [](const auto &iter) -> std::string {
            std::stringstream out;
            out << "\"" << iter.first << "\"" << ":" << iter.second;
            return out.str();
        });

        std::stringstream os;
        os << "{" << folly::join(",", value) << "}";
        return os.str();
    }

    bool operator==(const Map& rhs) const {
        return kvs == rhs.kvs;
    }

    bool contains(const Value &value) const {
        if (!value.isStr()) {
            return false;
        }
        return kvs.count(value.getStr()) != 0;
    }

    const Value& at(const std::string &key) const {
        auto iter = kvs.find(key);
        if (iter == kvs.end()) {
            return Value::kNullValue;
        }
        return iter->second;
    }
};

inline std::ostream &operator<<(std::ostream& os, const Map& m) {
    return os << m.toString();
}

}  // namespace nebula
#endif  // COMMON_DATATYPES_MAP_H_
