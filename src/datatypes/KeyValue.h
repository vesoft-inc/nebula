/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATATYPES_KEYVALUE_H_
#define DATATYPES_KEYVALUE_H_

#include "base/Base.h"

namespace nebula {

struct KeyValue {
    std::string key;
    std::string value;

    KeyValue() {}
    KeyValue(KeyValue&& rhs)
        : key(std::move(rhs.key))
        , value(std::move(rhs.value)) {}
    KeyValue(const KeyValue& rhs)
        : key(rhs.key)
        , value(rhs.value) {}
    explicit KeyValue(std::pair<std::string, std::string>&& kv)
        : key(std::move(kv.first))
        , value(std::move(kv.second)) {}

    bool operator==(const KeyValue& rhs) const {
        if (key != rhs.key) {
            return false;
        }
        return value == rhs.value;
    }

    bool operator<(const KeyValue& rhs) const {
        if (key != rhs.key) {
            return key < rhs.key;
        }
        return value < rhs.value;
    }
};

}  // namespace nebula
#endif  // DATATYPES_KEYVALUE_H_
