/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_DATATYPES_MAP_H_
#define COMMON_DATATYPES_MAP_H_

#include <folly/json.h>

#include <unordered_map>

#include "common/base/Logging.h"
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

  // constructor to convert from json string
  explicit Map(const std::string& json) {
    auto obj = folly::parseJson(json);
    for (auto& kv : obj.items()) {
      // for now, only limited primitive types in value field are supported
      //   no nested type(list, map, set) is supported
      if (kv.second.isString()) {
        kvs.emplace(kv.first.asString(), Value(kv.second.asString()));
      } else if (kv.second.isInt()) {
        kvs.emplace(kv.first.asString(), Value(kv.second.asInt()));
      } else if (kv.second.isDouble()) {
        kvs.emplace(kv.first.asString(), Value(kv.second.asDouble()));
      } else if (kv.second.isBool()) {
        kvs.emplace(kv.first.asString(), Value(kv.second.asBool()));
      } else if (kv.second.isNull()) {
        kvs.emplace(kv.first.asString(), Value());
      } else {
        LOG(ERROR) << "Only string, int64, double, bool, null are supported in "
                      "parseJson to construct Map, now trying to parse from: "
                   << kv.second.typeName();
      }
    }
  }

  Map& operator=(const Map& rhs) {
    if (this == &rhs) {
      return *this;
    }
    kvs = rhs.kvs;
    return *this;
  }
  Map& operator=(Map&& rhs) noexcept {
    if (this == &rhs) {
      return *this;
    }
    kvs = std::move(rhs.kvs);
    return *this;
  }

  void clear() {
    kvs.clear();
  }

  void __clear() {
    clear();
  }

  // the configs of rocksdb will use the interface, so the value need modify to
  // string
  std::string toString() const;
  folly::dynamic toJson() const;
  // Extract the metadata of the value of each kv pair
  folly::dynamic getMetaData() const;

  bool operator==(const Map& rhs) const {
    return kvs == rhs.kvs;
  }

  bool contains(const Value& value) const {
    if (!value.isStr()) {
      return false;
    }
    return kvs.count(value.getStr()) != 0;
  }

  const Value& at(const std::string& key) const {
    auto iter = kvs.find(key);
    if (iter == kvs.end()) {
      return Value::kNullUnknownProp;
    }
    return iter->second;
  }

  size_t size() const {
    return kvs.size();
  }
};

inline std::ostream& operator<<(std::ostream& os, const Map& m) {
  return os << m.toString();
}

}  // namespace nebula

namespace std {
template <>
struct hash<nebula::Map> {
  std::size_t operator()(const nebula::Map& m) const noexcept;
};

}  // namespace std
#endif  // COMMON_DATATYPES_MAP_H_
