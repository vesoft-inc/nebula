/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/datatypes/Map.h"

#include <folly/String.h>
#include <folly/json.h>

#include <sstream>

namespace nebula {

std::string Map::toString() const {
  std::vector<std::string> value(kvs.size());
  std::transform(kvs.begin(), kvs.end(), value.begin(), [](const auto& iter) -> std::string {
    std::stringstream out;
    out << iter.first << ":" << iter.second;
    return out.str();
  });

  std::stringstream os;
  os << "{" << folly::join(",", value) << "}";
  return os.str();
}

folly::dynamic Map::toJson() const {
  folly::dynamic mapJsonObj = folly::dynamic::object();

  for (const auto& iter : kvs) {
    mapJsonObj.insert(iter.first, iter.second.toJson());
  }

  return mapJsonObj;
}

folly::dynamic Map::getMetaData() const {
  auto mapMetadataObj = folly::dynamic::array();

  for (const auto& kv : kvs) {
    mapMetadataObj.push_back(kv.second.getMetaData());
  }

  return mapMetadataObj;
}

// Map constructor to covert from folly::dynamic object
// Called by function: json_extract()

// TODO(wey-gu) support Datetime, deeper nested Map/Datatypes
Map::Map(const folly::dynamic& obj) {
  DCHECK(obj.isObject());
  for (auto& kv : obj.items()) {
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
    } else if (kv.second.isObject()) {
      std::unordered_map<std::string, Value> values;
      for (auto& nkv : kv.second.items()) {
        if (nkv.second.isString()) {
          values.emplace(nkv.first.asString(), Value(nkv.second.asString()));
        } else if (nkv.second.isInt()) {
          values.emplace(nkv.first.asString(), Value(nkv.second.asInt()));
        } else if (nkv.second.isDouble()) {
          values.emplace(nkv.first.asString(), Value(nkv.second.asDouble()));
        } else if (nkv.second.isBool()) {
          values.emplace(nkv.first.asString(), Value(nkv.second.asBool()));
        } else {
          LOG(WARNING) << "JSON_EXTRACT nested layer 1: Map can be populated only by "
                          "Bool, Double, Int, String value and null, now trying to parse from: "
                       << nkv.second.typeName();
        }
      }
      kvs.emplace(kv.first.asString(), Value(Map(std::move(values))));
    } else {
      LOG(WARNING) << "JSON_EXTRACT Only Bool, Double, Int, String value, null and Map(depth==1) "
                      "are supported, now trying to parse from: "
                   << kv.second.typeName();
    }
  }
}

}  // namespace nebula

namespace std {
std::size_t hash<nebula::Map>::operator()(const nebula::Map& m) const noexcept {
  size_t seed = 0;
  for (auto& v : m.kvs) {
    seed ^= hash<std::string>()(v.first) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    seed ^= hash<nebula::Value>()(v.second) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  }
  return seed;
}

}  // namespace std
