/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/datatypes/Map.h"

#include <folly/String.h>

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

folly::dynamic Map::toJsonObj() const {
  folly::dynamic mapJsonObj = folly::dynamic::object();

  for (const auto& iter : kvs) {
    mapJsonObj.insert(iter.first, iter.second.toJsonObj());
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

}  // namespace nebula
