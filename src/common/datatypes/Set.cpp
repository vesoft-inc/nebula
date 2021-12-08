/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/datatypes/Set.h"

#include <folly/String.h>

#include <sstream>

namespace nebula {

std::string Set::toString() const {
  std::vector<std::string> value(values.size());
  std::transform(values.begin(), values.end(), value.begin(), [](const auto& v) -> std::string {
    return v.toString();
  });
  std::stringstream os;
  os << "{" << folly::join(",", value) << "}";
  return os.str();
}

folly::dynamic Set::toJson() const {
  auto setJsonObj = folly::dynamic::array();

  for (const auto& val : values) {
    setJsonObj.push_back(val.toJson());
  }

  return setJsonObj;
}

folly::dynamic Set::getMetaData() const {
  auto setMetadataObj = folly::dynamic::array();

  for (const auto& val : values) {
    setMetadataObj.push_back(val.getMetaData());
  }

  return setMetadataObj;
}

}  // namespace nebula

namespace std {
std::size_t hash<nebula::Set>::operator()(const nebula::Set& s) const noexcept {
  size_t seed = 0;
  for (auto& v : s.values) {
    seed ^= hash<nebula::Value>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  }
  return seed;
}

}  // namespace std
