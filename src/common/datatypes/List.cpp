/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/datatypes/List.h"

#include <folly/String.h>

#include <sstream>

namespace nebula {

std::string List::toString() const {
  std::vector<std::string> value(values.size());
  std::transform(values.begin(), values.end(), value.begin(), [](const auto& v) -> std::string {
    return v.toString();
  });
  std::stringstream os;
  os << "[" << folly::join(",", value) << "]";
  return os.str();
}

folly::dynamic List::toJson() const {
  auto listJsonObj = folly::dynamic::array();

  for (const auto& val : values) {
    listJsonObj.push_back(val.toJson());
  }

  return listJsonObj;
}

folly::dynamic List::getMetaData() const {
  auto listMetadataObj = folly::dynamic::array();

  for (const auto& val : values) {
    listMetadataObj.push_back(val.getMetaData());
  }

  return listMetadataObj;
}

}  // namespace nebula
