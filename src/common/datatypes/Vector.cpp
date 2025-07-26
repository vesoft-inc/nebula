/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/datatypes/Vector.h"

#include <folly/String.h>

#include <sstream>

namespace nebula {

std::string Vector::toString() const {
  std::vector<std::string> value(values.size());
  std::transform(values.begin(), values.end(), value.begin(), [](const auto& v) -> std::string {
    return std::to_string(v);
  });
  std::stringstream os;
  os << "vector (" << folly::join(",", value) << ")";
  return os.str();
}

folly::dynamic Vector::toJson() const {
  auto listJsonObj = folly::dynamic::array();

  for (const auto& val : values) {
    listJsonObj.push_back(val);
  }

  return listJsonObj;
}

}  // namespace nebula
