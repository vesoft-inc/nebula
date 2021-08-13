/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_UTIL_UTILS_H_
#define GRAPH_UTIL_UTILS_H_

#include <folly/String.h>

#include <iterator>
#include <string>
#include <vector>

namespace nebula {
namespace util {

template <typename Container, typename Fn>
std::string join(const Container& container, Fn fn, const std::string& delimiter = ",") {
  std::vector<std::string> strs;
  for (auto iter = std::begin(container), end = std::end(container); iter != end; ++iter) {
    strs.emplace_back(fn(*iter));
  }
  return folly::join(delimiter, strs);
}

}  // namespace util
}  // namespace nebula

#endif  // GRAPH_UTIL_UTILS_H_
