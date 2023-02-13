// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_UTIL_UTILS_H_
#define GRAPH_UTIL_UTILS_H_

#include <folly/String.h>
#include <folly/dynamic.h>

#include <iterator>
#include <string>
#include <vector>

#include "common/datatypes/HostAddr.h"

namespace nebula::storage::cpp2 {
class ResponseCommon;
}

namespace nebula::graph::util {

// Iterates the container and for each element, apply the function fn(). Joins the results of the
// fn() with the delimiter
template <typename Container, typename Fn>
std::string join(const Container& container, Fn fn, const std::string& delimiter = ",") {
  std::vector<std::string> strs;
  for (auto iter = std::begin(container), end = std::end(container); iter != end; ++iter) {
    strs.emplace_back(fn(*iter));
  }
  return folly::join(delimiter, strs);
}

folly::dynamic getStorageDetail(const std::map<std::string, int32_t>& profileDetail);

folly::dynamic collectRespProfileData(const storage::cpp2::ResponseCommon& resp,
                                      const std::tuple<HostAddr, int32_t, int32_t>& info,
                                      size_t numVertices = 0UL);

}  // namespace nebula::graph::util

#endif  // GRAPH_UTIL_UTILS_H_
