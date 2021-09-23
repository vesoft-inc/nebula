/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/session/PagingContext.h"

namespace nebula {
namespace graph {

StatusOr<std::vector<std::tuple<PartitionID, std::string>>> PagingContext::match(
    const GraphSpaceID spaceId, const int32_t schemaId, const int64_t offset) const {
  folly::RWSpinLock::ReadHolder r(lock_);
  const auto find = paging_.find(std::make_tuple(spaceId, schemaId));
  if (find == paging_.end()) {
    return Status::Error("Can't find paging context.");
  }
  if (std::get<0>(find->second) != offset) {
    return Status::Error("Can't find paging context.");
  }
  return std::get<1>(find->second);
}

void PagingContext::update(const GraphSpaceID spaceId,
                           const int32_t schemaId,
                           const int64_t offset,
                           std::vector<std::tuple<PartitionID, std::string>> keys) {
  folly::RWSpinLock::WriteHolder w(lock_);
  paging_[std::make_tuple(spaceId, schemaId)] = std::make_tuple(offset, std::move(keys));
}

}  // namespace graph
}  // namespace nebula
