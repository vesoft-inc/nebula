/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <bits/stdint-intn.h>

#include <tuple>
#include <unordered_map>
#include <vector>

#include "common/base/StatusOr.h"
#include "common/thrift/ThriftTypes.h"

namespace nebula {
namespace graph {

class PagingContext {
 public:
  StatusOr<std::vector<std::tuple<PartitionID, std::string>>> match(GraphSpaceID spaceId,
                                                                    int32_t schemaId,
                                                                    int64_t offset) const;

  void update(GraphSpaceID spaceId,
              int32_t schemaId,
              int64_t offset,
              std::vector<std::tuple<PartitionID, std::string>> keys);

 private:
  // Cached paging key
  std::unordered_map<
      std::tuple<GraphSpaceID, int32_t /* SchemaID */>,
      std::tuple<int64_t /* offset */, std::vector<std::tuple<PartitionID, std::string /* key */>>>>
      paging_;
  mutable folly::RWSpinLock lock_;
};

}  // namespace graph
}  // namespace nebula
