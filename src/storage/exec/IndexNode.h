/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
#include "common/base/ErrorOr.h"
#include "common/datatypes/DataSet.h"
#include "folly/container/F14Map.h"
#include "interface/gen-cpp2/common_types.h"
#include "storage/CommonUtils.h"
namespace nebula {
namespace storage {
using ErrorCode = ::nebula::cpp2::ErrorCode;
template <typename K, typename V>
using Map = folly::F14FastMap<K, V>;
class IndexNode {
 public:
  template <typename ResultType>
  using ErrorOr = ::nebula::ErrorOr<ErrorCode, ResultType>;
  explicit IndexNode(RuntimeContext* context);
  virtual nebula::cpp2::ErrorCode execute(PartitionID partId) = 0;
  virtual ErrorOr<Row> next(bool& hasNext) = 0;

 protected:
  RuntimeContext* context_;
  GraphSpaceID spaceId_;
  std::vector<IndexNode*> children_;
};
}  // namespace storage
}  // namespace nebula
