/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
#include "common/base/StatusOr.h"
#include "common/datatypes/DataSet.h"
#include "interface/gen-cpp2/common_types.h"
#include "storage/CommonUtils.h"
namespace nebula {
namespace storage {
class IndexNode {
 public:
  explicit IndexNode(RuntimeContext* context);
  virtual void init() = 0;
  virtual nebula::cpp2::ErrorCode execute(PartitionID partId) = 0;
  virtual StatusOr<Row> next() = 0;
  virtual bool vailed() = 0;

 protected:
  RuntimeContext* context_;
  GraphSpaceID spaceId_;
};
}  // namespace storage
}  // namespace nebula
