/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXLIMITNODE_H
#define STORAGE_EXEC_INDEXLIMITNODE_H
#include "folly/Likely.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {
class IndexLimitNode : public IndexNode {
 public:
  IndexLimitNode(const IndexLimitNode& node);
  IndexLimitNode(RuntimeContext* context, uint64_t offset, uint64_t limit);
  IndexLimitNode(RuntimeContext* context, uint64_t limit);
  std::unique_ptr<IndexNode> copy() override;
  std::string identify() override;

 protected:
  const uint64_t offset_, limit_;

 private:
  nebula::cpp2::ErrorCode doExecute(PartitionID partId) override;
  Result doNext() override;
  uint64_t currentOffset_ = 0;
};
}  // namespace storage

}  // namespace nebula
#endif
