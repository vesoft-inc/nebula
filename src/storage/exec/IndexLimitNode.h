/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXLIMITNODE_H
#define STORAGE_EXEC_INDEXLIMITNODE_H
#include <stdint.h>  // for uint64_t

#include <memory>  // for unique_ptr
#include <string>  // for string

#include "common/thrift/ThriftTypes.h"  // for PartitionID
#include "folly/Likely.h"
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "storage/exec/IndexNode.h"           // for IndexNode, IndexNode::R...

namespace nebula {
namespace storage {
struct RuntimeContext;

struct RuntimeContext;

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
