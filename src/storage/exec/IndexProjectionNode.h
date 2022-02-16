/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXPROJECTIONNODE_H
#define STORAGE_EXEC_INDEXPROJECTIONNODE_H

#include <stddef.h>  // for size_t

#include <memory>  // for unique_ptr
#include <string>  // for string, basic_string
#include <vector>  // for vector

#include "common/datatypes/DataSet.h"  // for Row
#include "folly/Likely.h"
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "storage/exec/IndexNode.h"           // for IndexNode, IndexNode::R...

namespace nebula {
namespace storage {
struct RuntimeContext;

struct RuntimeContext;

/**
 *
 * IndexProjectionNode
 *
 * reference: IndexNode
 *
 * `IndexProjectionNode` is the class which is used to reformat the row to ensure that the format of
 * the returned row meets the requirements of RPC request.
 *                   ┌───────────┐
 *                   │ IndexNode │
 *                   └─────┬─────┘
 *                         │
 *              ┌──────────┴──────────┐
 *              │ IndexProjectionNode │
 *              └─────────────────────┘
 *
 * Member:
 * `requiredColumns_` : Row format required by parent node
 * `colPos_`          : each column position in child node return row
 */
class IndexProjectionNode : public IndexNode {
 public:
  IndexProjectionNode(const IndexProjectionNode& node);
  IndexProjectionNode(RuntimeContext* context, const std::vector<std::string>& requiredColumns);
  nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  std::unique_ptr<IndexNode> copy() override;
  std::string identify() override;

 private:
  Result doNext() override;
  Row project(Row&& row);
  std::vector<std::string> requiredColumns_;
  Map<std::string, size_t> colPos_;
};
}  // namespace storage
}  // namespace nebula
#endif
