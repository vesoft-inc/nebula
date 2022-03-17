/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXPROJECTIONNODE_H
#define STORAGE_EXEC_INDEXPROJECTIONNODE_H

#include "folly/Likely.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {

/**
 * @brief IndexProjectionNode is the class which is used to reformat the row to ensure that the
 * format of the returned row meets the requirements of RPC request.
 *
 * @implements IndexNode
 * @see IndexNode, InitContext
 * @todo requiredColumns_ support expression
 */
class IndexProjectionNode : public IndexNode {
 public:
  /**
   * @brief shallow copy
   * @param node
   * @see IndexNode::IndexNode(const IndexNode& node)
   */
  IndexProjectionNode(const IndexProjectionNode& node);

  /**
   * @brief Construct a new Index Projection Node object
   *
   * @param context
   * @param requiredColumns the format next() will return
   */
  IndexProjectionNode(RuntimeContext* context, const std::vector<std::string>& requiredColumns);
  nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  std::unique_ptr<IndexNode> copy() override;
  std::string identify() override;

 private:
  Result doNext() override;

  /**
   * @brief according to required columns, adjust column order or calculate expression
   *
   * @param row
   * @return Row
   */
  Row project(Row&& row);

  /**
   * @brief Row format required by parent node
   *
   */
  std::vector<std::string> requiredColumns_;

  /**
   * @brief each column position in child node return row
   */
  Map<std::string, size_t> colPos_;
};
}  // namespace storage
}  // namespace nebula
#endif
