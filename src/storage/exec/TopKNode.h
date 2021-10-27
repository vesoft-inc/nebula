/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_TOPKNODE_H_
#define STORAGE_EXEC_TOPKNODE_H_

#include "common/base/Base.h"
#include "common/utils/TopKHeap.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {

// TopKNode will return a DataSet with fixed size
template <class T>
class TopKNode final : public RelNode<T> {
 public:
  using RelNode<T>::doExecute;

  TopKNode(nebula::DataSet* resultSet, std::vector<cpp2::OrderBy> orderBy, int limit)
      : resultSet_(resultSet), orderBy_(orderBy), limit_(limit) {
    RelNode<T>::name_ = "TopKNode";
  }

  nebula::cpp2::ErrorCode doExecute(PartitionID partId) override {
    auto ret = RelNode<T>::doExecute(partId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }
    if (limit_ > 0) {
      resultSet_->rows = topK(resultSet_->rows, orderBy_, limit_);
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  static std::vector<Row> topK(std::vector<Row>& rows, std::vector<cpp2::OrderBy> orderBy, int k) {
    auto comparator = [orderBy](const Row& lhs, const Row& rhs) {
      for (auto& item : orderBy) {
        auto index = item.get_pos();
        auto orderType = item.get_direction();
        if (lhs[index] == rhs[index]) {
          continue;
        }
        if (orderType == cpp2::OrderDirection::ASCENDING) {
          return lhs[index] < rhs[index];
        } else if (orderType == cpp2::OrderDirection::DESCENDING) {
          return lhs[index] > rhs[index];
        }
      }
      return false;
    };
    TopKHeap<Row> topKHeap(k, std::move(comparator));
    for (auto row : rows) {
      topKHeap.push(row);
    }
    return topKHeap.moveTopK();
  }

 private:
  nebula::DataSet* resultSet_;
  std::vector<cpp2::OrderBy> orderBy_{};
  int limit_{-1};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_TOPKNODE_H_
