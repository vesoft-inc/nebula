
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_DEDUPNODE_H_
#define STORAGE_EXEC_DEDUPNODE_H_

#include "common/base/Base.h"
#include "storage/exec/FilterNode.h"

namespace nebula {
namespace storage {

// DedupNode will be used to dedup the resultset based on the given fields
template <typename T>
class DeDupNode : public IterateNode<T> {
 public:
  using RelNode<T>::doExecute;

  DeDupNode(RuntimeContext* context, nebula::DataSet* resultSet, const std::vector<size_t>& pos)
      : IterateNode<T>(context, "DedupNode"), resultSet_(resultSet), pos_(pos) {}

  nebula::cpp2::ErrorCode doExecute(PartitionID partId) override {
    auto ret = RelNode<T>::doExecute(partId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }
    dedup(resultSet_->rows, pos_);
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  static void dedup(std::vector<Row>& rows, const std::vector<size_t>& pos) {
    std::sort(rows.begin(), rows.end(), [&pos](auto& l, auto& r) {
      for (const auto& p : pos) {
        if (l.values[p] != r.values[p]) {
          return l.values[p] < r.values[p];
        }
      }
      return false;
    });
    auto cmp = [&pos](auto& l, auto& r) {
      for (const auto& p : pos) {
        if (l.values[p] != r.values[p]) {
          return false;
        }
      }
      return true;
    };
    rows.erase(std::unique(rows.begin(), rows.end(), cmp), rows.end());
  }

 private:
  nebula::DataSet* resultSet_;
  std::vector<size_t> pos_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_DEDUPNODE_H_
