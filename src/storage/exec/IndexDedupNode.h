/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
#include "common/datatypes/DataSet.h"
#include "folly/container/F14Set.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {
class IndexDedupNode : public IndexNode {
 public:
  IndexDedupNode(RuntimeContext* context,
                 const std::vector<std::string>& dedupColumn,
                 const std::vector<std::string>& inputColumn);
  nebula::cpp2::ErrorCode execute(PartitionID partId) = 0;
  ErrorOr<Row> next(bool& hasNext) override {
    DCHECK_EQ(children_.size(), 1);
    auto& child = *children_[0];
    do {
      auto result = child.next(hasNext);
      if (!hasNext || !nebula::ok(result)) {
        return result;
      }
      if (dedup(::nebula::value(result))) {
        return result;
      }
    } while (true);
  }

 private:
  bool dedup(const Row& row) {
    auto result = dedupSet_.emplace(row);
    return result.second;
  }
  class RowWrapper {
   public:
    RowWrapper(const Row& row, const std::vector<size_t>& posList) {
      for (auto p : posList) {
        values_.emplace_back(row[p]);
      }
    }
    const List& values() const { return values_; }

   private:
    List values_;
  };
  struct Hasher {
    size_t operator()(const RowWrapper& wrapper) const {
      return std::hash<List>()(wrapper.values());
    }
  };
  struct Equal {
    bool operator()(const RowWrapper& a, const RowWrapper& b) const {
      return a.values() == b.values();
    }
  };
  std::vector<size_t> dedupPos_;
  folly::F14FastSet<RowWrapper, Hasher, Equal> dedupSet_;
};
}  // namespace storage

}  // namespace nebula
