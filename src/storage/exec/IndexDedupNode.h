/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXDEDUPNODE_H
#define STORAGE_EXEC_INDEXDEDUPNODE_H
#include "common/datatypes/DataSet.h"
#include "folly/container/F14Set.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {
/**
 *
 * IndexDedupNode
 *
 * reference: IndexNode
 *
 * `IndexDedupNode` is the class which is used to eliminate duplicate rows of data returned by
 * multiple child nodes.
 *                   ┌───────────┐
 *                   │ IndexNode │
 *                   └─────┬─────┘
 *                         │
 *                ┌────────┴───────┐
 *                │ IndexDedupNode │
 *                └────────────────┘
 * Member:
 * `dedupColumns_`: columns' name which are used to dedup
 * `dedupPos_`    : dedup columns' position in child return row
 * `dedupSet_`    : the set which record the rows have been return to parent
 * `currentChild_`: current iterate child
 */

class IndexDedupNode : public IndexNode {
 public:
  IndexDedupNode(const IndexDedupNode& node);
  IndexDedupNode(RuntimeContext* context, const std::vector<std::string>& dedupColumn);
  ::nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  std::unique_ptr<IndexNode> copy() override;
  std::string identify() override;

 private:
  inline bool dedup(const Row& row);
  ::nebula::cpp2::ErrorCode doExecute(PartitionID partId) override;
  Result doNext() override;
  Result iterateCurrentChild(size_t currentChild);
  // Define RowWrapper for dedup
  class RowWrapper {
   public:
    RowWrapper(const Row& row, const std::vector<size_t>& posList);
    inline const List& values() const {
      return values_;
    }

   private:
    List values_;
  };
  // End of RowWrapper
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
  std::vector<std::string> dedupColumns_;
  std::vector<size_t> dedupPos_;
  folly::F14FastSet<RowWrapper, Hasher, Equal> dedupSet_;
  size_t currentChild_ = 0;
};

/* Definition of inline function */
inline bool IndexDedupNode::dedup(const Row& row) {
  auto result = dedupSet_.emplace(row, dedupPos_);
  return result.second;
}

}  // namespace storage
}  // namespace nebula
#endif
