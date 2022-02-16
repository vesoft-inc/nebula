/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXAGGREGATENODE_H
#define STORAGE_EXEC_INDEXAGGREGATENODE_H
#include <stddef.h>  // for size_t
#include <stdint.h>  // for int64_t

#include <limits>   // for numeric_limits
#include <memory>   // for unique_ptr
#include <string>   // for string
#include <utility>  // for pair
#include <vector>   // for vector

#include "common/datatypes/DataSet.h"          // for Row
#include "common/datatypes/Value.h"            // for Value
#include "interface/gen-cpp2/common_types.h"   // for ErrorCode
#include "interface/gen-cpp2/storage_types.h"  // for StatType
#include "storage/exec/IndexNode.h"            // for IndexNode, IndexNode::...

namespace nebula {
namespace storage {
struct RuntimeContext;

struct RuntimeContext;

// used to save stat value for each column
struct ColumnStat {
  ColumnStat() = default;

  explicit ColumnStat(const cpp2::StatType& statType) : statType_(statType) {}

  cpp2::StatType statType_;
  mutable Value sum_ = 0L;
  mutable Value count_ = 0L;
  mutable Value min_ = std::numeric_limits<int64_t>::max();
  mutable Value max_ = std::numeric_limits<int64_t>::min();
};

class IndexAggregateNode : public IndexNode {
 public:
  IndexAggregateNode(const IndexAggregateNode& node);
  explicit IndexAggregateNode(RuntimeContext* context,
                              const std::vector<std::pair<std::string, cpp2::StatType>>& statInfos,
                              size_t returnColumnsCount);

  nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  void initStatValue();
  void addStatValue(const Value& value, ColumnStat* stat);
  Row project(Row&& row);
  Row calculateStats();

  std::unique_ptr<IndexNode> copy() override;
  std::string identify() override;

 private:
  Result doNext() override;
  std::vector<std::pair<std::string, cpp2::StatType>> statInfos_;
  std::vector<ColumnStat> stats_;
  Map<std::string, size_t> retColMap_;
  size_t returnColumnsCount_;
};

}  // namespace storage
}  // namespace nebula
#endif
