/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_STORAGE_EXECUTIONPLAN_H
#define NEBULA_STORAGE_EXECUTIONPLAN_H

#include <utility>

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class ExecutionPlan final {

public:
    enum class ScanType : uint8_t {
        DATA_SCAN          = 0x01,
        INDEX_SCAN         = 0x02,
    };

    ExecutionPlan() {};

    ExecutionPlan(std::shared_ptr<nebula::cpp2::IndexItem>  index,
                  std::map<std::string, nebula::cpp2::SupportedType>  indexCols,
                  std::vector<nebula::cpp2::ColumnHint>  columnHints)
        : index_(std::move(index)),
          indexCols_(std::move(indexCols)),
          columnHints_(std::move(columnHints)) {};

    ~ExecutionPlan() = default;

public:
    const std::shared_ptr<nebula::cpp2::IndexItem>& getIndex() const;

    void setIndex(const std::shared_ptr<nebula::cpp2::IndexItem>& index);

    const std::unique_ptr<Expression>& getExp() const;

    cpp2::ErrorCode decodeExp(folly::StringPiece filter);

    const std::map<std::string, nebula::cpp2::SupportedType>& getIndexCols() const;

    nebula::cpp2::SupportedType getIndexColType(const folly::StringPiece& prop) const;

    void setIndexCols(const std::map<std::string, nebula::cpp2::SupportedType>& indexCols);

    const std::vector<nebula::cpp2::ColumnHint>& getColumnHints() const;

    void setColumnHints(const std::vector<nebula::cpp2::ColumnHint>& columnHints);

    IndexID getIndexId() const;

    ScanType scanType() const;

    bool isRangeScan() const;

    StatusOr<std::string> getPrefixStr(PartitionID partId) const;

    StatusOr<std::pair<std::string, std::string>> getRangeStartStr(PartitionID partId) const;

private:
    std::shared_ptr<nebula::cpp2::IndexItem>              index_{nullptr};
    std::unique_ptr<ExpressionContext>                    expCtx_{nullptr};
    std::unique_ptr<Expression>                           exp_{nullptr};
    std::map<std::string, nebula::cpp2::SupportedType>    indexCols_;
    std::vector<nebula::cpp2::ColumnHint>                 columnHints_;
};

}  // namespace storage
}  // namespace nebula

#endif  // NEBULA_STORAGE_EXECUTIONPLAN_H
