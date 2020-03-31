/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/index/ExecutionPlan.h"

namespace nebula {
namespace storage {

const std::shared_ptr<nebula::cpp2::IndexItem>& ExecutionPlan::getIndex() const {
    return index_;
}

void ExecutionPlan::setIndex(const std::shared_ptr<nebula::cpp2::IndexItem>& index) {
    index_ = index;
}

const std::unique_ptr<Expression>& ExecutionPlan::getExp() const {
    return exp_;
}

cpp2::ErrorCode ExecutionPlan::decodeExp(folly::StringPiece filter) {
    auto ret = Expression::decode(filter);
    if (!ret.ok()) {
        VLOG(1) << "Can't decode the filter " << filter;
        return cpp2::ErrorCode::E_INVALID_FILTER;
    }
    exp_ = std::move(ret).value();
    expCtx_ = std::make_unique<ExpressionContext>();
    exp_->setContext(expCtx_.get());
    auto status = exp_->prepare();
    if (!status.ok()) {
        return cpp2::ErrorCode::E_INVALID_FILTER;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

const std::map<std::string, nebula::cpp2::SupportedType>& ExecutionPlan::getIndexCols() const {
    return indexCols_;
}

nebula::cpp2::SupportedType ExecutionPlan::getIndexColType(const folly::StringPiece& prop) const {
    auto it = indexCols_.find(prop.str());
    if (it != indexCols_.end()) {
        return it->second;
    }
    return nebula::cpp2::SupportedType::UNKNOWN;
}

void ExecutionPlan::setIndexCols(
    const std::map<std::string, nebula::cpp2::SupportedType>& indexCols) {
    indexCols_ = indexCols;
}

const std::vector<nebula::cpp2::ColumnHint>& ExecutionPlan::getColumnHints() const {
    return columnHints_;
}

void ExecutionPlan::setColumnHints(const std::vector<nebula::cpp2::ColumnHint>& columnHints) {
    columnHints_ = columnHints;
}

IndexID ExecutionPlan::getIndexId() const {
    if (index_ != nullptr) {
        return index_->get_index_id();
    }
    return -1;
}

ExecutionPlan::ScanType ExecutionPlan::scanType() const {
    return (index_ == nullptr) ? ScanType::DATA_SCAN : ScanType::INDEX_SCAN;
}

bool ExecutionPlan::isRangeScan() const {
    if (!columnHints_.empty()) {
        return columnHints_[0].get_scan_type() == nebula::cpp2::ScanType::RANGE;
    }
    return false;
}

StatusOr<std::string> ExecutionPlan::getPrefixStr(PartitionID partId) const {
    if (scanType() != ScanType::INDEX_SCAN) {
        return Status::Error("invalid scan type");
    }
    std::string prefix = NebulaKeyUtils::indexPrefix(partId, index_->get_index_id());
    for (auto& col : columnHints_) {
        if (col.__isset.scan_type &&
            col.get_scan_type() == nebula::cpp2::ScanType::PREFIX) {
            prefix.append(col.get_begin_raw());
        } else {
            return Status::Error("invalid scan type , colums : %s", col.get_column_name().c_str());
        }
    }
    return prefix;
}

StatusOr<std::pair<std::string, std::string>>
ExecutionPlan::getRangeStartStr(PartitionID partId) const {
    if (scanType() != ScanType::INDEX_SCAN) {
        return Status::Error("invalid scan type");
    }
    std::string start , end;
    start = end = NebulaKeyUtils::indexPrefix(partId, index_->get_index_id());
    for (auto& col : columnHints_) {
        if (!col.__isset.scan_type) {
            return Status::Error("missing scan type by column : %s", col.get_column_name().c_str());
        }
        if (col.get_scan_type() == nebula::cpp2::ScanType::PREFIX) {
            start.append(col.get_begin_raw());
            end.append(col.get_begin_raw());
        } else {
            start.append(col.get_begin_raw());
            end.append(col.get_end_raw());
        }
    }
    return std::make_pair<std::string, std::string>(std::move(start), std::move(end));
}

}  // namespace storage
}  // namespace nebula
