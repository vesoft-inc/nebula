/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_INDEXSCANNODE_H_
#define STORAGE_EXEC_INDEXSCANNODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {
template<typename T>
class IndexScanNode : public RelNode<T> {
public:
    using RelNode<T>::execute;

    IndexScanNode(PlanContext* planCtx,
                  IndexID indexId,
                  std::vector<cpp2::IndexColumnHint> columnHints)
    : planContext_(planCtx)
    , indexId_(indexId)
    , columnHints_(std::move(columnHints)) {
        /**
         * columnHints's elements are {scanType = PREFIX|RANGE; beginStr; endStr},
         *                            {scanType = PREFIX|RANGE; beginStr; endStr},...
         * if the scanType is RANGE, means the index scan is range scan.
         * if all scanType are PREFIX, means the index scan is prefix scan.
         */
        for (const auto &colHint : columnHints_) {
            if (colHint.get_scan_type() == cpp2::ScanType::RANGE) {
                isRangeScan_ = true;
                break;
            }
        }
    }

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        scanPair_ = scanStr(partId);
        std::unique_ptr<kvstore::KVIterator> iter;
        ret = isRangeScan_
              ? planContext_->env_->kvstore_->range(planContext_->spaceId_, partId,
                  scanPair_.first, scanPair_.second, &iter)
              : planContext_->env_->kvstore_->prefix(planContext_->spaceId_, partId,
                  scanPair_.first, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED && iter && iter->valid()) {
            planContext_->isEdge_
            ? iter_.reset(new EdgeIndexIterator(std::move(iter), planContext_->vIdLen_))
            : iter_.reset(new VertexIndexIterator(std::move(iter), planContext_->vIdLen_));
        } else {
            iter_.reset();
            return ret;
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    IndexIterator* iterator() {
        return iter_.get();
    }

    std::vector<kvstore::KV> moveData() {
        data_.clear();
        while (!!iter_ && iter_->valid()) {
            data_.emplace_back(iter_->key(), "");
            iter_->next();
        }
        return std::move(data_);
    }

private:
    std::pair<std::string, std::string> scanStr(PartitionID partId) {
        if (isRangeScan_) {
            return getRangeStr(partId);
        } else {
            return getPrefixStr(partId);
        }
    }

    std::pair<std::string, std::string> getPrefixStr(PartitionID partId) {
        std::string prefix;
        prefix.append(IndexKeyUtils::indexPrefix(partId, indexId_));
        for (auto& col : columnHints_) {
            prefix.append(IndexKeyUtils::encodeValue(col.get_begin_value()));
        }
        return {prefix, ""};
    }

    std::pair<std::string, std::string>  getRangeStr(PartitionID partId) {
        std::string start, end;
        start.append(IndexKeyUtils::indexPrefix(partId, indexId_));
        end.append(IndexKeyUtils::indexPrefix(partId, indexId_));
        for (auto& col : columnHints_) {
            if (col.get_scan_type() == cpp2::ScanType::PREFIX) {
                start.append(IndexKeyUtils::encodeValue(col.get_begin_value()));
                end.append(IndexKeyUtils::encodeValue(col.get_begin_value()));
            } else {
                start.append(IndexKeyUtils::encodeValue(col.get_begin_value()));
                end.append(IndexKeyUtils::encodeValue(col.get_end_value()));
            }
        }
        return {start, end};
    }

private:
    PlanContext*                        planContext_;
    IndexID                             indexId_;
    bool                                isRangeScan_{false};
    std::unique_ptr<IndexIterator>      iter_;
    std::pair<std::string, std::string> scanPair_;
    std::vector<cpp2::IndexColumnHint>  columnHints_;
    std::vector<kvstore::KV>            data_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_INDEXSCANNODE_H_
