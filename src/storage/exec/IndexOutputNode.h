/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef STORAGE_EXEC_INDEXOUTPUTNODE_H_
#define STORAGE_EXEC_INDEXOUTPUTNODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {

template<typename T>
class IndexOutputNode final : public RelNode<T> {
public:
    using RelNode<T>::execute;

    enum class IndexResultType : int8_t {
        kEdgeFromIndexScan,
        kEdgeFromIndexFilter,
        kEdgeFromDataScan,
        kEdgeFromDataFilter,
        kVertexFromIndexScan,
        kVertexFromIndexFilter,
        kVertexFromDataScan,
        kVertexFromDataFilter,
    };

    IndexOutputNode(nebula::DataSet* result,
                    PlanContext* planCtx,
                    IndexScanNode<T>* indexScanNode,
                    bool hasNullableCol,
                    const std::vector<meta::cpp2::ColumnDef>& fields)
        : result_(result)
        , planContext_(planCtx)
        , indexScanNode_(indexScanNode)
        , hasNullableCol_(hasNullableCol)
        , fields_(fields) {
        type_ = planContext_->isEdge_
                ? IndexResultType::kEdgeFromIndexScan
                : IndexResultType::kVertexFromIndexScan;
    }

    IndexOutputNode(nebula::DataSet* result,
                    PlanContext* planCtx,
                    IndexEdgeNode<T>* indexEdgeNode)
        : result_(result)
        , planContext_(planCtx)
        , indexEdgeNode_(indexEdgeNode) {
        type_ = IndexResultType::kEdgeFromDataScan;
    }

    IndexOutputNode(nebula::DataSet* result,
                    PlanContext* planCtx,
                    IndexVertexNode<T>* indexVertexNode)
        : result_(result)
        , planContext_(planCtx)
        , indexVertexNode_(indexVertexNode) {
        type_ = IndexResultType::kVertexFromDataScan;
    }

    IndexOutputNode(nebula::DataSet* result,
                    PlanContext* planCtx,
                    IndexFilterNode<T>* indexFilterNode,
                    bool indexFilter = false)
        : result_(result)
        , planContext_(planCtx)
        , indexFilterNode_(indexFilterNode) {
        hasNullableCol_ = indexFilterNode->hasNullableCol();
        fields_ = indexFilterNode_->indexCols();
        if (indexFilter) {
            type_ = planContext_->isEdge_
                    ? IndexResultType::kEdgeFromIndexFilter
                    : IndexResultType::kVertexFromIndexFilter;
        } else {
            type_ = planContext_->isEdge_
                    ? IndexResultType::kEdgeFromDataFilter
                    : IndexResultType::kVertexFromDataFilter;
        }
    }

    kvstore::ResultCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        switch (type_) {
            case IndexResultType::kEdgeFromIndexScan: {
                ret = collectResult(indexScanNode_->moveData());
                break;
            }
            case IndexResultType::kEdgeFromIndexFilter: {
                ret = collectResult(indexFilterNode_->moveData());
                break;
            }
            case IndexResultType::kEdgeFromDataScan: {
                ret = collectResult(indexEdgeNode_->moveData());
                break;
            }
            case IndexResultType::kEdgeFromDataFilter: {
                ret = collectResult(indexFilterNode_->moveData());
                break;
            }
            case IndexResultType::kVertexFromIndexScan: {
                ret = collectResult(indexScanNode_->moveData());
                break;
            }
            case IndexResultType::kVertexFromIndexFilter: {
                ret = collectResult(indexFilterNode_->moveData());
                break;
            }
            case IndexResultType::kVertexFromDataScan: {
                ret = collectResult(indexVertexNode_->moveData());
                break;
            }
            case IndexResultType::kVertexFromDataFilter: {
                ret = collectResult(indexFilterNode_->moveData());
                break;
            }
        }
        return ret;
    }

private:
    kvstore::ResultCode collectResult(const std::vector<kvstore::KV>& data) {
        kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
        switch (type_) {
            case IndexResultType::kEdgeFromIndexScan:
            case IndexResultType::kEdgeFromIndexFilter: {
                ret = edgeRowsFromIndex(data);
                break;
            }
            case IndexResultType::kEdgeFromDataScan:
            case IndexResultType::kEdgeFromDataFilter: {
                ret = edgeRowsFromData(data);
                break;
            }
            case IndexResultType::kVertexFromIndexScan:
            case IndexResultType::kVertexFromIndexFilter: {
                ret = vertexRowsFromIndex(data);
                break;
            }
            case IndexResultType::kVertexFromDataScan:
            case IndexResultType::kVertexFromDataFilter: {
                ret = vertexRowsFromData(data);
                break;
            }
        }
        return ret;
    }

    kvstore::ResultCode vertexRowsFromData(const std::vector<kvstore::KV>& data) {
        const auto* schema = type_ == IndexResultType::kVertexFromDataScan
                             ? indexVertexNode_->getSchema()
                             : indexFilterNode_->getSchema();
        if (schema == nullptr) {
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        auto returnCols = result_->colNames;
        for (const auto& val : data) {
            Row row;
            auto reader = RowReaderWrapper::getRowReader(schema, val.second);
            if (!reader) {
                VLOG(1) << "Can't get tag reader";
                return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
            }
            for (const auto& colName : returnCols) {
                if (colName == kVid) {
                    auto vId = NebulaKeyUtils::getVertexId(planContext_->vIdLen_, val.first);
                    addVidToRow(vId, row);
                } else if (colName == kTag) {
                    row.emplace_back(planContext_->tagId_);
                } else {
                    auto v = reader->getValueByName(colName);
                    row.emplace_back(std::move(v));
                }
            }
            result_->rows.emplace_back(std::move(row));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode vertexRowsFromIndex(const std::vector<kvstore::KV>& data) {
        auto returnCols = result_->colNames;
        for (const auto& val : data) {
            Row row;
            for (const auto& colName : returnCols) {
                if (colName == kVid) {
                    auto vId = IndexKeyUtils::getIndexVertexID(planContext_->vIdLen_, val.first);
                    addVidToRow(vId, row);
                } else if (colName == kTag) {
                    row.emplace_back(planContext_->tagId_);
                } else {
                    auto v = IndexKeyUtils::getValueFromIndexKey(planContext_->vIdLen_,
                                                                 val.first,
                                                                 colName,
                                                                 fields_,
                                                                 false,
                                                                 hasNullableCol_);
                    row.emplace_back(std::move(v));
                }
            }
            result_->rows.emplace_back(std::move(row));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode edgeRowsFromData(const std::vector<kvstore::KV>& data) {
        const auto* schema = type_ == IndexResultType::kEdgeFromDataScan
                             ? indexEdgeNode_->getSchema()
                             : indexFilterNode_->getSchema();
        if (schema == nullptr) {
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }
        auto returnCols = result_->colNames;
        for (const auto& val : data) {
            Row row;
            auto reader = RowReaderWrapper::getRowReader(schema, val.second);
            if (!reader) {
                VLOG(1) << "Can't get tag reader";
                return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
            }
            for (const auto& colName : returnCols) {
                if (colName == kSrc) {
                    auto src = NebulaKeyUtils::getSrcId(planContext_->vIdLen_, val.first);
                    addVidToRow(src, row);
                } else if (colName == kType) {
                    row.emplace_back(planContext_->edgeType_);
                } else if (colName == kRank) {
                    row.emplace_back(NebulaKeyUtils::getRank(planContext_->vIdLen_, val.first));
                } else if (colName == kDst) {
                    auto dst = NebulaKeyUtils::getDstId(planContext_->vIdLen_, val.first);
                    addVidToRow(dst, row);
                } else {
                    auto v = reader->getValueByName(colName);
                    row.emplace_back(std::move(v));
                }
            }
            result_->rows.emplace_back(std::move(row));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode edgeRowsFromIndex(const std::vector<kvstore::KV>& data) {
        auto returnCols = result_->colNames;
        for (const auto& val : data) {
            Row row;
            for (const auto& colName : returnCols) {
                if (colName == kSrc) {
                    auto src = IndexKeyUtils::getIndexSrcId(planContext_->vIdLen_, val.first);
                    addVidToRow(src, row);
                } else if (colName == kType) {
                    row.emplace_back(planContext_->edgeType_);
                } else if (colName == kRank) {
                    row.emplace_back(IndexKeyUtils::getIndexRank(planContext_->vIdLen_, val.first));
                } else if (colName == kDst) {
                    auto dst = IndexKeyUtils::getIndexDstId(planContext_->vIdLen_, val.first);
                    addVidToRow(dst, row);
                } else {
                    auto v = IndexKeyUtils::getValueFromIndexKey(planContext_->vIdLen_,
                                                                 val.first,
                                                                 colName,
                                                                 fields_,
                                                                 true,
                                                                 hasNullableCol_);
                    row.emplace_back(std::move(v));
                }
            }
            result_->rows.emplace_back(std::move(row));
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    void addVidToRow(VertexIDSlice vId, Row& row) {
        if (planContext_->isIntId_) {
            row.emplace_back(*reinterpret_cast<const int64_t*>(vId.data()));
        } else {
            row.emplace_back(vId.subpiece(0, vId.find_first_of('\0')).toString());
        }
    }

private:
    nebula::DataSet*                                  result_;
    PlanContext*                                      planContext_;
    IndexResultType                                   type_;
    IndexScanNode<T>*                                 indexScanNode_{nullptr};
    IndexEdgeNode<T>*                                 indexEdgeNode_{nullptr};
    IndexVertexNode<T>*                               indexVertexNode_{nullptr};
    IndexFilterNode<T>*                               indexFilterNode_{nullptr};
    bool                                              hasNullableCol_{};
    std::vector<meta::cpp2::ColumnDef>                fields_;
};

}  // namespace storage
}  // namespace nebula

#endif   // STORAGE_EXEC_INDEXOUTPUTNODE_H_
