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
                    RunTimeContext* context,
                    IndexScanNode<T>* indexScanNode,
                    bool hasNullableCol,
                    const std::vector<meta::cpp2::ColumnDef>& fields)
        : result_(result)
        , context_(context)
        , indexScanNode_(indexScanNode)
        , hasNullableCol_(hasNullableCol)
        , fields_(fields) {
        type_ = context_->isEdge()
                ? IndexResultType::kEdgeFromIndexScan
                : IndexResultType::kVertexFromIndexScan;
    }

    IndexOutputNode(nebula::DataSet* result,
                    RunTimeContext* context,
                    IndexEdgeNode<T>* indexEdgeNode)
        : result_(result)
        , context_(context)
        , indexEdgeNode_(indexEdgeNode) {
        type_ = IndexResultType::kEdgeFromDataScan;
    }

    IndexOutputNode(nebula::DataSet* result,
                    RunTimeContext* context,
                    IndexVertexNode<T>* indexVertexNode)
        : result_(result)
        , context_(context)
        , indexVertexNode_(indexVertexNode) {
        type_ = IndexResultType::kVertexFromDataScan;
    }

    IndexOutputNode(nebula::DataSet* result,
                    RunTimeContext* context,
                    IndexFilterNode<T>* indexFilterNode,
                    bool indexFilter = false)
        : result_(result)
        , context_(context)
        , indexFilterNode_(indexFilterNode) {
        hasNullableCol_ = indexFilterNode->hasNullableCol();
        fields_ = indexFilterNode_->indexCols();
        if (indexFilter) {
            type_ = context_->isEdge()
                    ? IndexResultType::kEdgeFromIndexFilter
                    : IndexResultType::kVertexFromIndexFilter;
        } else {
            type_ = context_->isEdge()
                    ? IndexResultType::kEdgeFromDataFilter
                    : IndexResultType::kVertexFromDataFilter;
        }
    }

    nebula::cpp2::ErrorCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
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
    nebula::cpp2::ErrorCode collectResult(const std::vector<kvstore::KV>& data) {
        auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
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

    nebula::cpp2::ErrorCode vertexRowsFromData(const std::vector<kvstore::KV>& data) {
        const auto& schemas = type_ == IndexResultType::kVertexFromDataScan
                              ? indexVertexNode_->getSchemas()
                              : indexFilterNode_->getSchemas();
        if (schemas.empty()) {
            return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        for (const auto& val : data) {
            Row row;
            auto reader = RowReaderWrapper::getRowReader(schemas, val.second);
            if (!reader) {
                VLOG(1) << "Can't get tag reader";
                return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
            }
            for (const auto& col : result_->colNames) {
                auto ret = addIndexValue(row, reader.get(), val, col, schemas.back().get());
                if (!ret.ok()) {
                    return nebula::cpp2::ErrorCode::E_INVALID_DATA;
                }
            }
            result_->rows.emplace_back(std::move(row));
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    nebula::cpp2::ErrorCode vertexRowsFromIndex(const std::vector<kvstore::KV>& data) {
        for (const auto& val : data) {
            Row row;
            for (const auto& col : result_->colNames) {
                auto ret = addIndexValue(row, val, col);
                if (!ret.ok()) {
                    return nebula::cpp2::ErrorCode::E_INVALID_DATA;
                }
            }
            result_->rows.emplace_back(std::move(row));
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    nebula::cpp2::ErrorCode edgeRowsFromData(const std::vector<kvstore::KV>& data) {
        const auto& schemas = type_ == IndexResultType::kEdgeFromDataScan
                              ? indexEdgeNode_->getSchemas()
                              : indexFilterNode_->getSchemas();
        if (schemas.empty()) {
            return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        for (const auto& val : data) {
            Row row;
            auto reader = RowReaderWrapper::getRowReader(schemas, val.second);
            if (!reader) {
                VLOG(1) << "Can't get tag reader";
                return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
            }
            for (const auto& col : result_->colNames) {
                auto ret = addIndexValue(row, reader.get(), val, col, schemas.back().get());
                if (!ret.ok()) {
                    return nebula::cpp2::ErrorCode::E_INVALID_DATA;
                }
            }
            result_->rows.emplace_back(std::move(row));
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    nebula::cpp2::ErrorCode edgeRowsFromIndex(const std::vector<kvstore::KV>& data) {
        for (const auto& val : data) {
            Row row;
            for (const auto& col : result_->colNames) {
                auto ret = addIndexValue(row, val, col);
                if (!ret.ok()) {
                    return nebula::cpp2::ErrorCode::E_INVALID_DATA;
                }
            }
            result_->rows.emplace_back(std::move(row));
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    // Add the value by data val
    Status addIndexValue(Row& row, RowReader* reader,
                         const kvstore::KV& data, const std::string& col,
                         const meta::NebulaSchemaProvider* schema) {
        switch (QueryUtils::toReturnColType(col)) {
            case QueryUtils::ReturnColType::kVid : {
                auto vId = NebulaKeyUtils::getVertexId(context_->vIdLen(), data.first);
                if (context_->isIntId()) {
                    row.emplace_back(*reinterpret_cast<const int64_t*>(vId.data()));
                } else {
                    row.emplace_back(vId.subpiece(0, vId.find_first_of('\0')).toString());
                }
                break;
            }
            case QueryUtils::ReturnColType::kTag : {
                row.emplace_back(NebulaKeyUtils::getTagId(context_->vIdLen(), data.first));
                break;
            }
            case QueryUtils::ReturnColType::kSrc : {
                auto src = NebulaKeyUtils::getSrcId(context_->vIdLen(), data.first);
                if (context_->isIntId()) {
                    row.emplace_back(*reinterpret_cast<const int64_t*>(src.data()));
                } else {
                    row.emplace_back(src.subpiece(0, src.find_first_of('\0')).toString());
                }
                break;
            }
            case QueryUtils::ReturnColType::kType : {
                row.emplace_back(NebulaKeyUtils::getEdgeType(context_->vIdLen(), data.first));
                break;
            }
            case QueryUtils::ReturnColType::kRank : {
                row.emplace_back(NebulaKeyUtils::getRank(context_->vIdLen(), data.first));
                break;
            }
            case QueryUtils::ReturnColType::kDst : {
                auto dst = NebulaKeyUtils::getDstId(context_->vIdLen(), data.first);
                if (context_->isIntId()) {
                    row.emplace_back(*reinterpret_cast<const int64_t*>(dst.data()));
                } else {
                    row.emplace_back(dst.subpiece(0, dst.find_first_of('\0')).toString());
                }
                break;
            }
            default: {
                auto retVal = QueryUtils::readValue(reader, col, schema);
                if (!retVal.ok()) {
                    VLOG(3) << "Bad value for field : " << col;
                    return retVal.status();
                }
                row.emplace_back(std::move(retVal.value()));
            }
        }
        return Status::OK();
    }

    // Add the value by index key
    Status addIndexValue(Row& row, const kvstore::KV& data, const std::string& col) {
        switch (QueryUtils::toReturnColType(col)) {
            case QueryUtils::ReturnColType::kVid : {
                auto vId = IndexKeyUtils::getIndexVertexID(context_->vIdLen(), data.first);
                if (context_->isIntId()) {
                    row.emplace_back(*reinterpret_cast<const int64_t*>(vId.data()));
                } else {
                    row.emplace_back(vId.subpiece(0, vId.find_first_of('\0')).toString());
                }
                break;
            }
            case QueryUtils::ReturnColType::kTag : {
                row.emplace_back(context_->tagId_);
                break;
            }
            case QueryUtils::ReturnColType::kSrc : {
                auto src = IndexKeyUtils::getIndexSrcId(context_->vIdLen(), data.first);
                if (context_->isIntId()) {
                    row.emplace_back(*reinterpret_cast<const int64_t*>(src.data()));
                } else {
                    row.emplace_back(src.subpiece(0, src.find_first_of('\0')).toString());
                }
                break;
            }
            case QueryUtils::ReturnColType::kType : {
                row.emplace_back(context_->edgeType_);
                break;
            }
            case QueryUtils::ReturnColType::kRank : {
                row.emplace_back(IndexKeyUtils::getIndexRank(context_->vIdLen(), data.first));
                break;
            }
            case QueryUtils::ReturnColType::kDst : {
                auto dst = IndexKeyUtils::getIndexDstId(context_->vIdLen(), data.first);
                if (context_->isIntId()) {
                    row.emplace_back(*reinterpret_cast<const int64_t*>(dst.data()));
                } else {
                    row.emplace_back(dst.subpiece(0, dst.find_first_of('\0')).toString());
                }
                break;
            }
            default: {
                auto v = IndexKeyUtils::getValueFromIndexKey(context_->vIdLen(),
                                                             data.first,
                                                             col,
                                                             fields_,
                                                             context_->isEdge(),
                                                             hasNullableCol_);
                row.emplace_back(std::move(v));
            }
        }
        return Status::OK();
    }

private:
    nebula::DataSet*                                  result_;
    RunTimeContext*                                   context_;
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
