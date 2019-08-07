/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/InterimResult.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace graph {

InterimResult::InterimResult(std::unique_ptr<RowSetWriter> rsWriter) {
    rsWriter_ = std::move(rsWriter);
    rsReader_ = std::make_unique<RowSetReader>(rsWriter_->schema(), rsWriter_->data());
}


InterimResult::InterimResult(std::vector<VertexID> vids) {
    vids_ = std::move(vids);
}


StatusOr<std::vector<VertexID>> InterimResult::getVIDs(const std::string &col) const {
    if (!vids_.empty()) {
        DCHECK(rsReader_ == nullptr);
        return vids_;
    }
    DCHECK(rsReader_ != nullptr);
    std::vector<VertexID> result;
    auto iter = rsReader_->begin();
    while (iter) {
        VertexID vid;
        auto rc = iter->getVid(col, vid);
        if (rc != ResultType::SUCCEEDED) {
            return Status::Error("Column `%s' not found", col.c_str());
        }
        result.emplace_back(vid);
        ++iter;
    }
    return result;
}

std::vector<cpp2::RowValue> InterimResult::getRows() const {
    DCHECK(rsReader_ != nullptr);
    auto schema = rsReader_->schema();
    auto columnCnt = schema->getNumFields();
    std::vector<cpp2::RowValue> rows;
    folly::StringPiece piece;
    using nebula::cpp2::SupportedType;
    auto rowIter = rsReader_->begin();
    while (rowIter) {
        std::vector<cpp2::ColumnValue> row;
        row.reserve(columnCnt);
        auto fieldIter = schema->begin();
        while (fieldIter) {
            auto type = fieldIter->getType().type;
            auto field = fieldIter->getName();
            row.emplace_back();
            switch (type) {
                case SupportedType::VID: {
                    int64_t v;
                    auto rc = rowIter->getVid(field, v);
                    CHECK(rc == ResultType::SUCCEEDED);
                    row.back().set_integer(v);
                    break;
                }
                case SupportedType::DOUBLE: {
                    double v;
                    auto rc = rowIter->getDouble(field, v);
                    CHECK(rc == ResultType::SUCCEEDED);
                    row.back().set_double_precision(v);
                    break;
                }
                case SupportedType::BOOL: {
                    bool v;
                    auto rc = rowIter->getBool(field, v);
                    CHECK(rc == ResultType::SUCCEEDED);
                    row.back().set_bool_val(v);
                    break;
                }
                case SupportedType::STRING: {
                    auto rc = rowIter->getString(field, piece);
                    CHECK(rc == ResultType::SUCCEEDED);
                    row.back().set_str(piece.toString());
                    break;
                }
                default:
                    LOG(FATAL) << "Unknown Type: " << static_cast<int32_t>(type);
            }
            ++fieldIter;
        }
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
        ++rowIter;
    }
    return rows;
}


std::unique_ptr<InterimResult::InterimResultIndex>
InterimResult::buildIndex(const std::string &vidColumn) const {
    using nebula::cpp2::SupportedType;
    std::unique_ptr<InterimResultIndex> index;

    DCHECK(rsReader_ != nullptr);
    auto schema = rsReader_->schema();
    auto columnCnt = schema->getNumFields();
    uint32_t vidIndex = 0u;

    index = std::make_unique<InterimResultIndex>();
    for (auto i = 0u; i < columnCnt; i++) {
        auto name = schema->getFieldName(i);
        if (vidColumn == name) {
            if (schema->getFieldType(i).type != SupportedType::VID) {
                return nullptr;
            }
            vidIndex = i;
        }
        index->columnToIndex_[name] = i;
    }

    auto rowIter = rsReader_->begin();
    auto rowIndex = 0u;
    while (rowIter) {
        Row row;
        row.reserve(columnCnt);
        for (auto i = 0u; i < columnCnt; i++) {
            auto type = schema->getFieldType(i).type;
            switch (type) {
                case SupportedType::VID: {
                    int64_t v;
                    auto rc = rowIter->getVid(i, v);
                    CHECK(rc == ResultType::SUCCEEDED);
                    if (i == vidIndex) {
                        index->vidToRowIndex_[v] = rowIndex++;
                    }
                    row.emplace_back(v);
                    break;
                }
                case SupportedType::DOUBLE: {
                    double v;
                    auto rc = rowIter->getDouble(i, v);
                    CHECK(rc == ResultType::SUCCEEDED);
                    row.emplace_back(v);
                    break;
                }
                case SupportedType::BOOL: {
                    bool v;
                    auto rc = rowIter->getBool(i, v);
                    CHECK(rc == ResultType::SUCCEEDED);
                    row.emplace_back(v);
                    break;
                }
                case SupportedType::STRING: {
                    folly::StringPiece piece;
                    auto rc = rowIter->getString(i, piece);
                    CHECK(rc == ResultType::SUCCEEDED);
                    row.emplace_back(piece.toString());
                    break;
                }
                default:
                    LOG(FATAL) << "Unknown Type: " << static_cast<int32_t>(type);
            }
        }
        index->rows_.emplace_back(std::move(row));
        ++rowIter;
    }

    return index;
}


VariantType InterimResult::InterimResultIndex::getColumnWithVID(VertexID id,
                                                                const std::string &col) const {
    uint32_t rowIndex = 0;
    {
        auto iter = vidToRowIndex_.find(id);
        DCHECK(iter != vidToRowIndex_.end());
        rowIndex = iter->second;
    }
    uint32_t columnIndex = 0;
    {
        auto iter = columnToIndex_.find(col);
        // TODO(dutor) should report error
        DCHECK(iter != columnToIndex_.end());
        columnIndex = iter->second;
    }
    return rows_[rowIndex][columnIndex];
}


}   // namespace graph
}   // namespace nebula
