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

constexpr char NotSupported[] = "Type not supported yet";

InterimResult::InterimResult(std::vector<VertexID> vids) {
    vids_ = std::move(vids);
}

InterimResult::InterimResult(std::vector<std::string> &&colNames) {
    colNames_ = std::move(colNames);
}

void InterimResult::setInterim(std::unique_ptr<RowSetWriter> rsWriter) {
    rsWriter_ = std::move(rsWriter);
    rsReader_ = std::make_unique<RowSetReader>(rsWriter_->schema(), rsWriter_->data());
}

StatusOr<std::vector<VertexID>> InterimResult::getVIDs(const std::string &col) const {
    if (!vids_.empty()) {
        DCHECK(rsReader_ == nullptr);
        return vids_;
    }
    if (!hasData()) {
        return Status::Error("Interim has no data.");
    }
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

StatusOr<std::vector<VertexID>> InterimResult::getDistinctVIDs(const std::string &col) const {
    if (!vids_.empty()) {
        DCHECK(rsReader_ == nullptr);
        return vids_;
    }
    if (!hasData()) {
        return Status::Error("Interim has no data.");
    }
    std::unordered_set<VertexID> uniq;
    auto iter = rsReader_->begin();
    while (iter) {
        VertexID vid;
        auto rc = iter->getVid(col, vid);
        if (rc != ResultType::SUCCEEDED) {
            return Status::Error("Column `%s' not found", col.c_str());
        }
        uniq.emplace(vid);
        ++iter;
    }
    std::vector<VertexID> result(uniq.begin(), uniq.end());
    return result;
}

StatusOr<std::vector<cpp2::RowValue>> InterimResult::getRows() const {
    if (!hasData()) {
        return Status::Error("Interim has no data.");
    }
    auto schema = rsReader_->schema();
    auto columnCnt = schema->getNumFields();
    VLOG(1) << "columnCnt: " << columnCnt;
    std::vector<cpp2::RowValue> rows;
    folly::StringPiece piece;
    using nebula::cpp2::SupportedType;
    auto rowIter = rsReader_->begin();
    while (rowIter) {
        std::vector<cpp2::ColumnValue> row;
        row.reserve(columnCnt);
        auto fieldIter = schema->begin();
        int64_t cnt = 0;
        while (fieldIter) {
            ++cnt;
            auto type = fieldIter->getType().type;
            auto field = fieldIter->getName();
            VLOG(1) << "field: " << field << " type: " << static_cast<int64_t>(type);
            row.emplace_back();
            switch (type) {
                case SupportedType::VID: {
                    int64_t v;
                    auto rc = rowIter->getVid(field, v);
                    if (rc != ResultType::SUCCEEDED) {
                        return Status::Error(
                                "Get vid from interim failed, field: %s, index: %ld.",
                                field, cnt);
                    }
                    row.back().set_id(v);
                    break;
                }
                case SupportedType::DOUBLE: {
                    double v;
                    auto rc = rowIter->getDouble(field, v);
                    if (rc != ResultType::SUCCEEDED) {
                        return Status::Error(
                                "Get double from interim failed, field: %s, index: %ld.",
                                field, cnt);
                    }
                    row.back().set_double_precision(v);
                    break;
                }
                case SupportedType::BOOL: {
                    bool v;
                    auto rc = rowIter->getBool(field, v);
                    if (rc != ResultType::SUCCEEDED) {
                        return Status::Error(
                                "Get bool from interim failed, field: %s, index: %ld.",
                                field, cnt);
                    }
                    row.back().set_bool_val(v);
                    break;
                }
                case SupportedType::STRING: {
                    auto rc = rowIter->getString(field, piece);
                    if (rc != ResultType::SUCCEEDED) {
                        return Status::Error(
                                "Get string from interim failed, field: %s, index: %ld.",
                                field, cnt);
                    }
                    row.back().set_str(piece.toString());
                    break;
                }
                case SupportedType::INT: {
                    int64_t v;
                    auto rc = rowIter->getInt(field, v);
                    if (rc != ResultType::SUCCEEDED) {
                        return Status::Error(
                                "Get int from interim failed, field: %s, index: %ld.",
                                field, cnt);
                    }
                    row.back().set_integer(v);
                    break;
                }
                case SupportedType::TIMESTAMP: {
                    int64_t v;
                    auto rc = rowIter->getInt(field, v);
                    if (rc != ResultType::SUCCEEDED) {
                        return Status::Error(
                                "Get timestamp from interim failed, field: %s, index: %ld.",
                                field, cnt);
                    }
                    row.back().set_timestamp(v);
                    break;
                }
                default:
                    std::string err =
                        folly::sformat("Unknown Type: %d", static_cast<int32_t>(type));
                    LOG(ERROR) << err;
                    return Status::Error(err);
            }
            ++fieldIter;
        }
        rows.emplace_back();
        rows.back().set_columns(std::move(row));
        ++rowIter;
    }
    return rows;
}

StatusOr<std::unique_ptr<InterimResult::InterimResultIndex>>
InterimResult::buildIndex(const std::string &vidColumn) const {
    using nebula::cpp2::SupportedType;
    std::unique_ptr<InterimResultIndex> index;

    if (!hasData()) {
        return Status::Error("Interim has no data.");
    }
    auto schema = rsReader_->schema();
    auto columnCnt = schema->getNumFields();
    uint32_t vidIndex = 0u;

    index = std::make_unique<InterimResultIndex>();
    for (auto i = 0u; i < columnCnt; i++) {
        auto name = schema->getFieldName(i);
        if (vidColumn == name) {
            VLOG(1) << "col name: " << vidColumn << ", col index: " << i;
            if (schema->getFieldType(i).type != SupportedType::VID) {
                return Status::Error(
                        "Build internal index for input data failed. "
                        "The specific vid column `%s' is not type of VID, column index: %ul.",
                        vidColumn.c_str(), i);
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
                    if (rc != ResultType::SUCCEEDED) {
                        return Status::Error("Get vid from interim failed.");
                    }
                    if (i == vidIndex) {
                        index->vidToRowIndex_.emplace(v, rowIndex++);
                    }
                    row.emplace_back(v);
                    break;
                }
                case SupportedType::DOUBLE: {
                    double v;
                    auto rc = rowIter->getDouble(i, v);
                    if (rc != ResultType::SUCCEEDED) {
                        return Status::Error("Get double from interim failed.");
                    }
                    row.emplace_back(v);
                    break;
                }
                case SupportedType::BOOL: {
                    bool v;
                    auto rc = rowIter->getBool(i, v);
                    if (rc != ResultType::SUCCEEDED) {
                        return Status::Error("Get bool from interim failed.");
                    }
                    row.emplace_back(v);
                    break;
                }
                case SupportedType::STRING: {
                    folly::StringPiece piece;
                    auto rc = rowIter->getString(i, piece);
                    if (rc != ResultType::SUCCEEDED) {
                        return Status::Error("Get string from interim failed.");
                    }
                    row.emplace_back(piece.toString());
                    break;
                }
                case SupportedType::INT:
                case SupportedType::TIMESTAMP: {
                    int64_t v;
                    auto rc = rowIter->getInt(i, v);
                    if (rc != ResultType::SUCCEEDED) {
                        return Status::Error("Get int from interim failed.");
                    }
                    row.emplace_back(v);
                    break;
                }
                default:
                    std::string err =
                        folly::sformat("Unknown Type: %d", static_cast<int32_t>(type));
                    LOG(ERROR) << err;
                    return Status::Error(err);
            }
        }
        index->rows_.emplace_back(std::move(row));
        ++rowIter;
    }
    index->schema_ = schema;
    return index;
}

OptVariantType
InterimResult::InterimResultIndex::getColumnWithRow(std::size_t row, const std::string &col) const {
    if (row >= rows_.size()) {
        return Status::Error("Out of range");
    }
    uint32_t columnIndex = 0;
    {
        auto iter = columnToIndex_.find(col);
        if (iter == columnToIndex_.end()) {
            LOG(ERROR) << "Prop `" << col << "' not found";
            return Status::Error("Prop `%s' not found", col.c_str());
        }
        columnIndex = iter->second;
    }
    return rows_[row][columnIndex];
}

nebula::cpp2::SupportedType InterimResult::getColumnType(
    const std::string &col) const {
    auto schema = rsReader_->schema();
    if (schema == nullptr) {
        return nebula::cpp2::SupportedType::UNKNOWN;
    }
    auto type = schema->getFieldType(col);
    return type.type;
}


Status InterimResult::castTo(cpp2::ColumnValue *col,
                             const nebula::cpp2::SupportedType &type) {
    using nebula::cpp2::SupportedType;
    switch (type) {
        case SupportedType::VID:
            return castToVid(col);
        case SupportedType::INT:
            return castToInt(col);
        case SupportedType::TIMESTAMP:
            return castToTimestamp(col);
        case SupportedType::DOUBLE:
            return castToDouble(col);
        case SupportedType::BOOL:
            return castToBool(col);
        case SupportedType::STRING:
            return castToStr(col);
        default:
            // Notice: if we implement some other type,
            // we should update here.
            LOG(ERROR) << NotSupported << static_cast<int32_t>(type);
            return Status::Error(NotSupported);
    }
}

Status InterimResult::castToInt(cpp2::ColumnValue *col) {
    switch (col->getType()) {
        case cpp2::ColumnValue::Type::integer:
            break;
        case cpp2::ColumnValue::Type::id:
            col->set_integer(col->get_id());
            break;
        case cpp2::ColumnValue::Type::timestamp:
            col->set_integer(col->get_timestamp());
            break;
        case cpp2::ColumnValue::Type::double_precision: {
            auto d2i = static_cast<int64_t>(col->get_double_precision());
            col->set_integer(d2i);
            break;
        }
        case cpp2::ColumnValue::Type::bool_val: {
            auto b2i = static_cast<int64_t>(col->get_bool_val());
            col->set_integer(b2i);
            break;
        }
        case cpp2::ColumnValue::Type::str: {
            auto r = folly::tryTo<int64_t>(col->get_str());
            if (r.hasValue()) {
                col->set_integer(r.value());
                break;
            } else {
                return Status::Error(
                    "Casting from string `%s' to int failed.", col->get_str().c_str());
            }
        }
        default:
            LOG(ERROR) << NotSupported << static_cast<int32_t>(col->getType());
            return Status::Error(NotSupported);
    }
    return Status::OK();
}

Status InterimResult::castToVid(cpp2::ColumnValue *col) {
    switch (col->getType()) {
        case cpp2::ColumnValue::Type::id:
            break;
        case cpp2::ColumnValue::Type::integer:
            col->set_id(col->get_integer());
            break;
        case cpp2::ColumnValue::Type::timestamp:
            col->set_id(col->get_timestamp());
            break;
        case cpp2::ColumnValue::Type::double_precision: {
            auto d2i = static_cast<int64_t>(col->get_double_precision());
            col->set_id(d2i);
            break;
        }
        case cpp2::ColumnValue::Type::bool_val: {
            auto b2i = static_cast<int64_t>(col->get_bool_val());
            col->set_id(b2i);
            break;
        }
        case cpp2::ColumnValue::Type::str: {
            auto r = folly::tryTo<int64_t>(col->get_str());
            if (r.hasValue()) {
                col->set_id(r.value());
                break;
            } else {
                return Status::Error(
                    "Casting from string %s to vid failed.", col->get_str().c_str());
            }
        }
        default:
            LOG(ERROR) << NotSupported << static_cast<int32_t>(col->getType());
            return Status::Error(NotSupported);
    }
    return Status::OK();
}

Status InterimResult::castToTimestamp(cpp2::ColumnValue *col) {
    switch (col->getType()) {
        case cpp2::ColumnValue::Type::timestamp:
            break;
        case cpp2::ColumnValue::Type::integer:
            col->set_timestamp(col->get_integer());
            break;
        case cpp2::ColumnValue::Type::id:
            col->set_timestamp(col->get_id());
            break;
        case cpp2::ColumnValue::Type::double_precision: {
            auto d2i = static_cast<int64_t>(col->get_double_precision());
            col->set_timestamp(d2i);
            break;
        }
        case cpp2::ColumnValue::Type::bool_val: {
            auto b2i = static_cast<int64_t>(col->get_bool_val());
            col->set_timestamp(b2i);
            break;
        }
        case cpp2::ColumnValue::Type::str: {
            auto r = folly::tryTo<int64_t>(col->get_str());
            if (r.hasValue()) {
                col->set_timestamp(r.value());
                break;
            } else {
                return Status::Error(
                    "Casting from string %s to timestamp failed.", col->get_str().c_str());
            }
        }
        default:
            LOG(ERROR) << NotSupported << static_cast<int32_t>(col->getType());
            return Status::Error(NotSupported);
    }
    return Status::OK();
}

Status InterimResult::castToDouble(cpp2::ColumnValue *col) {
    switch (col->getType()) {
        case cpp2::ColumnValue::Type::id: {
            auto i2d = static_cast<double>(col->get_id());
            col->set_double_precision(i2d);
            break;
        }
        case cpp2::ColumnValue::Type::integer: {
            auto i2d = static_cast<double>(col->get_integer());
            col->set_double_precision(i2d);
            break;
        }
        case cpp2::ColumnValue::Type::double_precision:
            break;
        case cpp2::ColumnValue::Type::bool_val: {
            auto b2d = static_cast<double>(col->get_bool_val());
            col->set_double_precision(b2d);
            break;
        }
        case cpp2::ColumnValue::Type::str: {
            auto r = folly::tryTo<double>(col->get_str());
            if (r.hasValue()) {
                col->set_double_precision(r.value());
                break;
            } else {
                return Status::Error(
                    "Casting from string %s to double failed.", col->get_str().c_str());
            }
        }
        case cpp2::ColumnValue::Type::timestamp: {
            auto i2d = static_cast<double>(col->get_timestamp());
            col->set_double_precision(i2d);
            break;
        }
        default:
            LOG(ERROR) << NotSupported << static_cast<int32_t>(col->getType());
            return Status::Error(NotSupported);
    }
    return Status::OK();
}

Status InterimResult::castToBool(cpp2::ColumnValue *col) {
    switch (col->getType()) {
        case cpp2::ColumnValue::Type::id: {
            auto i2b = col->get_id() != 0;
            col->set_bool_val(i2b);
            break;
        }
        case cpp2::ColumnValue::Type::integer: {
            auto i2b = col->get_integer() != 0;
            col->set_bool_val(i2b);
            break;
        }
        case cpp2::ColumnValue::Type::double_precision: {
            auto d2b = col->get_double_precision() != 0.0;
            col->set_bool_val(d2b);
            break;
        }
        case cpp2::ColumnValue::Type::bool_val:
            break;
        case cpp2::ColumnValue::Type::str: {
            auto s2b = col->get_str().empty();
            col->set_bool_val(s2b);
            break;
        }
        case cpp2::ColumnValue::Type::timestamp: {
            auto i2b = col->get_timestamp() != 0;
            col->set_bool_val(i2b);
            break;
        }
        default:
            LOG(ERROR) << NotSupported << static_cast<int32_t>(col->getType());
            return Status::Error(NotSupported);
    }
    return Status::OK();
}

Status InterimResult::castToStr(cpp2::ColumnValue *col) {
    switch (col->getType()) {
        case cpp2::ColumnValue::Type::id: {
            auto i2s = folly::to<std::string>(col->get_id());
            col->set_str(std::move(i2s));
            break;
        }
        case cpp2::ColumnValue::Type::integer: {
            auto i2s = folly::to<std::string>(col->get_integer());
            col->set_str(std::move(i2s));
            break;
        }
        case cpp2::ColumnValue::Type::double_precision: {
            auto d2s = folly::to<std::string>(col->get_double_precision());
            col->set_str(std::move(d2s));
            break;
        }
        case cpp2::ColumnValue::Type::bool_val: {
            auto b2s = folly::to<std::string>(col->get_bool_val());
            col->set_str(std::move(b2s));
            break;
        }
        case cpp2::ColumnValue::Type::str:
            break;
        case cpp2::ColumnValue::Type::timestamp: {
            auto i2s = folly::to<std::string>(col->get_timestamp());
            col->set_str(std::move(i2s));
            break;
        }
        default:
            LOG(ERROR) << NotSupported << static_cast<int32_t>(col->getType());
            return Status::Error(NotSupported);
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<InterimResult>>
InterimResult::getInterim(
            std::shared_ptr<const meta::SchemaProviderIf> resultSchema,
            std::vector<cpp2::RowValue> &rows) {
    auto rsWriter = std::make_unique<RowSetWriter>(resultSchema);
    for (auto &r : rows) {
        RowWriter writer(resultSchema);
        auto &cols = r.get_columns();
        for (auto &col : cols) {
            switch (col.getType()) {
                case cpp2::ColumnValue::Type::id:
                    writer << col.get_id();
                    break;
                case cpp2::ColumnValue::Type::integer:
                    writer << col.get_integer();
                    break;
                case cpp2::ColumnValue::Type::double_precision:
                    writer << col.get_double_precision();
                    break;
                case cpp2::ColumnValue::Type::bool_val:
                    writer << col.get_bool_val();
                    break;
                case cpp2::ColumnValue::Type::str:
                    writer << col.get_str();
                    break;
                case cpp2::ColumnValue::Type::timestamp:
                    writer << col.get_timestamp();
                    break;
                default:
                    LOG(ERROR) << NotSupported << static_cast<int32_t>(col.getType());
                    return Status::Error(NotSupported);
            }
        }
        rsWriter->addRow(writer);
    }

    std::vector<std::string> colNames;
    auto iter = resultSchema->begin();
    while (iter) {
        colNames.emplace_back(iter->getName());
        ++iter;
    }
    auto result = std::make_unique<InterimResult>(std::move(colNames));
    result->setInterim(std::move(rsWriter));
    return result;
}

Status InterimResult::applyTo(std::function<Status(const RowReader *reader)> visitor,
                              int64_t limit) const {
    auto status = Status::OK();
    auto iter = rsReader_->begin();
    while (iter && (limit > 0)) {
        status = visitor(&*iter);
        if (!status.ok()) {
            break;
        }
        --limit;
        ++iter;
    }
    return status;
}

Status InterimResult::getResultWriter(const std::vector<cpp2::RowValue> &rows,
                                      RowSetWriter *rsWriter) {
    if (rsWriter == nullptr) {
        return Status::Error("rsWriter is nullptr");
    }
    using Type = cpp2::ColumnValue::Type;
    for (auto &row : rows) {
        RowWriter writer(rsWriter->schema());
        auto columns = row.get_columns();
        for (auto &column : columns) {
            switch (column.getType()) {
                case Type::id:
                    writer << column.get_id();
                    break;
                case Type::integer:
                    writer << column.get_integer();
                    break;
                case Type::double_precision:
                    writer << column.get_double_precision();
                    break;
                case Type::bool_val:
                    writer << column.get_bool_val();
                    break;
                case Type::str:
                    writer << column.get_str();
                    break;
                case Type::timestamp:
                    writer << column.get_timestamp();
                    break;
                default:
                    LOG(ERROR) << "Not Support: " << column.getType();
                    return Status::Error("Not Support: %d", column.getType());
            }
        }
        rsWriter->addRow(writer);
    }

    return Status::OK();
}
}   // namespace graph
}   // namespace nebula
