/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_INTERIMRESULT_H_
#define GRAPH_INTERIMRESULT_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "filter/Expressions.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowSetWriter.h"
#include "dataman/SchemaWriter.h"

namespace nebula {
namespace graph {
/**
 * The intermediate form of execution result, used in pipeline and variable.
 */
class InterimResult final {
public:
    InterimResult() = default;
    ~InterimResult() = default;
    InterimResult(const InterimResult &) = delete;
    InterimResult& operator=(const InterimResult &) = delete;
    InterimResult(InterimResult &&) = default;
    InterimResult& operator=(InterimResult &&) = default;

    explicit InterimResult(std::vector<VertexID> vids);
    explicit InterimResult(std::vector<std::string> &&colNames);

    static StatusOr<std::unique_ptr<InterimResult>> getInterim(
            std::shared_ptr<const meta::SchemaProviderIf> resultSchema,
            std::vector<cpp2::RowValue> &rows);
    static Status castTo(cpp2::ColumnValue *col,
                         const nebula::cpp2::SupportedType &type);
    static Status castToInt(cpp2::ColumnValue *col);
    static Status castToVid(cpp2::ColumnValue *col);
    static Status castToTimestamp(cpp2::ColumnValue *col);
    static Status castToDouble(cpp2::ColumnValue *col);
    static Status castToBool(cpp2::ColumnValue *col);
    static Status castToStr(cpp2::ColumnValue *col);

    static Status getResultWriter(const std::vector<cpp2::RowValue> &rows,
                                  RowSetWriter *rsWriter);

    void setColNames(std::vector<std::string> &&colNames) {
        colNames_ = std::move(colNames);
    }

    void setInterim(std::unique_ptr<RowSetWriter> rsWriter);

    bool hasData() const {
        return (rsWriter_ != nullptr)
                && (!rsWriter_->data().empty())
                && (rsReader_ != nullptr);
    }

    std::shared_ptr<const meta::SchemaProviderIf> schema() const {
        if (!hasData()) {
            return nullptr;
        }
        return rsReader_->schema();
    }

    std::vector<std::string> getColNames() const {
        return colNames_;
    }

    StatusOr<std::vector<VertexID>> getVIDs(const std::string &col) const;

    StatusOr<std::vector<VertexID>> getDistinctVIDs(const std::string &col) const;

    StatusOr<std::vector<cpp2::RowValue>> getRows() const;

    class InterimResultIndex;
    StatusOr<std::unique_ptr<InterimResultIndex>>
    buildIndex(const std::string &vidColumn) const;

    Status applyTo(std::function<Status(const RowReader *reader)> visitor,
                   int64_t limit = INT64_MAX) const;

    nebula::cpp2::SupportedType getColumnType(const std::string &col) const;

    class InterimResultIndex final {
    public:
        OptVariantType getColumnWithRow(std::size_t row, const std::string &col) const;
        nebula::cpp2::SupportedType getColumnType(const std::string &col) const;
        auto rowsOfVid(VertexID id) {
            return vidToRowIndex_.equal_range(id);
        }

    private:
        friend class InterimResult;
        using Row = std::vector<VariantType>;
        std::vector<Row>                            rows_;
        using SchemaPtr = std::shared_ptr<const meta::SchemaProviderIf>;
        SchemaPtr                                   schema_{nullptr};
        std::unordered_map<std::string, uint32_t>   columnToIndex_;
        std::multimap<VertexID, uint32_t>           vidToRowIndex_;
    };

private:
    using Row = std::vector<VariantType>;
    std::vector<std::string>                    colNames_;
    std::unique_ptr<RowSetReader>               rsReader_;
    std::unique_ptr<RowSetWriter>               rsWriter_;
    std::vector<VertexID>                       vids_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_INTERIMRESULT_H_
