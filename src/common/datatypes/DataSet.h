/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_DATASET_H_
#define COMMON_DATATYPES_DATASET_H_

#include "common/base/Base.h"
#include "common/datatypes/Value.h"

namespace nebula {

struct Row {
    std::vector<Value> columns;

    Row() = default;
    // reuse the verctor constructor
    explicit Row(std::vector<Value>&& vals) : columns(std::move(vals)) {}
    explicit Row(const std::vector<Value>& vals) : columns(vals) {}
    Row(const Row& r) noexcept {
        columns = r.columns;
    }
    Row(Row&& r) noexcept {
        columns = std::move(r.columns);
    }

    Row& operator=(const Row& r) noexcept {
        columns = r.columns;
        return *this;
    }
    Row& operator=(Row&& r) noexcept {
        columns = std::move(r.columns);
        return *this;
    }

    template <typename T, typename = std::enable_if_t<std::is_convertible<T, Value>::value, T>>
    void emplace_back(T&& v) {
        columns.emplace_back(std::forward<T>(v));
    }

    void clear() {
        columns.clear();
    }

    std::size_t size() const {
        return columns.size();
    }

    bool operator==(const Row& rhs) const {
        return columns == rhs.columns;
    }
};

struct DataSet {
    std::vector<std::string> colNames;
    std::vector<Row> rows;

    DataSet() = default;
    explicit DataSet(std::vector<std::string>&& colNames_) : colNames(std::move(colNames_)) {}
    DataSet(const DataSet& ds) noexcept {
        colNames = ds.colNames;
        rows = ds.rows;
    }
    DataSet(DataSet&& ds) noexcept {
        colNames = std::move(ds.colNames);
        rows = std::move(ds.rows);
    }
    DataSet& operator=(const DataSet& ds) noexcept {
        colNames = ds.colNames;
        rows = ds.rows;
        return *this;
    }
    DataSet& operator=(DataSet&& ds) noexcept {
        colNames = std::move(ds.colNames);
        rows = std::move(ds.rows);
        return *this;
    }

    template <typename T, typename = std::enable_if_t<std::is_convertible<T, Row>::value, T>>
    bool emplace_back(T&& row) {
        if (row.size() != colNames.size()) {
            return false;
        }
        rows.emplace_back(std::forward<T>(row));
        return true;
    }

    // append the DataSet to one with same header
    bool append(DataSet&& o) {
        if (colNames != o.colNames) {
            return false;
        }
        rows.reserve(o.rows.size());
        rows.insert(rows.end(),
                    std::make_move_iterator(o.rows.begin()),
                    std::make_move_iterator(o.rows.end()));
        return true;
    }

    // merge two DataSet Horizontally with same row count
    bool merge(DataSet&& o) {
        if (rowSize() != o.rowSize()) {
            return false;
        }
        colNames.reserve(o.colSize());
        colNames.insert(colNames.end(),
                        std::make_move_iterator(o.colNames.begin()),
                        std::make_move_iterator(o.colNames.end()));
        for (std::size_t i = 0; i < rowSize(); ++i) {
            rows[i].columns.reserve(o.rows[i].size());
            rows[i].columns.insert(rows[i].columns.begin(),
                                   std::make_move_iterator(o.rows[i].columns.begin()),
                                   std::make_move_iterator(o.rows[i].columns.end()));
        }
        return true;
    }

    void clear() {
        colNames.clear();
        rows.clear();
    }

    std::size_t rowSize() const {
        return rows.size();
    }

    std::size_t colSize() const {
        return colNames.size();
    }

    bool operator==(const DataSet& rhs) const {
        return colNames == rhs.colNames && rows == rhs.rows;
    }
};

}  // namespace nebula
#endif  // COMMON_DATATYPES_DATASET_H_
