/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_DATATYPES_DATASET_H_
#define COMMON_DATATYPES_DATASET_H_

#include <iostream>
#include <iterator>
#include <sstream>
#include <string>

#include "common/datatypes/Value.h"
#include "common/datatypes/List.h"

namespace nebula {

using Row = List;

struct DataSet {
    std::vector<std::string> colNames;
    std::vector<Row> rows;

    DataSet() = default;
    explicit DataSet(std::vector<std::string> columns) : colNames(std::move(columns)) {}
    DataSet(const DataSet& ds) noexcept {
        colNames = ds.colNames;
        rows = ds.rows;
    }
    DataSet(DataSet&& ds) noexcept {
        colNames = std::move(ds.colNames);
        rows = std::move(ds.rows);
    }
    DataSet& operator=(const DataSet& ds) noexcept {
        if (&ds != this) {
            colNames = ds.colNames;
            rows = ds.rows;
        }
        return *this;
    }
    DataSet& operator=(DataSet&& ds) noexcept {
        if (&ds != this) {
            colNames = std::move(ds.colNames);
            rows = std::move(ds.rows);
        }
        return *this;
    }

    const std::vector<std::string>& keys() const {
        return colNames;
    }

    const std::vector<Value>& rowValues(std::size_t index) const {
        return rows[index].values;
    }

    std::vector<Value> colValues(const std::string &colName) const {
        std::vector<Value> col;
        const auto find = std::find(colNames.begin(), colNames.end(), colName);
        if (find == colNames.end()) {
            return col;
        }
        std::size_t index = std::distance(colNames.begin(), find);
        col.reserve(rows.size());
        for (const auto &row : rows) {
            col.emplace_back(row.values[index]);
        }
        return col;
    }

    using iterator = std::vector<Row>::iterator;
    using const_iterator = std::vector<Row>::const_iterator;

    iterator begin() {
        return rows.begin();
    }

    const_iterator begin() const {
        return rows.begin();
    }

    iterator end() {
        return rows.end();
    }

    const_iterator end() const {
        return rows.end();
    }

    template <typename T,
              typename = typename std::enable_if<std::is_convertible<T, Row>::value, T>::type>
    bool emplace_back(T&& row) {
        if (row.size() != colNames.size()) {
            return false;
        }
        rows.emplace_back(std::forward<T>(row));
        return true;
    }

    // append the DataSet to one with same header
    bool append(DataSet&& o) {
        if (colNames.empty()) {
            colNames = std::move(o.colNames);
        } else {
            if (colNames != o.colNames) {
                return false;
            }
        }
        rows.reserve(rowSize() + o.rowSize());
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
        auto newColSize = colSize() + o.colSize();
        colNames.reserve(newColSize);
        colNames.insert(colNames.end(),
                        std::make_move_iterator(o.colNames.begin()),
                        std::make_move_iterator(o.colNames.end()));
        for (std::size_t i = 0; i < rowSize(); ++i) {
            rows[i].values.reserve(newColSize);
            rows[i].values.insert(rows[i].values.begin(),
                                  std::make_move_iterator(o.rows[i].values.begin()),
                                  std::make_move_iterator(o.rows[i].values.end()));
        }
        return true;
    }

    void clear() {
        colNames.clear();
        rows.clear();
    }

    void __clear() {
        clear();
    }

    std::size_t size() const {
        return rowSize();
    }

    std::size_t rowSize() const {
        return rows.size();
    }

    std::size_t colSize() const {
        return colNames.size();
    }

    std::string toString() const {
        std::stringstream os;
        // header
        for (const auto &h : colNames) {
            os << h << "|";
        }
        os << std::endl;

        // body
        for (const auto &row : rows) {
            for (const auto &col : row.values) {
                os << col << "|";
            }
            os << std::endl;
        }
        os << std::endl;
        return os.str();
    }

    bool operator==(const DataSet& rhs) const {
        return colNames == rhs.colNames && rows == rhs.rows;
    }
};

inline std::ostream &operator<<(std::ostream& os, const DataSet& d) {
    return os << d.toString();
}

}  // namespace nebula
#endif  // COMMON_DATATYPES_DATASET_H_
