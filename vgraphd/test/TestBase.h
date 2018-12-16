/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_TEST_TESTBASE_H_
#define VGRAPHD_TEST_TESTBASE_H_

#include "base/Base.h"
#include <gtest/gtest.h>
#include "interface/gen-cpp2/GraphDbService.h"

/**
 * According to the ADL(Argument-dependent Lookup) rules,
 * we have to define operator<< for `std::tuple<...>' in the global scope or `std'
 */
namespace std {

template <typename Tuple, size_t...Is>
void printTupleImpl(std::ostream &os, const Tuple &tuple, std::index_sequence<Is...>) {
    auto flags = os.flags();
    os << std::boolalpha;

    using DumyType = int[];
    (void)DumyType{(void(os << (Is == 0 ? "" : ", ") << std::get<Is>(tuple)), 0)...};

    os.flags(flags);
}


template <typename Tuple>
void printTuple(std::ostream &os, const Tuple &tuple) {
    printTupleImpl(os, tuple, std::make_index_sequence<std::tuple_size<Tuple>::value>());
}


template <typename...Args>
std::ostream& operator<<(std::ostream &os, const std::tuple<Args...> &tuple) {
    os << "[";
    printTuple(os, tuple);
    os << "]";
    return os;
}

}   // namespace std


namespace vesoft {
namespace vgraph {

class TestBase : public ::testing::Test {
protected:
    void SetUp() override;

    void TearDown() override;

    using AssertionResult = ::testing::AssertionResult;
    using ColumnType = cpp2::ColumnValue::Type;
    using Row = std::vector<cpp2::ColumnValue>;
    using Rows = std::vector<Row>;

    template <typename, typename>
    struct uniform_tuple_impl;
    template <typename T, size_t...Is>
    struct uniform_tuple_impl<T, std::index_sequence<Is...>> {
        template <size_t>
        using IndexedType = T;
        using type = std::tuple<IndexedType<Is>...>;
    };

    template <typename T, size_t N>
    struct uniform_tuple {
        using type = typename uniform_tuple_impl<T, std::make_index_sequence<N>>::type;
    };

    template <typename T, size_t N>
    using uniform_tuple_t = typename uniform_tuple<T, N>::type;

    Rows respToRecords(const cpp2::ExecutionResponse &resp) {
        CHECK(resp.get_rows() != nullptr);
        Rows result;
        for (auto &row : *resp.get_rows()) {
            auto &columns = row.get_columns();
            result.emplace_back();
            result.back().assign(columns.begin(), columns.end());
        }
        return result;
    }

    /**
     * Convert `ColumnValue' to its cooresponding type
     */
    template <typename T>
    std::enable_if_t<std::is_integral<T>::value, T>
    convert(const cpp2::ColumnValue &v) {
        switch (v.getType()) {
            case ColumnType::integer:
                return v.get_integer();
            case ColumnType::timestamp:
                return v.get_timestamp();
            case ColumnType::id:
                return v.get_id();
            case ColumnType::boolean:
                return v.get_boolean();
            default:
                throw TestError() << "Cannot convert unknown dynamic column type to integer: "
                                  << static_cast<int32_t>(v.getType());
        }
        return T();     // suppress the no-return waring
    }

    template <typename T>
    std::enable_if_t<std::is_same<T, std::string>::value, T>
    convert(const cpp2::ColumnValue &v) {
        switch (v.getType()) {
            case ColumnType::str:
                return v.get_str();
            default:
                throw TestError() << "Cannot convert unknown dynamic column type to string: "
                                  << static_cast<int32_t>(v.getType());
        }
        return T();     // suppress the no-return warning
    }

    /**
     * Transform rows of dynamic type to tuples of static type
     */
    template <typename Tuple, size_t...Is>
    auto rowToTupleImpl(const Row &row, std::index_sequence<Is...>) {
        return std::make_tuple(convert<std::tuple_element_t<Is, Tuple>>(row[Is])...);
    }

    template <typename Tuple>
    auto rowToTuple(const Row &row) {
        constexpr auto tupleSize = std::tuple_size<Tuple>::value;
        return rowToTupleImpl<Tuple>(row, std::make_index_sequence<tupleSize>());
    }

    template <typename Tuple>
    auto rowsToTuples(const Rows &rows) {
        std::vector<Tuple> result;
        if (rows.empty()) {
            return result;
        }
        if (rows.back().size() != std::tuple_size<Tuple>::value) {
            throw TestError() << "Column count not match: "
                              << rows.back().size() << " vs. "
                              << std::tuple_size<Tuple>::value;
        }
        for (auto &row : rows) {
            result.emplace_back(rowToTuple<Tuple>(row));
        }
        return result;
    }

    template <typename Tuple>
    AssertionResult verifyResult(const cpp2::ExecutionResponse &resp,
                                 std::vector<Tuple> &expected) {
        if (resp.get_error_code() != cpp2::ErrorCode::SUCCEEDED) {
            auto *errmsg = resp.get_error_msg();
            return TestError() << "Query failed with `"
                               << static_cast<int32_t>(resp.get_error_code())
                               << (errmsg == nullptr ? "'" : "': " + *errmsg);
        }

        std::vector<Tuple> rows;
        try {
            rows = rowsToTuples<Tuple>(respToRecords(resp));
        } catch (const AssertionResult &e) {
            return e;
        } catch (const std::exception &e) {
            return TestError() << "Unknown exception thrown: " << e.what();
        }

        if (expected.size() != rows.size()) {
            return TestError() << "Rows' count not match: "
                               << rows.size() << " vs. " << expected.size();
        }

        if (expected.empty()) {
            return TestOK();
        }

        std::sort(rows.begin(), rows.end());
        std::sort(expected.begin(), expected.end());

        for (auto i = 0u; i < rows.size(); i++) {
            if (rows[i] != expected[i]) {
                return TestError() << rows[i] << " vs. " << expected[i];
            }
        }
        return TestOK();
    }

protected:
    std::function<AssertionResult()> TestOK = [] { return ::testing::AssertionSuccess(); };
    std::function<AssertionResult()> TestError = [] { return ::testing::AssertionFailure(); };
};

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_TEST_TESTBASE_H_
