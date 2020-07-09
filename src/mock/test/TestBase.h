/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_TEST_TESTBASE_H_
#define MOCK_TEST_TESTBASE_H_

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/interface/gen-cpp2/GraphService.h"

namespace nebula {
namespace graph {

class TestBase : public ::testing::Test {
protected:
    void SetUp() override;

    void TearDown() override;

    static ::testing::AssertionResult TestOK() {
        return ::testing::AssertionSuccess();
    }

    static ::testing::AssertionResult TestError() {
        return ::testing::AssertionFailure();
    }

    ::testing::AssertionResult verifyColNames(const cpp2::ExecutionResponse &resp,
                                              const std::vector<std::string> &expected) {
        if (resp.get_error_code() != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "query failed: "
                               << cpp2::_ErrorCode_VALUES_TO_NAMES.at(resp.get_error_code());
        }
        bool emptyData = resp.__isset.data ? resp.get_data()->colNames.empty() : true;
        if (emptyData && expected.empty()) {
            return TestOK();
        }

        if (emptyData) {
            return TestError() << "data is empty";
        }

        const auto &colNames = resp.get_data()->colNames;

        if (colNames.size() != expected.size()) {
            return TestError() << "ColNames' count not match: "
                               << colNames.size() << " vs. " << expected.size();
        }
        for (auto i = 0u; i < colNames.size(); i++) {
            if (colNames[i] != expected[i]) {
                return TestError() << "resp colName: " << colNames[i]
                                   << ", expect colName: " << expected[i];
            }
        }
        return TestOK();
    }

    ::testing::AssertionResult verifyValues(const cpp2::ExecutionResponse &resp,
                                            const std::vector<Value> &expected) {
        std::vector<std::vector<Value>> temp;
        temp.emplace_back(expected);
        return verifyValues(resp, temp);
    }

    ::testing::AssertionResult verifyValues(const cpp2::ExecutionResponse &resp,
                                            const std::vector<std::vector<Value>> &expected) {
        if (resp.get_error_code() != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "query failed: "
                               << cpp2::_ErrorCode_VALUES_TO_NAMES.at(resp.get_error_code());
        }

        bool emptyData = resp.__isset.data ? resp.get_data()->rows.empty() : true;
        if (emptyData && expected.empty()) {
            return TestOK();
        }

        if (emptyData) {
            return TestError() << "data is empty";
        }

        const auto &rows = resp.get_data()->rows;

        if (rows.size() != expected.size()) {
            return TestError() << "rows' count not match: "
                               << rows.size() << " vs. " << expected.size();
        }

        for (auto i = 0u; i < rows.size(); i++) {
            if (rows[i].values.size() != expected[i].size()) {
                return TestError() << "The row[" << i << "]' size not match "
                                   << rows[i].values.size() << " vs. " << expected[i].size();
            }
            for (auto j = 0u; j < rows[i].values.size(); j++) {
                if (rows[i].values[j] != expected[i][j]) {
                    return TestError() << rows[i].values[j] << " vs. " << expected[i][j];
                }
            }
        }
        return TestOK();
    }
};

}   // namespace graph
}   // namespace nebula

#endif  // MOCK_TEST_TESTBASE_H_
