/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/network/NetworkUtils.h"
#include "kvstore/plugins/hbase/HBaseClient.h"
#include <gtest/gtest.h>

/**
 * TODO(zhangguoqing) Add a test runner to provide HBase/thrift2 service.
 * hbase/bin/hbase-daemon.sh start thrift2 -b 127.0.0.1 -p 9096
 * hbase(main):001:0> create 'Nebula_Graph_Space_0', 'cf'
 * */

namespace nebula {
namespace kvstore {

TEST(HBaseClientTest, SimpleTest) {
    auto hbaseClient = std::make_shared<HBaseClient>(HostAddr(0, 9096));
    std::string tableName = "Nebula_Graph_Space_0";
    std::string rowKey = "rowKey";
    std::vector<KV> putData;
    for (int32_t i = 0; i < 10; i++) {
        putData.emplace_back(folly::stringPrintf("col_%d", i),
                             folly::stringPrintf("val_%d", i));
    }
    EXPECT_EQ(ResultCode::SUCCEEDED, hbaseClient->put(tableName, rowKey, putData));

    KVMap getData;
    EXPECT_EQ(ResultCode::SUCCEEDED, hbaseClient->get(tableName, rowKey, getData));
    EXPECT_EQ(10, getData.size());
    for (int32_t i = 0; i < 10;  i++) {
        auto kvIt = getData.find(folly::stringPrintf("col_%d", i));
        EXPECT_TRUE(kvIt != getData.end());
        if (kvIt != getData.end()) {
            EXPECT_EQ(kvIt->second, folly::stringPrintf("val_%d", i));
        }
    }

    EXPECT_EQ(ResultCode::SUCCEEDED, hbaseClient->remove(tableName, rowKey));
    EXPECT_EQ(ResultCode::ERR_KEY_NOT_FOUND, hbaseClient->get(tableName, rowKey, getData));
}


TEST(HBaseClientTest, MultiTest) {
    auto hbaseClient = std::make_shared<HBaseClient>(HostAddr(0, 9096));
    std::string tableName = "Nebula_Graph_Space_0";
    std::vector<std::string> rowKeys;
    std::vector<std::pair<std::string, std::vector<KV>>> dataList;
    for (int32_t i = 10; i < 20;  i++) {
        std::string rowKey = "rowKey_" + folly::to<std::string>(i);
        rowKeys.emplace_back(rowKey);
        std::vector<KV> putData;
        for (int32_t j = 0; j < 10; j++) {
            putData.emplace_back(folly::stringPrintf("col_%d", j),
                                 folly::stringPrintf("val_%d", j));
        }
        dataList.emplace_back(rowKey, putData);
    }
    EXPECT_EQ(ResultCode::SUCCEEDED, hbaseClient->multiPut(tableName, dataList));

    std::vector<std::pair<std::string, KVMap>> retDataList;
    auto ret = hbaseClient->multiGet(tableName, rowKeys, retDataList);
    EXPECT_EQ(ResultCode::SUCCEEDED, ret.first);
    EXPECT_EQ(10, retDataList.size());
    for (size_t index = 0; index < retDataList.size(); index++) {
        EXPECT_EQ(rowKeys[index], retDataList[index].first);
        EXPECT_EQ(10, retDataList[index].second.size());
        for (int32_t i = 0; i < 10; i++) {
            auto kvIt = retDataList[index].second.find(folly::stringPrintf("col_%d", i));
            EXPECT_TRUE(kvIt != retDataList[index].second.end());
            if (kvIt != retDataList[index].second.end()) {
                EXPECT_EQ(kvIt->second, folly::stringPrintf("val_%d", i));
            }
        }
    }

    EXPECT_EQ(ResultCode::SUCCEEDED, hbaseClient->multiRemove(tableName, rowKeys));
    retDataList.clear();
    ret = hbaseClient->multiGet(tableName, rowKeys, retDataList);
    EXPECT_EQ(ResultCode::E_UNKNOWN, ret.first);
    EXPECT_EQ(0, retDataList.size());
}


TEST(HBaseClientTest, RangeTest) {
    auto hbaseClient = std::make_shared<HBaseClient>(HostAddr(0, 9096));
    std::string tableName = "Nebula_Graph_Space_0";
    std::vector<std::string> rowKeys;
    std::vector<std::pair<std::string, std::vector<KV>>> dataList;
    for (int32_t i = 10; i < 20;  i++) {
        std::string rowKey = "rowKey_" + folly::to<std::string>(i);
        rowKeys.emplace_back(rowKey);
        std::vector<KV> putData;
        putData.emplace_back("col", "val");
        dataList.emplace_back(rowKey, putData);
    }
    EXPECT_EQ(ResultCode::SUCCEEDED, hbaseClient->multiPut(tableName, dataList));

    auto checkRange = [&](int32_t start, int32_t end,
                          int32_t expectedFrom, int32_t expectedTotal) {
        LOG(INFO) << "start " << start << ", end " << end
                  << ", expectedFrom " << expectedFrom << ", expectedTotal " << expectedTotal;
        std::string s = "rowKey_" + folly::to<std::string>(start);
        std::string e = "rowKey_" + folly::to<std::string>(end);
        std::vector<std::pair<std::string, KVMap>> retDataList;
        EXPECT_EQ(ResultCode::SUCCEEDED, hbaseClient->range(tableName, s, e, retDataList));

        int32_t num = 0;
        for (auto& retData : retDataList) {
            EXPECT_EQ("rowKey_" + folly::to<std::string>(expectedFrom + num), retData.first);
            EXPECT_EQ(1, retData.second.size());
            auto kvIt = retData.second.find("col");
            EXPECT_TRUE(kvIt != retData.second.end());
            if (kvIt != retData.second.end()) {
                EXPECT_EQ(kvIt->second, "val");
            }
            num++;
        }
        EXPECT_EQ(expectedTotal, num);
    };

    checkRange(10, 20, 10, 10);
    checkRange(1, 50, 10, 10);
    checkRange(15, 18, 15, 3);
    checkRange(15, 23, 15, 5);
    checkRange(1, 15, 10, 5);
    EXPECT_EQ(ResultCode::SUCCEEDED, hbaseClient->multiRemove(tableName, rowKeys));
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

