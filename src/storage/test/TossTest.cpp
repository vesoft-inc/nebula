/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "TossEnvironment.h"
#include "folly/String.h"
#define LOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

namespace nebula {
namespace storage {

using StorageClient = storage::GraphStorageClient;

constexpr bool kUseToss = true;
constexpr bool kNotUseToss = false;

static int32_t b_ = 1;
static int32_t gap = 1000;

enum class TossTestEnum {
    NO_TOSS = 1,
    // add one edge
    ADD_ONES_EDGE = 2,
    // add two edges(same local part, same remote part)
    TWO_EDGES_CASE_1,
    // add two edges(same local part, diff remote part)
    TWO_EDGES_CASE_2,
    // add two edges(diff local part, same remote part)
    TWO_EDGES_CASE_3,
    // add two edges(diff local part, diff remote part)
    TWO_EDGES_CASE_4,
    // add 10 edges(same local part, same remote part)
    TEN_EDGES_CASE_1,
    // add 10 edges(same local part, diff remote part)
    TEN_EDGES_CASE_2,
    // add 10 edges(diff local part, same remote part)
    TEN_EDGES_CASE_3,
    // add 10 edges(diff local part, diff remote part)
    TEN_EDGES_CASE_4,
};

class TossTest : public ::testing::Test {
public:
    TossTest() {
        types_.emplace_back(meta::cpp2::PropertyType::INT64);
        types_.emplace_back(meta::cpp2::PropertyType::STRING);
        colDefs_ = genColDefs(types_);

        boost::uuids::uuid u;
        values_.emplace_back();
        values_.back().setInt(1024);
        values_.emplace_back();
        values_.back().setStr("defaul:" + boost::uuids::to_string(u));

        env_ = TossEnvironment::getInstance(kMetaName, kMetaPort);
        env_->init(kSpaceName, kPart, kReplica, colDefs_);
    }

    void SetUp() override {
        if (b_ % gap) {
            b_ = (b_ / gap + 1) * gap;
        } else {
            b_ += gap;
        }
    }

    void TearDown() override {
    }

protected:
    TossEnvironment* env_{nullptr};

    std::vector<meta::cpp2::PropertyType>   types_;
    std::vector<meta::cpp2::ColumnDef>      colDefs_;
    std::vector<nebula::Value>              values_;
    boost::uuids::random_generator          gen_;

    // generate ColumnDefs from data types, set default value to NULL.
    std::vector<meta::cpp2::ColumnDef>
    genColDefs(const std::vector<meta::cpp2::PropertyType>& types) {
        auto N = types.size();
        auto colNames = TossEnvironment::makeColNames(N);
        std::vector<meta::cpp2::ColumnDef> ret(N);
        for (auto i = 0U; i != N; ++i) {
            ret[i].set_name(colNames[i]);
            meta::cpp2::ColumnTypeDef tpDef;
            tpDef.set_type(types[i]);
            ret[i].set_type(tpDef);
            ret[i].set_nullable(true);
        }
        return ret;
    }
};

// make sure environment ok
TEST_F(TossTest, NO_TOSS) {
    int32_t num = 1;
    auto values = TossTestUtils::genValues(num);

    int32_t __e = static_cast<int32_t>(TossTestEnum::NO_TOSS) * 100;
    int64_t src = env_->getSpaceId() * __e * kPart;

    std::vector<cpp2::NewEdge> edges;
    edges.emplace_back(env_->generateEdge(src, 0, values.back(), src + kPart));

    std::vector<cpp2::NewEdge> startWith{edges[0]};
    auto props = env_->getNeiProps(startWith);
    EXPECT_EQ(env_->countSquareBrackets(props), 0);

    LOG(INFO) << "going to add edge:" << edges.back().get_props().back();
    auto code = env_->syncAddMultiEdges(edges, kNotUseToss);
    ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);

    props = env_->getNeiProps(startWith);

    LOG(INFO) << "props.size()=" << props.size();
    for (auto& prop : props) {
        LOG(INFO) << "prop: " << prop;
    }
    EXPECT_EQ(env_->countSquareBrackets(props), 1);
}

TEST_F(TossTest, ONE_EDGE) {
    auto num = 1U;
    auto values = TossTestUtils::genValues(num);

    std::vector<cpp2::NewEdge> edges;
    edges.emplace_back(env_->generateEdge(b_, 0, values.back(), b_ + kPart));

    std::vector<cpp2::NewEdge> startWith{edges[0]};
    auto props = env_->getNeiProps(startWith);
    EXPECT_EQ(env_->countSquareBrackets(props), 0);

    LOG(INFO) << "going to add edge:" << TossTestUtils::hexEdgeId(edges.back().get_key());
    auto code = env_->syncAddMultiEdges(edges, kUseToss);
    ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);;

    props = env_->getNeiProps(startWith);

    LOG(INFO) << "props.size()=" << props.size();
    for (auto& prop : props) {
        LOG(INFO) << "prop: " << prop;
    }
    auto results = TossTestUtils::splitNeiResults(props);
    auto cnt = results.size();
    EXPECT_EQ(cnt, num);
    for (auto& res : results) {
        LOG(INFO) << "res: " << res;
    }
}

TEST_F(TossTest, TWO_EDGES_CASE_1) {
    int32_t num = 2;
    auto values = TossTestUtils::genValues(num);

    std::vector<cpp2::NewEdge> edges;
    edges.emplace_back(env_->generateEdge(b_, 0, values[0], b_ + kPart));
    edges.emplace_back(env_->generateEdge(b_, 0, values[1], b_ + kPart*2));

    for (auto& e : edges) {
        LOG(INFO) << "going to add edge: " << TossTestUtils::hexEdgeId(e.get_key());
    }

    std::vector<cpp2::NewEdge> startWith{edges[0]};
    auto props = env_->getNeiProps(startWith);
    EXPECT_EQ(env_->countSquareBrackets(props), 0);

    auto code = env_->syncAddMultiEdges(edges, kUseToss);
    ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);

    props = env_->getNeiProps(startWith);
    EXPECT_EQ(env_->countSquareBrackets(props), 2);
    if (env_->countSquareBrackets(props) != 2) {
        TossTestUtils::print_svec(props);
    }
}

TEST_F(TossTest, TWO_EDGES_CASE_2) {
    int32_t num = 2;
    auto values = TossTestUtils::genValues(num);

    int32_t __e = static_cast<int32_t>(TossTestEnum::TWO_EDGES_CASE_2) * 100;
    int64_t src = env_->getSpaceId() * __e * kPart;

    std::vector<cpp2::NewEdge> edges;
    edges.emplace_back(env_->generateEdge(src, 0, values[0], src + kPart));
    edges.emplace_back(env_->generateEdge(src, 0, values[1], src + kPart+1));

    LOG(INFO) << "src=" << src << ", hex=" << TossTestUtils::hexVid(src);

    std::vector<cpp2::NewEdge> first{edges[0]};
    auto props = env_->getNeiProps(first);
    EXPECT_EQ(env_->countSquareBrackets(props), 0);

    auto code = env_->syncAddMultiEdges(edges, kUseToss);
    LOG_IF(FATAL, code != nebula::cpp2::ErrorCode::SUCCEEDED)
        << "fatal code=" << apache::thrift::util::enumNameSafe(code);

    props = env_->getNeiProps(first);
    EXPECT_EQ(env_->countSquareBrackets(props), 2);
    if (env_->countSquareBrackets(props) != 2) {
        TossTestUtils::print_svec(props);
    }
}

TEST_F(TossTest, TWO_EDGES_CASE_3) {
    int32_t num = 2;
    auto values = TossTestUtils::genValues(num);

    int32_t __e = static_cast<int32_t>(TossTestEnum::TWO_EDGES_CASE_3) * 100;
    int64_t src = env_->getSpaceId() * __e * kPart;
    int64_t src1st = src;
    int64_t src2nd = src1st+1;

    std::vector<cpp2::NewEdge> edges;
    edges.emplace_back(env_->generateEdge(src, 0, values[0], src + kPart));
    edges.emplace_back(env_->generateEdge(src2nd, 0, values[1], src + kPart));

    LOG(INFO) << "src1st=" << src1st << ", hex=" << TossTestUtils::hexVid(src1st);
    LOG(INFO) << "src2nd=" << src2nd << ", hex=" << TossTestUtils::hexVid(src2nd);

    std::vector<cpp2::NewEdge> first{edges[0]};
    std::vector<cpp2::NewEdge> second{edges[1]};
    auto props = env_->getNeiProps(first);
    EXPECT_EQ(env_->countSquareBrackets(props), 0);
    props = env_->getNeiProps(second);
    EXPECT_EQ(env_->countSquareBrackets(props), 0);

    auto code = env_->syncAddMultiEdges(edges, kUseToss);
    ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);

    props = env_->getNeiProps(first);
    LOG(INFO) << "props.size()=" << props.size();
    for (auto& prop : props) {
        LOG(INFO) << prop;
    }
    EXPECT_EQ(env_->countSquareBrackets(props), 1);

    props = env_->getNeiProps(second);
    LOG(INFO) << "props.size()=" << props.size();
    for (auto& prop : props) {
        LOG(INFO) << prop;
    }
    EXPECT_EQ(env_->countSquareBrackets(props), 1);

    props = env_->getNeiProps(edges);
    LOG(INFO) << "props.size()=" << props.size();
    for (auto& prop : props) {
        LOG(INFO) << prop;
    }
    EXPECT_EQ(env_->countSquareBrackets(props), 2);
}

TEST_F(TossTest, TWO_EDGES_CASE_4) {
    int32_t num = 2;
    auto values = TossTestUtils::genValues(num);

    int32_t __e = static_cast<int32_t>(TossTestEnum::TWO_EDGES_CASE_4) * 100;
    int64_t src = env_->getSpaceId() * __e * kPart;

    std::vector<cpp2::NewEdge> edges;
    edges.emplace_back(env_->generateEdge(src, 0, values[0], src + kPart+1));
    edges.emplace_back(env_->generateEdge(src+1, 0, values[1], src + kPart));

    std::vector<cpp2::NewEdge> startWith{edges[0]};
    auto props = env_->getNeiProps(startWith);
    EXPECT_EQ(env_->countSquareBrackets(props), 0);

    auto code = env_->syncAddMultiEdges(edges, kUseToss);
    LOG_IF(FATAL, code != nebula::cpp2::ErrorCode::SUCCEEDED)
        << "fatal code=" << apache::thrift::util::enumNameSafe(code);

    props = env_->getNeiProps(startWith);
    for (auto& prop : props) {
        LOG(INFO) << prop;
    }
    EXPECT_EQ(env_->countSquareBrackets(props), 1);
}

TEST_F(TossTest, TEN_EDGES_CASE_1) {
    int32_t num = 10;
    auto values = TossTestUtils::genValues(num);

    int32_t __e = static_cast<int32_t>(TossTestEnum::TEN_EDGES_CASE_1) * 100;
    int64_t src = env_->getSpaceId() * __e * kPart;

    std::vector<cpp2::NewEdge> edges;
    for (int32_t i = 0; i != num; ++i) {
        edges.emplace_back(env_->generateEdge(src, 0, values[i], src + kPart+i+1));
    }

    std::vector<cpp2::NewEdge> first{edges[0]};
    // auto props = env_->getNeiProps(startWith);
    // EXPECT_EQ(env_->countSquareBrackets(props), 0);

    auto code = env_->syncAddMultiEdges(edges, kUseToss);
    LOG_IF(FATAL, code != nebula::cpp2::ErrorCode::SUCCEEDED)
        << "fatal code=" << apache::thrift::util::enumNameSafe(code);

    auto props = env_->getNeiProps(first);
    // LOG(INFO) << "props: " << props;
    EXPECT_EQ(env_->countSquareBrackets(props), num);
}

TEST_F(TossTest, TEN_EDGES_CASE_2) {
    auto num = 100U;
    auto values = TossTestUtils::genValues(num);

    int32_t __e = static_cast<int32_t>(TossTestEnum::TEN_EDGES_CASE_2) * 100;
    int64_t src = env_->getSpaceId() * __e * kPart;

    std::vector<cpp2::NewEdge> edges;
    for (auto i = 0U; i != num; ++i) {
        edges.emplace_back(env_->generateEdge(src, 0, values[i], src + kPart+i+1));
    }

    std::vector<cpp2::NewEdge> first{edges[0]};

    auto code = env_->syncAddMultiEdges(edges, kUseToss);
    LOG_IF(FATAL, code != nebula::cpp2::ErrorCode::SUCCEEDED)
        << "fatal code=" << apache::thrift::util::enumNameSafe(code);

    auto props = env_->getNeiProps(first);
    // auto actual = env_->countSquareBrackets(props);
    auto svec = TossTestUtils::splitNeiResults(props);
    auto actual = svec.size();
    EXPECT_EQ(actual, num);
    if (actual != num) {
        for (auto& s : svec) {
            LOG(INFO) << "s" << s;
        }
    }
}

TEST_F(TossTest, lock_test_0) {
    auto num = 1;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);

    auto edge = TossTestUtils::toVertexIdEdge(edges[0]);

    auto vIdLen = 8;
    auto partId = 5;  // just a random number
    auto rawKey = TransactionUtils::edgeKey(vIdLen, partId, edge.get_key());
    auto lockKey = NebulaKeyUtils::toLockKey(rawKey);

    ASSERT_TRUE(NebulaKeyUtils::isLock(vIdLen, lockKey));
}

/**
 * @brief good lock
 */
TEST_F(TossTest, lock_test_1) {
    auto num = 1;
    LOG(INFO) << "b_=" << b_;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);

    auto lockKey = env_->insertLock(edges[0], true);
    LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);

    ASSERT_FALSE(env_->keyExist(lockKey));

    auto rawKey = NebulaKeyUtils::toEdgeKey(lockKey);
    ASSERT_TRUE(env_->keyExist(rawKey));

    auto reversedEdgeKey = env_->reverseEdgeKey(edges[0].get_key());
    auto reversedRawKey = env_->makeRawKey(reversedEdgeKey);
    ASSERT_TRUE(env_->keyExist(reversedRawKey.first));

    ASSERT_EQ(svec.size(), num);
}

/**
 * @brief good lock + edge
 */
TEST_F(TossTest, lock_test_2) {
    LOG(INFO) << "b_=" << b_;
    auto num = 1;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_, true);

    auto lockKey = env_->insertLock(edges[0], true);

    LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);
    auto rawKey = env_->insertEdge(edges[1]);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey));

    // step 2nd: edge key exist
    // auto rawKey = NebulaKeyUtils::toEdgeKey(lockKey);
    ASSERT_TRUE(env_->keyExist(rawKey));

    // step 3rd: reverse edge key exist
    auto reversedEdgeKey = env_->reverseEdgeKey(edges[0].get_key());
    auto reversedRawKey = env_->makeRawKey(reversedEdgeKey);
    ASSERT_TRUE(env_->keyExist(reversedRawKey.first));

    // step 4th: the get neighbors result is from lock
    auto lockStrVal = edges[0].get_props()[1].toString();
    ASSERT_TRUE(svec.back().size() > lockStrVal.size());
    auto neighborStrVal = svec.back().substr(svec.back().size() - lockStrVal.size());
    ASSERT_EQ(lockStrVal, neighborStrVal);

    ASSERT_EQ(svec.size(), num);
}

/**
 * @brief bad lock
 */
TEST_F(TossTest, lock_test_3) {
    LOG(INFO) << "b_=" << b_;
    auto num = 1;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_, true);

    auto lockKey = env_->insertLock(edges[0], false);
    ASSERT_TRUE(env_->keyExist(lockKey));
    LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);
    EXPECT_EQ(svec.size(), 0);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey));
}

/**
 * @brief bad lock + edge
 */
TEST_F(TossTest, lock_test_4) {
    LOG(INFO) << "b_=" << b_;
    auto num = 1;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_, true);
    ASSERT_EQ(edges.size(), 2);

    auto lockKey = env_->insertLock(edges[0], false);
    ASSERT_TRUE(env_->keyExist(lockKey));
    LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

    auto rawKey = env_->insertEdge(edges[1]);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);
    ASSERT_EQ(svec.size(), num);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey));

    // step 2nd: edge key exist
    // auto rawKey = NebulaKeyUtils::toEdgeKey(lockKey);
    ASSERT_TRUE(env_->keyExist(rawKey));

    // step 3rd: reverse edge key exist
    auto reversedEdgeKey = env_->reverseEdgeKey(edges[0].get_key());
    auto reversedRawKey = env_->makeRawKey(reversedEdgeKey);
    ASSERT_FALSE(env_->keyExist(reversedRawKey.first));

    // step 4th: the get neighbors result is from lock
    auto edgeStrVal = edges[1].get_props()[1].toString();
    ASSERT_TRUE(svec.back().size() > edgeStrVal.size());
    auto neighborStrVal = svec.back().substr(svec.back().size() - edgeStrVal.size());
    ASSERT_EQ(edgeStrVal, neighborStrVal);
}

/**
 * @brief neighbor edge + edge
 *        check normal data(without lock) can be read without err
 */
TEST_F(TossTest, neighbors_test_1) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto rawKey1 = env_->insertEdge(edges[0]);
    auto rawKey2 = env_->insertEdge(edges[1]);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);
    ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
}

/**
 * @brief neighbor edge + good lock
 */
TEST_F(TossTest, neighbors_test_2) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto rawKey1 = env_->insertEdge(edges[0]);

    auto lockKey = env_->insertLock(edges[1], true);
    LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);
    ASSERT_TRUE(TossTestUtils::compareSize(svec, num));

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey));

    // step 2nd: edge key exist
    auto rawKey = NebulaKeyUtils::toEdgeKey(lockKey);
    ASSERT_TRUE(env_->keyExist(rawKey));

    // step 3rd: reverse edge key exist
    auto reversedEdgeKey = env_->reverseEdgeKey(edges[1].get_key());
    auto reversedRawKey = env_->makeRawKey(reversedEdgeKey);
    ASSERT_TRUE(env_->keyExist(reversedRawKey.first));
}

/**
 * @brief neighbor edge + good lock + edge
 */
TEST_F(TossTest, neighbors_test_3) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);
    auto lockEdge = env_->dupEdge(edges[1]);

    auto rawKey1 = env_->insertEdge(edges[0]);
    auto rawKey2 = env_->insertEdge(edges[1]);
    auto lockKey = env_->insertLock(lockEdge, true);
    LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);
    ASSERT_TRUE(TossTestUtils::compareSize(svec, num));

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey));

    // step 2nd: edge key exist
    ASSERT_TRUE(env_->keyExist(rawKey1));
    ASSERT_TRUE(env_->keyExist(rawKey2));

    // step 3rd: reverse edge key exist
    auto reversedEdgeKey = env_->reverseEdgeKey(edges[1].get_key());
    auto reversedRawKey = env_->makeRawKey(reversedEdgeKey);
    ASSERT_TRUE(env_->keyExist(reversedRawKey.first));

    // TossTestUtils::print_svec(svec);
    LOG(INFO) << "edges[0]=" << edges[0].get_props()[1].toString();
    LOG(INFO) << "edges[1]=" << edges[1].get_props()[1].toString();
    LOG(INFO) << "lockEdge=" << lockEdge.get_props()[1].toString();

    LOG(INFO) << "lockEdge.size()=" << lockEdge.get_props()[1].toString().size();

    // step 4th: the get neighbors result is from lock
    auto strProps = env_->extractStrVals(svec);
    decltype(strProps) expect{
        edges[0].get_props()[1].toString(),
        lockEdge.get_props()[1].toString()
    };
    ASSERT_EQ(strProps, expect);
}

/**
 * @brief neighbor edge + bad lock
 */
TEST_F(TossTest, neighbors_test_4) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto rawKey1 = env_->insertEdge(edges[0]);
    auto lockEdge = env_->dupEdge(edges[1]);

    auto lockKey = env_->insertLock(lockEdge, false);
    LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);
    ASSERT_TRUE(TossTestUtils::compareSize(svec, num-1));

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey));

    // step 2nd: edge key exist
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto reversedEdgeKey = env_->reverseEdgeKey(lockEdge.get_key());
    auto reversedRawKey = env_->makeRawKey(reversedEdgeKey);
    ASSERT_FALSE(env_->keyExist(reversedRawKey.first));

    // TossTestUtils::print_svec(svec);
    LOG(INFO) << "edges[0]=" << edges[0].get_props()[1].toString();

    // step 4th: the get neighbors result is from lock
    auto strProps = env_->extractStrVals(svec);
    decltype(strProps) expect{edges[0].get_props()[1].toString()};
    ASSERT_EQ(strProps, expect);
}

/**
 * @brief neighbor edge + bad lock + edge
 */
TEST_F(TossTest, neighbors_test_5) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock1 = env_->dupEdge(edges[1]);

    auto lockKey1 = env_->insertLock(lock1, false);
    LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey1);

    auto rawKey0 = env_->insertEdge(edges[0]);
    auto rawKey1 = env_->insertEdge(edges[1]);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto reversedEdgeKey = env_->reverseEdgeKey(lock1.get_key());
    auto reversedRawKey = env_->makeRawKey(reversedEdgeKey);
    ASSERT_FALSE(env_->keyExist(reversedRawKey.first));

    LOG(INFO) << "edges[0]=" << edges[0].get_props()[1].toString();

    // step 4th: the get neighbors result is from lock
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{
        edges[0].get_props()[1].toString(),
        edges[1].get_props()[1].toString()
    };
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
}

/**
 * @brief neighbor good lock + edge
 */
TEST_F(TossTest, neighbors_test_6) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lockEdge = env_->dupEdge(edges[0]);

    auto lockKey = env_->insertLock(lockEdge, true);
    LOG(INFO) << "lock_test_1 lock hexlify = " << folly::hexlify(lockKey);

    // auto rawKey0 = env_->insertEdge(edges[0]);
    auto rawKey1 = env_->insertEdge(edges[1]);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey));

    // step 2nd: edge key exist
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto reversedEdgeKey = env_->reverseEdgeKey(lockEdge.get_key());
    auto reversedRawKey = env_->makeRawKey(reversedEdgeKey);
    ASSERT_TRUE(env_->keyExist(reversedRawKey.first));

    // step 4th: the get neighbors result is from lock
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{
        lockEdge.get_props()[1].toString(),
        edges[1].get_props()[1].toString()
    };
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
}

/**
 * @brief neighbor good lock + good lock
 */
TEST_F(TossTest, neighbors_test_7) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);

    auto lockKey0 = env_->insertLock(lock0, true);
    auto lockKey1 = env_->insertLock(lock1, true);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    // auto rawKey0 = env_->insertEdge(edges[0]);
    // auto rawKey1 = env_->insertEdge(edges[1]);
    auto rawKey0 = env_->makeRawKey(edges[0].get_key());
    auto rawKey1 = env_->makeRawKey(edges[1].get_key());

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_TRUE(env_->keyExist(rawKey0.first));
    ASSERT_TRUE(env_->keyExist(rawKey1.first));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_TRUE(env_->keyExist(rRawKey0.first));
    ASSERT_TRUE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{lock0.get_props()[1].toString(), lock1.get_props()[1].toString()};
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
}

/**
 * @brief neighbor good lock + good lock + edge
 */
TEST_F(TossTest, neighbors_test_8) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);

    auto lockKey0 = env_->insertLock(lock0, true);
    auto lockKey1 = env_->insertLock(lock1, true);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    auto rawKey0 = env_->makeRawKey(edges[0].get_key()).first;
    // auto rawKey1 = env_->makeRawKey(edges[1].key).first;

    // auto rawKey0 = env_->insertEdge(edges[0]);
    auto rawKey1 = env_->insertEdge(edges[1]);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_TRUE(env_->keyExist(rawKey0));
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_TRUE(env_->keyExist(rRawKey0.first));
    ASSERT_TRUE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{lock0.get_props()[1].toString(), lock1.get_props()[1].toString()};
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
}

/**
 * @brief neighbor good lock + bad lock
 */
TEST_F(TossTest, neighbors_test_9) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);

    auto lockKey0 = env_->insertLock(lock0, true);
    auto lockKey1 = env_->insertLock(lock1, false);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    auto rawKey0 = env_->makeRawKey(edges[0].get_key()).first;
    auto rawKey1 = env_->makeRawKey(edges[1].get_key()).first;

    // auto rawKey0 = env_->insertEdge(edges[0]);
    // auto rawKey1 = env_->insertEdge(edges[1]);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_TRUE(env_->keyExist(rawKey0));
    ASSERT_FALSE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_TRUE(env_->keyExist(rRawKey0.first));
    ASSERT_FALSE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{lock0.get_props()[1].toString()};
    ASSERT_EQ(actualProps, expect);

    // ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
}

/**
 * @brief neighbor good lock + bad lock + edge
 */
TEST_F(TossTest, neighbors_test_10) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);

    auto lockKey0 = env_->insertLock(lock0, true);
    auto lockKey1 = env_->insertLock(lock1, false);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    auto rawKey0 = env_->makeRawKey(edges[0].get_key()).first;
    auto rawKey1 = env_->makeRawKey(edges[1].get_key()).first;

    // auto rawKey0 = env_->insertEdge(edges[0]);
    env_->syncAddEdge(edges[1]);

    auto props = env_->getNeiProps(edges);
    auto svec = TossTestUtils::splitNeiResults(props);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_TRUE(env_->keyExist(rawKey0));
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_TRUE(env_->keyExist(rRawKey0.first));
    ASSERT_TRUE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{
        lock0.get_props()[1].toString(),
        edges[1].get_props()[1].toString()
    };
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, num));
}

/**
 * @brief neighbor bad lock + edge
 */
TEST_F(TossTest, neighbors_test_11) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);

    auto lockKey0 = env_->insertLock(lock0, false);
    // auto lockKey1 = env_->insertLock(lock1, false);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    // LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    auto rawKey0 = env_->makeRawKey(edges[0].get_key()).first;
    auto rawKey1 = env_->makeRawKey(edges[1].get_key()).first;

    env_->syncAddEdge(edges[1]);

    auto props = env_->getNeiProps(edges);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    // ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_FALSE(env_->keyExist(rawKey0));
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_FALSE(env_->keyExist(rRawKey0.first));
    ASSERT_TRUE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto svec = TossTestUtils::splitNeiResults(props);
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{edges[1].get_props()[1].toString()};
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, num-1));
}

/**
 * @brief neighbor bad lock + good lock
 */
TEST_F(TossTest, neighbors_test_12) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);


    auto rawKey0 = env_->makeRawKey(edges[0].get_key()).first;
    auto rawKey1 = env_->makeRawKey(edges[1].get_key()).first;

    // env_->syncAddEdge(edges[1]);
    auto lockKey0 = env_->insertLock(lock0, false);
    auto lockKey1 = env_->insertLock(lock1, true);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    auto props = env_->getNeiProps(edges);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_FALSE(env_->keyExist(rawKey0));
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_FALSE(env_->keyExist(rRawKey0.first));
    ASSERT_TRUE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto svec = TossTestUtils::splitNeiResults(props);
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{lock1.get_props()[1].toString()};
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, num-1));
}

/**
 * @brief neighbor bad lock + good lock + edge
 */
TEST_F(TossTest, neighbors_test_13) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);


    auto rawKey0 = env_->makeRawKey(edges[0].get_key()).first;
    auto rawKey1 = env_->makeRawKey(edges[1].get_key()).first;

    env_->syncAddEdge(edges[1]);
    auto lockKey0 = env_->insertLock(lock0, false);
    auto lockKey1 = env_->insertLock(lock1, true);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    auto props = env_->getNeiProps(edges);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_FALSE(env_->keyExist(rawKey0));
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_FALSE(env_->keyExist(rRawKey0.first));
    ASSERT_TRUE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto svec = TossTestUtils::splitNeiResults(props);
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{lock1.get_props()[1].toString()};
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, num-1));
}

/**
 * @brief neighbor bad lock + bad lock
 */
TEST_F(TossTest, neighbors_test_14) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);

    auto rawKey0 = env_->makeRawKey(edges[0].get_key()).first;
    auto rawKey1 = env_->makeRawKey(edges[1].get_key()).first;

    // env_->syncAddEdge(edges[1]);

    auto lockKey0 = env_->insertLock(lock0, false);
    auto lockKey1 = env_->insertLock(lock1, false);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    auto props = env_->getNeiProps(edges);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_FALSE(env_->keyExist(rawKey0));
    ASSERT_FALSE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_FALSE(env_->keyExist(rRawKey0.first));
    ASSERT_FALSE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto svec = TossTestUtils::splitNeiResults(props);
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{};
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, 0));
}

/**
 * @brief neighbor bad lock + bad lock + edge
 */
TEST_F(TossTest, neighbors_test_15) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);

    auto rawKey0 = env_->makeRawKey(edges[0].get_key()).first;
    auto rawKey1 = env_->makeRawKey(edges[1].get_key()).first;

    env_->syncAddEdge(edges[1]);

    auto lockKey0 = env_->insertLock(lock0, false);
    auto lockKey1 = env_->insertLock(lock1, false);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    auto props = env_->getNeiProps(edges);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_FALSE(env_->keyExist(rawKey0));
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_FALSE(env_->keyExist(rRawKey0.first));
    ASSERT_TRUE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto svec = TossTestUtils::splitNeiResults(props);
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{edges[1].get_props()[1].toString()};
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, 1));
}

/**
 * @brief neighbor good lock + neighbor edge + good lock + edge
 */
TEST_F(TossTest, neighbors_test_16) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);

    auto rawKey0 = env_->makeRawKey(edges[0].get_key()).first;
    auto rawKey1 = env_->makeRawKey(edges[1].get_key()).first;

    env_->syncAddEdge(edges[1]);

    auto lockKey0 = env_->insertLock(lock0, true);
    auto lockKey1 = env_->insertLock(lock1, true);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    auto props = env_->getNeiProps(edges);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_TRUE(env_->keyExist(rawKey0));
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_TRUE(env_->keyExist(rRawKey0.first));
    ASSERT_TRUE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto svec = TossTestUtils::splitNeiResults(props);
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{lock0.get_props()[1].toString(), lock1.get_props()[1].toString()};
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, 2));
}

/**
 * @brief  neighbor good lock + neighbor edge + bad lock + edge
 */
TEST_F(TossTest, neighbors_test_17) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);

    auto rawKey0 = env_->makeRawKey(edges[0].get_key()).first;
    auto rawKey1 = env_->makeRawKey(edges[1].get_key()).first;

    env_->syncAddEdge(edges[1]);

    auto lockKey0 = env_->insertLock(lock0, true);
    auto lockKey1 = env_->insertLock(lock1, false);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    auto props = env_->getNeiProps(edges);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_TRUE(env_->keyExist(rawKey0));
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_TRUE(env_->keyExist(rRawKey0.first));
    ASSERT_TRUE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto svec = TossTestUtils::splitNeiResults(props);
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{
        lock0.get_props()[1].toString(),
        edges[1].get_props()[1].toString()
    };
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, 2));
}

/**
 * @brief  neighbor bad lock + neighbor edge + good lock + edge
 */
TEST_F(TossTest, neighbors_test_18) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);

    auto rawKey0 = env_->makeRawKey(edges[0].get_key()).first;
    auto rawKey1 = env_->makeRawKey(edges[1].get_key()).first;

    env_->syncAddEdge(edges[0]);
    env_->syncAddEdge(edges[1]);

    auto lockKey0 = env_->insertLock(lock0, false);
    auto lockKey1 = env_->insertLock(lock1, true);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    auto props = env_->getNeiProps(edges);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_TRUE(env_->keyExist(rawKey0));
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_TRUE(env_->keyExist(rRawKey0.first));
    ASSERT_TRUE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto svec = TossTestUtils::splitNeiResults(props);
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{
        edges[0].get_props()[1].toString(),
        lock1.get_props()[1].toString()
    };
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, 2));
}

/**
 * @brief neighbor bad lock + neighbor edge + bad lock + edge
 */
TEST_F(TossTest, neighbors_test_19) {
    LOG(INFO) << "b_=" << b_;
    auto num = 2;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);
    ASSERT_EQ(edges.size(), 2);

    auto lock0 = env_->dupEdge(edges[0]);
    auto lock1 = env_->dupEdge(edges[1]);

    auto rawKey0 = env_->makeRawKey(edges[0].get_key()).first;
    auto rawKey1 = env_->makeRawKey(edges[1].get_key()).first;

    env_->syncAddEdge(edges[0]);
    env_->syncAddEdge(edges[1]);

    auto lockKey0 = env_->insertLock(lock0, false);
    auto lockKey1 = env_->insertLock(lock1, false);
    LOG(INFO) << "lockKey0 hexlify = " << folly::hexlify(lockKey0);
    LOG(INFO) << "lockKey1 hexlify = " << folly::hexlify(lockKey1);

    auto props = env_->getNeiProps(edges);

    // step 1st: lock key not exist
    ASSERT_FALSE(env_->keyExist(lockKey0));
    ASSERT_FALSE(env_->keyExist(lockKey1));

    // step 2nd: edge key exist
    ASSERT_TRUE(env_->keyExist(rawKey0));
    ASSERT_TRUE(env_->keyExist(rawKey1));

    // step 3rd: reverse edge key exist
    auto rRawKey0 = env_->makeRawKey(env_->reverseEdgeKey(lock0.get_key()));
    auto rRawKey1 = env_->makeRawKey(env_->reverseEdgeKey(lock1.get_key()));

    ASSERT_TRUE(env_->keyExist(rRawKey0.first));
    ASSERT_TRUE(env_->keyExist(rRawKey1.first));

    // step 4th: the get neighbors result is from lock
    auto svec = TossTestUtils::splitNeiResults(props);
    auto actualProps = env_->extractStrVals(svec);
    decltype(actualProps) expect{
        edges[0].get_props()[1].toString(),
        edges[1].get_props()[1].toString()
    };
    ASSERT_EQ(actualProps, expect);

    ASSERT_TRUE(TossTestUtils::compareSize(svec, 2));
}

/**
 * @brief
 */
TEST_F(TossTest, get_props_test_0) {
    LOG(INFO) << "getProps_test_0 b_=" << b_;
    auto num = 1;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);

    LOG(INFO) << "going to add edge:" << edges.back().get_props().back();
    auto code = env_->syncAddMultiEdges(edges, kNotUseToss);
    ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);

    auto vvec = env_->getProps(edges[0]);
    LOG(INFO) << "vvec.size()=" << vvec.size();
    for (auto& val : vvec) {
        LOG(INFO) << "val.toString()=" << val.toString();
    }
}


/**
 * @brief
 */
TEST_F(TossTest, get_props_test_1) {
    LOG(INFO) << __func__ << " b_=" << b_;
    auto num = 1;
    std::vector<cpp2::NewEdge> edges = env_->generateMultiEdges(num, b_);

    LOG(INFO) << "going to add edge:" << edges.back().get_props().back();
    auto code = env_->syncAddMultiEdges(edges, kUseToss);
    ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);

    auto vvec = env_->getProps(edges[0]);
    LOG(INFO) << "vvec.size()=" << vvec.size();
    for (auto& val : vvec) {
        LOG(INFO) << "val.toString()=" << val.toString();
    }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    FLAGS_heartbeat_interval_secs = 1;

    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, false);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
