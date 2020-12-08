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
constexpr bool kNotToss = false;

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

    LOG(INFO) << "going to add edge:" << edges.back().props.back();
    auto code = env_->syncAddMultiEdges(edges, kNotToss);
    ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);

    props = env_->getNeiProps(startWith);

    LOG(INFO) << "props.size()=" << props.size();
    for (auto& prop : props) {
        LOG(INFO) << "prop: " << prop;
    }
    EXPECT_EQ(env_->countSquareBrackets(props), 1);
}

TEST_F(TossTest, ADD_ONES_EDGE) {
    auto num = 1U;
    auto values = TossTestUtils::genValues(num);

    int32_t __e = static_cast<int32_t>(TossTestEnum::ADD_ONES_EDGE) * 100;
    int64_t src = env_->getSpaceId() * __e * kPart;

    std::vector<cpp2::NewEdge> edges;
    edges.emplace_back(env_->generateEdge(src, 0, values.back(), src + kPart));

    std::vector<cpp2::NewEdge> startWith{edges[0]};
    auto props = env_->getNeiProps(startWith);
    EXPECT_EQ(env_->countSquareBrackets(props), 0);

    LOG(INFO) << "going to add edge:" << edges.back().props.back();
    LOG(INFO) << "going to add edge:" << TossTestUtils::hexEdgeId(edges.back().key);
    auto code = env_->syncAddMultiEdges(edges, kUseToss);
    ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);

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

    int32_t __e = static_cast<int32_t>(TossTestEnum::TWO_EDGES_CASE_1) * 100;
    int64_t src = env_->getSpaceId() * __e * kPart;

    std::vector<cpp2::NewEdge> edges;
    edges.emplace_back(env_->generateEdge(src, 0, values[0], src + kPart));
    edges.emplace_back(env_->generateEdge(src, 0, values[1], src + kPart*2));

    std::vector<cpp2::NewEdge> startWith{edges[0]};
    auto props = env_->getNeiProps(startWith);
    EXPECT_EQ(env_->countSquareBrackets(props), 0);

    auto code = env_->syncAddMultiEdges(edges, kUseToss);
    LOG_IF(FATAL, code != cpp2::ErrorCode::SUCCEEDED)
        << "fatal code=" << static_cast<int32_t>(code);

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
    LOG_IF(FATAL, code != cpp2::ErrorCode::SUCCEEDED)
        << "fatal code=" << static_cast<int32_t>(code);

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
    ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);

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
    LOG_IF(FATAL, code != cpp2::ErrorCode::SUCCEEDED)
        << "fatal code=" << static_cast<int32_t>(code);

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
    LOG_IF(FATAL, code != cpp2::ErrorCode::SUCCEEDED)
        << "fatal code=" << static_cast<int32_t>(code);

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
    LOG_IF(FATAL, code != cpp2::ErrorCode::SUCCEEDED)
        << "fatal code=" << static_cast<int32_t>(code);

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

TEST_F(TossTest, internal_storage_client_test) {
    auto num = 1U;
    std::vector<cpp2::NewEdge> edges = env_->generateNEdges(num);

    // auto interClient =

    std::vector<cpp2::NewEdge> first{edges[0]};

    auto code = env_->syncAddMultiEdges(edges, kUseToss);
    ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);

    auto props = env_->getNeiProps(first);

    auto svec = TossTestUtils::splitNeiResults(props);
    EXPECT_EQ(svec.size(), num);
    // TossTestUtils::logIfSizeNotAsExpect(svec.size(), num);
}

/*
TEST_F(TossTest, base_component_test_1) {
    auto num = 1U;
    std::vector<cpp2::NewEdge> edges = env_->generateNEdges(num);

    std::vector<cpp2::NewEdge> first{edges[0]};

    auto code = env_->syncAddMultiEdges(edges, kUseToss);
    ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);

    auto props = env_->getNeiProps(first);

    auto svec = TossTestUtils::splitNeiResults(props);
    EXPECT_EQ(svec.size(), num);
    // TossTestUtils::logIfSizeNotAsExpect(svec.size(), num);

    auto lockKey = env_->insertLock(edges[0]);
    LOG(INFO) << "base_component_test_1 lockKey = " << lockKey;
}
*/

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    FLAGS_heartbeat_interval_secs = 1;

    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, false);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
