/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "TossEnvironment.h"

#define LOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

constexpr int kInEdgeFail = 19;
constexpr int kOutEdgeFail = 29;
// constexpr int kRemoveLockFail = 37;
constexpr int kSucceeded = 99;

// only used in Toss Error Test, may remove after that test removed.
static std::string extractSingleProps(const std::string& props) {
    size_t cnt = 0;
    if (props.empty()) {
        return 0;
    }
    size_t p = props.find(',', 0);
    while (cnt < 3 && p != std::string::npos) {
        ++cnt;
        ++p;
        p = props.find(',', p);
    }
    return props.substr(p+1);
}

namespace nebula {
namespace storage {

using StorageClient = storage::GraphStorageClient;

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

        env_ = TossEnvironment::getInstance(gMetaName, gMetaPort);
        LOG(INFO) << "before init(gSpaceName..)";
        env_->init(gSpaceName, gPart, gReplica, colDefs_);
    }

protected:
    TossEnvironment* env_{nullptr};
    std::string     gMetaName{"hp-server"};
    int             gMetaPort = 6500;

    std::string     gSpaceName{"test"};
    int             gPart = 2;
    int             gReplica = 3;
    static int      src_;

    std::vector<meta::cpp2::PropertyType>   types_;
    std::vector<meta::cpp2::ColumnDef>      colDefs_;
    std::vector<nebula::Value>              values_;
    boost::uuids::random_generator          gen_;

    // make every sub test use different srcVid
    void incrSrc(int n) {
        src_ += (n / gPart + 5) * gPart;
    }

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

int TossTest::src_ = 5000;


TEST_F(TossTest, getProps_happyPath) {
    boost::uuids::random_generator gen;
    boost::uuids::uuid u = gen();
    std::vector<nebula::Value> val1(types_.size());
    val1[0].setInt(2048);
    val1[1].setStr("bi-edge:"+ boost::uuids::to_string(u));

    std::vector<cpp2::NewEdge> edges;
    auto idst = src_ + gPart;
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));

    auto vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), "__EMPTY__");

    for (auto& edge : edges) {
        LOG(INFO) << "going to add edge:" << edge.props.back();
        env_->addEdgeAsync(edge);
    }

    vals = env_->getProps(edges.back());

    LOG(INFO) << "vals.back().toString(): " << vals.back().toString();
    EXPECT_EQ(vals.back().toString(), val1.back());
    incrSrc(10);
}


TEST_F(TossTest, getProps_inEdgeFailed) {
    boost::uuids::uuid u = gen_();
    std::vector<nebula::Value> val1(types_.size());
    val1[0].setInt(0);
    val1[1].setStr("command:"+ boost::uuids::to_string(u));

    std::vector<nebula::Value> val2(types_.size());
    val2[0].setInt(1024);
    val2[1].setStr("in-edge:" + boost::uuids::to_string(u));

    std::vector<cpp2::NewEdge> edges;
    auto idst = src_ + gPart;
    edges.emplace_back(env_->generateEdge(src_-gPart, kInEdgeFail, val1, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

    for (auto& edge : edges) {
        LOG(INFO) << "going to add edge:" << edge.props.back();
        env_->addEdgeAsync(edge);;
    }

    auto vals = env_->getProps(edges.back());
    EXPECT_EQ(TossEnvironment::extractSingleProps(vals.back().toString()),
            "__EMPTY__");

    // double check
    vals = env_->getProps(edges.back());
    EXPECT_EQ(TossEnvironment::extractSingleProps(vals.back().toString()),
            "__EMPTY__");
    incrSrc(10);
}


TEST_F(TossTest, getProps_outEdgeFailed) {
    boost::uuids::uuid u = gen_();
    std::vector<nebula::Value> val1(types_.size());
    val1[0].setInt(0);
    val1[1].setStr("command:"+ boost::uuids::to_string(u));

    std::vector<nebula::Value> val2(types_.size());
    val2[0].setInt(1024);
    val2[1].setStr("out-edge:" + boost::uuids::to_string(u));

    std::vector<cpp2::NewEdge> edges;
    auto idst = src_ + gPart;
    edges.emplace_back(env_->generateEdge(src_-gPart, kOutEdgeFail, val1, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

    for (auto& edge : edges) {
        LOG(INFO) << "going to add edge:" << edge.props.back();
        env_->addEdgeAsync(edge);;
    }

    auto vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val2.back().toString());

    // double check
    vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val2.back().toString());
    incrSrc(10);
}

TEST_F(TossTest, getProps_lockBeforeEdge) {
    boost::uuids::uuid u = gen_();
    std::vector<nebula::Value> val1(types_.size());
    val1[0].setInt(0);
    val1[1].setStr("command:"+ boost::uuids::to_string(u));

    std::vector<nebula::Value> val2(types_.size());
    val2[0].setInt(1024);
    val2[1].setStr("out-edge:" + boost::uuids::to_string(u));

    std::vector<cpp2::NewEdge> edges;
    auto idst = src_ + gPart;
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));
    edges.emplace_back(env_->generateEdge(src_-gPart, kOutEdgeFail, val1, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

    for (auto& edge : edges) {
        LOG(INFO) << "going to add edge:" << edge.props.back();
        env_->addEdgeAsync(edge);;
    }

    auto vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val2.back().toString());

    // double check
    vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val2.back().toString());
    incrSrc(10);
}


TEST_F(TossTest, getProps_failedLockBeforeEdge) {
    boost::uuids::uuid u = gen_();
    std::vector<nebula::Value> val1(types_.size());
    val1[0].setInt(0);
    val1[1].setStr("command:"+ boost::uuids::to_string(u));

    std::vector<nebula::Value> val2(types_.size());
    val2[0].setInt(1024);
    val2[1].setStr("out-edge:" + boost::uuids::to_string(u));

    std::vector<cpp2::NewEdge> edges;
    auto idst = src_ + gPart;
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));
    edges.emplace_back(env_->generateEdge(src_-gPart, kInEdgeFail, val1, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

    for (auto& edge : edges) {
        LOG(INFO) << "going to add edge:" << edge.props.back();
        env_->addEdgeAsync(edge);;
    }

    auto vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val1.back().toString());

    // double check
    vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val1.back().toString());
    incrSrc(10);
}

TEST_F(TossTest, getProps_badLockbadLockAndEdge) {
    boost::uuids::uuid u = gen_();
    std::vector<nebula::Value> val1(types_.size());
    val1[0].setInt(0);
    val1[1].setStr("command:"+ boost::uuids::to_string(u));

    std::vector<nebula::Value> val2(types_.size());
    val2[0].setInt(1024);
    val2[1].setStr("out-edge:" + boost::uuids::to_string(u));

    std::vector<cpp2::NewEdge> edges;
    auto idst = src_ + gPart;
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));
    edges.emplace_back(env_->generateEdge(src_-gPart, kInEdgeFail, val2, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));
    edges.emplace_back(env_->generateEdge(src_-gPart, kInEdgeFail, val2, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

    for (auto& edge : edges) {
        LOG(INFO) << "going to add edge:" << edge.props.back();
        env_->addEdgeAsync(edge);;
    }

    auto vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val1.back().toString());

    // double check
    vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val1.back().toString());
    incrSrc(10);
}

TEST_F(TossTest, getProps_badLockGoodLockAnd) {
    boost::uuids::uuid u = gen_();
    std::vector<nebula::Value> val1(types_.size());
    val1[0].setInt(1000);
    val1[1].setStr("bad lock:"+ boost::uuids::to_string(u));

    std::vector<nebula::Value> val2(types_.size());
    val2[0].setInt(2000);
    val2[1].setStr("good lock:" + boost::uuids::to_string(u));

    std::vector<nebula::Value> val3(types_.size());
    val3[0].setInt(3000);
    val3[1].setStr("edge:" + boost::uuids::to_string(u));

    std::vector<cpp2::NewEdge> edges;
    auto idst = src_ + gPart;
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val3, idst));
    edges.emplace_back(env_->generateEdge(src_-gPart, kOutEdgeFail, val2, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));
    edges.emplace_back(env_->generateEdge(src_-gPart, kInEdgeFail, val1, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));

    for (auto& edge : edges) {
        LOG(INFO) << "going to add edge:" << edge.props.back();
        env_->addEdgeAsync(edge);;
    }

    auto vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val2.back().toString());

    // double check
    vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val2.back().toString());
    incrSrc(10);
}

TEST_F(TossTest, getProps_goodLockBadLockAndEdge) {
    boost::uuids::uuid u = gen_();
    std::vector<nebula::Value> val1(types_.size());
    val1[0].setInt(1000);
    val1[1].setStr("bad lock:"+ boost::uuids::to_string(u));

    std::vector<nebula::Value> val2(types_.size());
    val2[0].setInt(1000);
    val2[1].setStr("good lock:" + boost::uuids::to_string(u));

    std::vector<nebula::Value> val3(types_.size());
    val3[0].setInt(3000);
    val3[1].setStr("edge:" + boost::uuids::to_string(u));

    std::vector<cpp2::NewEdge> edges;
    auto idst = src_ + gPart;
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val3, idst));
    edges.emplace_back(env_->generateEdge(src_-gPart, kInEdgeFail, val1, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));
    edges.emplace_back(env_->generateEdge(src_-gPart, kOutEdgeFail, val2, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

    for (auto& edge : edges) {
        LOG(INFO) << "going to add edge:" << edge.props.back();
        env_->addEdgeAsync(edge);;
    }

    auto vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val2.back().toString());

    // double check
    vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val2.back().toString());
    incrSrc(10);
}

TEST_F(TossTest, getProps_goodLockGoodLockAndEdge) {
    boost::uuids::uuid u = gen_();

    std::vector<nebula::Value> val2(types_.size());
    val2[0].setInt(2000);
    val2[1].setStr("good lock:" + boost::uuids::to_string(u));

    std::vector<nebula::Value> val3(types_.size());
    val3[0].setInt(3000);
    val3[1].setStr("edge:" + boost::uuids::to_string(u));

    std::vector<cpp2::NewEdge> edges;
    auto idst = src_ + gPart;
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val3, idst));
    edges.emplace_back(env_->generateEdge(src_-gPart, kOutEdgeFail, val2, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));
    edges.emplace_back(env_->generateEdge(src_-gPart, kInEdgeFail, val2, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

    for (auto& edge : edges) {
        LOG(INFO) << "going to add edge:" << edge.props.back();
        env_->addEdgeAsync(edge);;
    }

    auto vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val2.back().toString());

    // double check
    vals = env_->getProps(edges.back());
    EXPECT_EQ(vals.back().toString(), val2.back().toString());
    incrSrc(10);
}


/* ****************************************************
 *                 getNeighbors test
 * **************************************************** */
TEST_F(TossTest, getNeighborsTest_happyPath) {
    std::vector<nebula::Value> values(types_.size());
    values[0].setInt(65536);
    values[1].setFloat(3.14f);
    values[2].setStr("feng-timo");

    std::vector<cpp2::NewEdge> edges;
    auto idst = kSum - src_;
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, values, idst));
    env_->addEdgeAsync(edges.back(), false);

    auto sf = env_->getNeighborsWrapper(edges);
    sf.wait();

    ASSERT_TRUE(sf.valid());
    StorageRpcResponse<cpp2::GetNeighborsResponse> rpc = sf.value();
    ASSERT_TRUE(rpc.succeeded());

    std::vector<cpp2::GetNeighborsResponse> resps = rpc.responses();
    for (cpp2::GetNeighborsResponse& resp : resps) {
        nebula::DataSet ds = resp.vertices;
        LOG(INFO) << "ds.colNames.size()=" << ds.colNames.size();
        LOG(INFO) << "ds.rows.size()=" << ds.rows.size();
        LOG(INFO) << TossTestUtils::dumpDataSet(ds);
    }

    ASSERT_EQ(resps.size(), 1);
}


/*
 * assume there is only one dangle lock in kvstore
 * check getNeighbors will get this edge successfully
 * */
TEST_F(TossTest, committedInEdgeTest_noPrevNoNext) {
    auto edgeCnt = 1;

    boost::uuids::random_generator gen;
    boost::uuids::uuid u = gen();
    std::vector<nebula::Value> values(types_.size());
    values[0].setInt(65536);
    values[1].setStr("committedInEdgeTest_noPrevNoNext:" + boost::uuids::to_string(u));

    LOG(INFO) << "add edge: " << values[1];

    std::vector<cpp2::NewEdge> edges;
    auto idst = kSum - src_;
    edges.emplace_back(env_->generateEdge(src_ - gPart, kOutEdgeFail, values, idst));
    edges.emplace_back(env_->generateEdge(src_, kSucceeded, values, idst));

    // step 2 of 4, check there is no prev edges
    auto nGetNeighbors = env_->getCountOfNeighbors(edges);
    ASSERT_EQ(nGetNeighbors, 0);

    // step 3 of 4, add edges
    for (auto& edge : edges) {
        env_->addEdgeAsync(edge);;
    }

    // step 4 of 4, check get neighbors will get edge successfully
    EXPECT_EQ(env_->getCountOfNeighbors(edges), edgeCnt);

    // additional test, just call get neighbors again
    EXPECT_EQ(env_->getCountOfNeighbors(edges), edgeCnt);
    incrSrc(10);
}


// TEST_F(TossTest, committedInEdgeTest_noPrevSameEdgeNext) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid id = gen();
//     std::vector<nebula::Value> val1(types_.size());
//     val1[0].setInt(1024);
//     val1[1].setStr("noPrevSameEdgeNext-bi-edge:" + boost::uuids::to_string(id));
//     LOG(INFO) << "add edge: " << val1[1];

//     std::vector<nebula::Value> val2(types_.size());
//     val2[0].setInt(2048);
//     val2[1].setStr("noPrevSameEdgeNext-in-edge:" + boost::uuids::to_string(id));
//     LOG(INFO) << "add edge: " << val2[1];

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kOutEdgeFail, val1, idst - gPart));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

//     // step 2 of 4, add edges
//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     edges.erase(edges.begin()+1);

//     auto props = env_->getNeiProps(edges);
//     // LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(env_->getCountOfNeighbors(edges), 1);

//     auto svec = TossEnvironment::extractProps(props);
//     EXPECT_EQ(svec.size(), 1);
//     EXPECT_EQ(TossEnvironment::extractSingleProps(svec.back()),
//               TossTestUtils::concatValues(val2));

//     // additional test, just call get neighbors again
//     EXPECT_EQ(env_->getCountOfNeighbors(edges), 1);
//     incrSrc(10);
// }


// TEST_F(TossTest, committedInEdgeTest_SameEdgePrevNoNext) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     std::vector<nebula::Value> valCtl(types_.size());
//     valCtl[0].setInt(1024);
//     valCtl[1].setStr("bi-edge:"+ boost::uuids::to_string(u));
//     LOG(INFO) << "add edge: " << valCtl[1];

//     std::vector<nebula::Value> valExpr(types_.size());
//     valExpr[0].setInt(2048);
//     valExpr[1].setStr("in-edge:" + boost::uuids::to_string(u));
//     LOG(INFO) << "add edge: " << valExpr[1];

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kOutEdgeFail, valCtl, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, valExpr, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, valCtl, idst));
//     // step 2 of 4, add edges
//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     edges.erase(edges.begin());

//     std::string props = env_->getOutNeighborsProps(edges);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), 1);

//     auto svec = TossEnvironment::extractProps(props);
//     ASSERT_EQ(svec.size(), 1);
//     EXPECT_EQ(TossEnvironment::extractSingleProps(svec.back()),
//               TossTestUtils::concatValues(valCtl));

//     // additional test, just call get neighbors again
//     EXPECT_EQ(env_->getCountOfNeighbors(edges), 1);
//     incrSrc(10);
// }


// TEST_F(TossTest, committedInEdgeTest_neighborEdgePrevNoNext) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     std::vector<nebula::Value> valCtl(types_.size());
//     valCtl[0].setInt(1024);
//     valCtl[1].setStr("bi-edge:"+ boost::uuids::to_string(u));

//     std::vector<nebula::Value> valExpr(types_.size());
//     valExpr[0].setInt(2048);
//     valExpr[1].setStr("in-edge:" + boost::uuids::to_string(u));

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, valCtl, idst-1));
//     edges.emplace_back(env_->generateEdge(src_-gPart, kOutEdgeFail, valCtl, idst - gPart));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, valExpr, idst));

//     // step 2 of 4, add edges
//     for (auto& edge : edges) {
//         LOG(INFO) << "going to add edge:" << edge.props.back();
//         env_->addEdgeAsync(edge);;
//     }

//     std::vector<cpp2::NewEdge> edgesToSearch{edges[2]};

//     std::string props = env_->getOutNeighborsProps(edgesToSearch);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), 2);

//     auto svec = TossEnvironment::extractProps(props);
//     EXPECT_EQ(svec.size(), 2);
//     EXPECT_EQ(TossEnvironment::extractSingleProps(svec.back()),
//               TossTestUtils::concatValues(valExpr));

//     // additional test, just call get neighbors again
//     EXPECT_EQ(env_->getCountOfNeighbors(edgesToSearch), 2);
//     incrSrc(10);
// }

// TEST_F(TossTest, committedInEdgeTest_sameCommitLockPrevNoNext) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     boost::uuids::uuid u2 = gen();
//     std::vector<nebula::Value> val0(types_.size());
//     val0[0].setInt(2048);
//     val0[1].setStr("cmd-edge-committed:" + boost::uuids::to_string(u2));
//     LOG(INFO) << "add edge: " << val0[1];

//     std::vector<nebula::Value> val1(types_.size());
//     val1[0].setInt(1024);
//     val1[1].setStr("in-edge1:"+ boost::uuids::to_string(u));
//     LOG(INFO) << "add edge: " << val1[1];

//     std::vector<nebula::Value> val2(types_.size());
//     val2[0].setInt(2048);
//     val2[1].setStr("in-edge2:" + boost::uuids::to_string(u));
//     LOG(INFO) << "add edge: " << val2[1];

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kOutEdgeFail, val0, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kOutEdgeFail, val0, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

//     // step 2 of 4, add edges
//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     std::vector<cpp2::NewEdge> edgesToSearch{edges[1]};

//     int expectNum = 1;

//     std::string props = env_->getOutNeighborsProps(edgesToSearch);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), expectNum);

//     auto svec = TossEnvironment::extractProps(props);
//     EXPECT_EQ(svec.size(), expectNum);
//     EXPECT_EQ(TossEnvironment::extractSingleProps(svec.back()),
//               TossTestUtils::concatValues(val2));

//     // additional test, just call get neighbors again
//     EXPECT_EQ(env_->getCountOfNeighbors(edgesToSearch), expectNum);
//     incrSrc(10);
// }



// TEST_F(TossTest, committedInEdgeTest_sameUnommitLockPrevNoNext) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     boost::uuids::uuid u2 = gen();
//     std::vector<nebula::Value> val0(types_.size());
//     val0[0].setInt(2048);
//     val0[1].setStr("cmd-edge-committed:" + boost::uuids::to_string(u2));
//     LOG(INFO) << "add edge: " << val0[1];

//     std::vector<nebula::Value> val1(types_.size());
//     val1[0].setInt(1024);
//     val1[1].setStr("in-edge:"+ boost::uuids::to_string(u));
//     LOG(INFO) << "add edge: " << val1[1];

//     std::vector<nebula::Value> val2(types_.size());
//     val2[0].setInt(2048);
//     val2[1].setStr("in-edge-uncommitted:" + boost::uuids::to_string(u));
//     LOG(INFO) << "add edge: " << val2[1];

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kOutEdgeFail, val0, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kInEdgeFail, val0, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

//     // step 2 of 4, add edges
//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     std::vector<cpp2::NewEdge> edgesToSearch{edges[1]};

//     int expectNum = 1;

//     std::string props = env_->getOutNeighborsProps(edgesToSearch);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), expectNum);

//     auto svec = TossEnvironment::extractProps(props);
//     ASSERT_EQ(svec.size(), expectNum);
//     EXPECT_EQ(TossEnvironment::extractSingleProps(svec.back()),
//               TossTestUtils::concatValues(val1));

//     // additional test, just call get neighbors again
//     EXPECT_EQ(env_->getCountOfNeighbors(edgesToSearch), expectNum);
//     incrSrc(10);
// }


// TEST_F(TossTest, committedInEdgeTest_neighborCommitLock) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     boost::uuids::uuid u2 = gen();
//     std::vector<nebula::Value> val0(types_.size());
//     val0[0].setInt(2048);
//     val0[1].setStr("cmd-edge-committed:" + boost::uuids::to_string(u2));


//     std::vector<nebula::Value> val1(types_.size());
//     val1[0].setInt(1024);
//     val1[1].setStr("in-edge:"+ boost::uuids::to_string(u));

//     std::vector<nebula::Value> val2(types_.size());
//     val2[0].setInt(2048);
//     val2[1].setStr("in-edge-uncommitted:" + boost::uuids::to_string(u));

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kOutEdgeFail, val0, idst));
//     LOG(INFO) << "add edge: " << val0.back();
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kSucceeded, val1, idst));
//     LOG(INFO) << "add edge: " << val1.back();
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kOutEdgeFail, val0, idst));
//     LOG(INFO) << "add edge: " << val0.back();
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));
//     LOG(INFO) << "add edge: " << val2.back();

//     // step 2 of 4, add edges
//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     std::vector<cpp2::NewEdge> edgesToSearch{edges.back()};

//     int expectNum = 1;

//     std::string props = env_->getOutNeighborsProps(edgesToSearch);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), expectNum);

//     auto svec = TossEnvironment::extractProps(props);
//     ASSERT_EQ(svec.size(), expectNum);
//     EXPECT_EQ(TossEnvironment::extractSingleProps(svec.back()),
//               TossTestUtils::concatValues(val2));

//     // additional test, just call get neighbors again
//     EXPECT_EQ(env_->getCountOfNeighbors(edgesToSearch), expectNum);
//     incrSrc(10);
// }


// TEST_F(TossTest, committedInEdgeTest_neighborUncommitLock) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     boost::uuids::uuid u2 = gen();

//     std::vector<nebula::Value> val0(types_.size());
//     val0[0].setInt(2048);
//     val0[1].setStr("cmd-edge-committed:" + boost::uuids::to_string(u2));

//     std::vector<nebula::Value> val1(types_.size());
//     val1[0].setInt(1024);
//     val1[1].setStr("in-edge:"+ boost::uuids::to_string(u));

//     std::vector<nebula::Value> val2(types_.size());
//     val2[0].setInt(2048);
//     val2[1].setStr("in-edge-uncommitted:" + boost::uuids::to_string(u));

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kOutEdgeFail, val0, idst));
//     LOG(INFO) << "add edge: " << val0.back();
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kSucceeded, val1, idst));
//     LOG(INFO) << "add edge: " << val1.back();
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kInEdgeFail, val0, idst));
//     LOG(INFO) << "add edge: " << val0.back();
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));
//     LOG(INFO) << "add edge: " << val2.back();

//     // step 2 of 4, add edges
//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     std::vector<cpp2::NewEdge> edgesToSearch{edges.back()};

//     int expectNum = 0;

//     std::string props = env_->getOutNeighborsProps(edgesToSearch);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), expectNum);

//     auto svec = TossEnvironment::extractProps(props);
//     ASSERT_EQ(svec.size(), 0);
//     incrSrc(10);
// }


// /*
//  * committed in-edge
//  * another common edge
//  * */
// TEST_F(TossTest, committedInEdgeTest_noPrevDiffEdgeNext) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     std::vector<nebula::Value> valCtl(types_.size());
//     valCtl[0].setInt(1024);
//     valCtl[1].setStr("bi-edge:"+ boost::uuids::to_string(u));
//     LOG(INFO) << "add edge: " << valCtl[1];

//     std::vector<nebula::Value> valExp(types_.size());
//     valExp[0].setInt(2048);
//     valExp[1].setStr("in-edge:" + boost::uuids::to_string(u));
//     LOG(INFO) << "add edge: " << valExp[1];

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, valCtl, idst));
//     edges.emplace_back(env_->generateEdge(src_ + gPart, kOutEdgeFail, valCtl, idst - gPart));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, valExp, idst));
//     // add edges
//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     edges.erase(edges.begin()+1);

//     std::string props = env_->getOutNeighborsProps(edges);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), 1);

//     auto svec = TossEnvironment::extractProps(props);
//     EXPECT_EQ(svec.size(), 1);
//     EXPECT_EQ(TossEnvironment::extractSingleProps(svec.back()),
//               TossTestUtils::concatValues(valExp));

//     // additional test, just call get neighbors again
//     EXPECT_EQ(env_->getCountOfNeighbors(edges), 1);
//     incrSrc(10);
// }

// /*
//  * commit one edge using Toss
//  * failed before in-edge committed
//  * check getNeighbors will not get this edge
//  * */
// TEST_F(TossTest, uncommittedInEdgeTest_noPrevNoNext) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();

//     std::vector<nebula::Value> vals1(types_.size());
//     vals1[0].setInt(65536);
//     vals1[1].setStr(boost::uuids::to_string(u));
//     LOG(INFO) << "add edge: " << vals1[1];

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kInEdgeFail, vals1, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, vals1, idst));

//     auto nGetNeighbors = env_->getCountOfNeighbors(edges);
//     ASSERT_EQ(nGetNeighbors, 0);

//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     std::vector<cpp2::NewEdge> search{edges.back()};

//     nGetNeighbors = env_->getCountOfNeighbors(search);
//     ASSERT_EQ(nGetNeighbors, 0);
//     incrSrc(10);
// }


// /*
//  * new edge can overwrite partial committed edge
//  * (in-edge not committed)
//  * */
// TEST_F(TossTest, uncommittedInEdgeTest_sameEdgePrevNoNext) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     std::vector<nebula::Value> val1(types_.size());
//     val1[0].setInt(1024);
//     val1[1].setStr("bi-edge:"+ boost::uuids::to_string(u));

//     std::vector<nebula::Value> val2(types_.size());
//     val2[0].setInt(2048);
//     val2[1].setStr("in-edge:" + boost::uuids::to_string(u));

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kInEdgeFail, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));

//     std::vector<cpp2::NewEdge> search{edges.back()};

//     LOG(INFO) << "add edge: " << val1[1];
//     LOG(INFO) << "add edge: " << val2[1];
//     LOG(INFO) << "add edge: " << val1[1];

//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     std::string props = env_->getOutNeighborsProps(search);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), 1);

//     auto svec = TossEnvironment::extractProps(props);
//     ASSERT_EQ(svec.size(), 1);
//     EXPECT_EQ(TossEnvironment::extractSingleProps(svec.back()),
//               TossTestUtils::concatValues(val1));

//     // additional test, just call get neighbors again
//     EXPECT_EQ(env_->getCountOfNeighbors(search), 1);
//     incrSrc(10);
// }


// TEST_F(TossTest, uncommittedInEdgeTest_sameCommitLockPrevNoNext) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     std::vector<nebula::Value> val1(types_.size());
//     val1[0].setInt(1024);
//     val1[1].setStr("bi-edge:"+ boost::uuids::to_string(u));

//     std::vector<nebula::Value> val2(types_.size());
//     val2[0].setInt(2048);
//     val2[1].setStr("in-edge:" + boost::uuids::to_string(u));

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kInEdgeFail, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kOutEdgeFail, val2, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

//     std::vector<cpp2::NewEdge> search{edges.back()};

//     LOG(INFO) << "add edge: " << val1[1];
//     LOG(INFO) << "add edge: " << val1[1];
//     LOG(INFO) << "add edge: " << val2[1];
//     LOG(INFO) << "add edge: " << val2[1];

//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     std::string props = env_->getOutNeighborsProps(search);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), 1);

//     auto svec = TossEnvironment::extractProps(props);
//     ASSERT_EQ(svec.size(), 1);
//     EXPECT_EQ(TossEnvironment::extractSingleProps(svec.back()),
//               TossTestUtils::concatValues(val2));

//     // additional test, just call get neighbors again
//     EXPECT_EQ(env_->getCountOfNeighbors(search), 1);
//     incrSrc(10);
// }


// TEST_F(TossTest, uncommittedInEdgeTest_sameUncommitLockPrevNoNext) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     std::vector<nebula::Value> val1(types_.size());
//     val1[0].setInt(1024);
//     val1[1].setStr("bi-edge:"+ boost::uuids::to_string(u));

//     std::vector<nebula::Value> val2(types_.size());
//     val2[0].setInt(2048);
//     val2[1].setStr("in-edge:" + boost::uuids::to_string(u));

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kInEdgeFail, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kInEdgeFail, val2, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

//     std::vector<cpp2::NewEdge> search{edges.back()};

//     LOG(INFO) << "add edge: " << val1[1];
//     LOG(INFO) << "add edge: " << val1[1];
//     LOG(INFO) << "add edge: " << val2[1];
//     LOG(INFO) << "add edge: " << val2[1];

//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     std::string props = env_->getOutNeighborsProps(search);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), 0);

//     auto svec = TossEnvironment::extractProps(props);
//     ASSERT_EQ(svec.size(), 0);
//     incrSrc(10);
// }


// TEST_F(TossTest, uncommittedInEdgeTest_neighborCommitLockPrevNoNext) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     std::vector<nebula::Value> val1(types_.size());
//     val1[0].setInt(1024);
//     val1[1].setStr("bi-edge:"+ boost::uuids::to_string(u));

//     std::vector<nebula::Value> val2(types_.size());
//     val2[0].setInt(2048);
//     val2[1].setStr("in-edge:" + boost::uuids::to_string(u));

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_ - gPart*2, kOutEdgeFail, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kSucceeded, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_ - gPart*2, kInEdgeFail, val2, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

//     std::vector<cpp2::NewEdge> search{edges[1], edges[3]};

//     LOG(INFO) << "add edge: " << val1[1];
//     LOG(INFO) << "add edge: " << val1[1];
//     LOG(INFO) << "add edge: " << val2[1];
//     LOG(INFO) << "add edge: " << val2[1];

//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     std::string props = env_->getOutNeighborsProps(search);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), 1);

//     auto svec = TossEnvironment::extractProps(props);
//     ASSERT_EQ(svec.size(), 1);
//     EXPECT_EQ(TossEnvironment::extractSingleProps(svec.back()),
//               TossTestUtils::concatValues(val1));

//     // additional test, call get neighbors again
//     EXPECT_EQ(env_->getCountOfNeighbors(search), 1);
//     incrSrc(10);
// }


// TEST_F(TossTest, uncommittedInEdgeTest_neighborUncommitLockPrevNoNext) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     std::vector<nebula::Value> val1(types_.size());
//     val1[0].setInt(1024);
//     val1[1].setStr("bi-edge:"+ boost::uuids::to_string(u));

//     std::vector<nebula::Value> val2(types_.size());
//     val2[0].setInt(2048);
//     val2[1].setStr("in-edge:" + boost::uuids::to_string(u));

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_ - gPart*2, kInEdgeFail, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kSucceeded, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_ - gPart*2, kInEdgeFail, val2, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

//     std::vector<cpp2::NewEdge> search{edges[1], edges[3]};

//     LOG(INFO) << "add edge: " << val1[1];
//     LOG(INFO) << "add edge: " << val1[1];
//     LOG(INFO) << "add edge: " << val2[1];
//     LOG(INFO) << "add edge: " << val2[1];

//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     std::string props = env_->getOutNeighborsProps(search);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), 0);

//     auto svec = TossEnvironment::extractProps(props);
//     ASSERT_EQ(svec.size(), 0);
//     incrSrc(10);
// }


// /*
//  * run two getNeighbors(same src) simultaneous
//  * */
// TEST_F(TossTest, getNeighborsTest_simultaneousGetNeighbors) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();
//     std::vector<nebula::Value> val1(types_.size());
//     val1[0].setInt(2048);
//     val1[1].setStr("bi-edge:"+ boost::uuids::to_string(u));

//     std::vector<nebula::Value> val2(types_.size());
//     val2[0].setInt(1024);
//     val2[1].setStr("in-edge:" + boost::uuids::to_string(u));

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kOutEdgeFail, val2, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val2, idst));

//     for (auto& edge : edges) {
//         LOG(INFO) << "going to add edge:" << edge.props.back();
//         env_->addEdgeAsync(edge);;
//     }

//     std::vector<cpp2::NewEdge> search{edges.back(), edges.back()};

//     std::string props = env_->getOutNeighborsProps(search);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), 1);

//     auto svec = TossEnvironment::extractProps(props);
//     ASSERT_EQ(svec.size(), 1);
//     EXPECT_EQ(TossEnvironment::extractSingleProps(svec.back()),
//               TossTestUtils::concatValues(val2));
//     incrSrc(10);
// }


// TEST_F(TossTest, getNeighborsTest_removeLockFailed) {
//     boost::uuids::random_generator gen;
//     boost::uuids::uuid u = gen();

//     std::vector<nebula::Value> val1(types_.size());
//     val1[0].setInt(1024);
//     val1[1].setStr("in-edge:" + boost::uuids::to_string(u));

//     std::vector<nebula::Value> val2(types_.size());
//     val2[0].setInt(2048);
//     val2[1].setStr("bi-edge:"+ boost::uuids::to_string(u));

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = kSum - src_;
//     edges.emplace_back(env_->generateEdge(src_ - gPart, kRemoveLockFail, val2, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, val1, idst));

//     EXPECT_EQ(env_->getCountOfNeighbors(edges), 0);

//     for (auto& edge : edges) {
//         env_->addEdgeAsync(edge);;
//     }

//     EXPECT_EQ(env_->getCountOfNeighbors(edges), 1);

//     EXPECT_EQ(env_->getCountOfNeighbors(edges), 1);
// }

// TEST_F(TossTest, limitTest_goodLockAndNeighborEdge) {
//     incrSrc(10);
//     boost::uuids::uuid u = gen_();
//     int64_t limit = 1;

//     std::vector<nebula::Value> valGoodLock(types_.size());
//     valGoodLock[0].setInt(2000);
//     valGoodLock[1].setStr("good lock:" + boost::uuids::to_string(u));

//     std::vector<nebula::Value> valEdge(types_.size());
//     valEdge[0].setInt(3000);
//     valEdge[1].setStr("edge:" + boost::uuids::to_string(u));

//     std::vector<cpp2::NewEdge> edges;
//     auto idst = src_ + gPart;
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, valEdge, idst));
//     edges.emplace_back(env_->generateEdge(src_-gPart, kOutEdgeFail, valGoodLock, idst));
//     edges.emplace_back(env_->generateEdge(src_, kSucceeded, valGoodLock, idst));

//     for (auto& edge : edges) {
//         LOG(INFO) << "going to add edge:" << edge.props.back();
//         env_->addEdgeAsync(edge);;
//     }

//     std::vector<cpp2::NewEdge> startWith{edges[0]};

//     std::string props = env_->getOutNeighborsProps(startWith, limit);
//     LOG(INFO) << "outNeighborsProps: " << props;
//     EXPECT_EQ(TossEnvironment::countSquareBrackets(props), limit);

//     auto svec = TossEnvironment::extractProps(props);
//     ASSERT_EQ(svec.size(), limit);
//     EXPECT_EQ(TossEnvironment::extractSingleProps(svec.back()),
//               TossTestUtils::concatValues(valGoodLock));

//     // additional test, call get neighbors again
//     EXPECT_EQ(env_->getCountOfNeighbors(startWith), limit);
// }

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    FLAGS_heartbeat_interval_secs = 3;

    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, false);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
