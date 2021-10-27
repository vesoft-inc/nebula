/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/CommonUtils.h"
#include "storage/transaction/ChainResumeProcessor.h"
#include "storage/transaction/ChainUpdateEdgeProcessorLocal.h"
#include "storage/transaction/ChainUpdateEdgeProcessorRemote.h"

namespace nebula {
namespace storage {

extern const int32_t mockSpaceId;
extern const int32_t mockPartNum;
extern const int32_t mockSpaceVidLen;

using KeyGenerator = std::function<std::string(PartitionID partId, const cpp2::NewEdge& edge)>;

class ChainTestUtils {
 public:
  ChainTestUtils() {
    genKey = [&](PartitionID partId, const cpp2::NewEdge& edge) {
      auto key = ConsistUtil::edgeKey(spaceVidLen_, partId, edge.get_key());
      return key;
    };
    genPrime = [&](PartitionID partId, const cpp2::NewEdge& edge) {
      auto key = ConsistUtil::primeKey(spaceVidLen_, partId, edge.get_key());
      return key;
    };
    genDoublePrime = [&](PartitionID partId, const cpp2::NewEdge& edge) {
      auto key = ConsistUtil::doublePrime(spaceVidLen_, partId, edge.get_key());
      return key;
    };
  }

 public:
  int32_t spaceVidLen_{32};
  KeyGenerator genKey;
  KeyGenerator genPrime;
  KeyGenerator genDoublePrime;
};

// , StorageEnv* env
int numOfKey(const cpp2::AddEdgesRequest& req, KeyGenerator gen, StorageEnv* env) {
  int numOfEdges = 0;
  int totalEdge = 0;
  auto spaceId = req.get_space_id();
  for (auto& edgesOfPart : *req.parts_ref()) {
    auto partId = edgesOfPart.first;
    auto& edgeVec = edgesOfPart.second;
    for (auto& edge : edgeVec) {
      ++totalEdge;
      auto key = gen(partId, edge);
      std::unique_ptr<kvstore::KVIterator> iter;
      EXPECT_EQ(Code::SUCCEEDED, env->kvstore_->prefix(spaceId, partId, key, &iter));
      if (iter && iter->valid()) {
        ++numOfEdges;
      } else {
        // LOG(INFO) << "key: " << key << " not exist";
      }
    }
  }
  LOG(INFO) << "numOfEdges = " << numOfEdges;
  LOG(INFO) << "totalEdge = " << totalEdge;
  return numOfEdges;
}

std::pair<GraphSpaceID, PartitionID> extractSpaceAndPart(const cpp2::AddEdgesRequest& req) {
  auto spaceId = req.get_space_id();
  CHECK_EQ(req.get_parts().size(), 1);
  auto partId = req.get_parts().begin()->first;
  return std::make_pair(spaceId, partId);
}

bool keyExist(StorageEnv* env, GraphSpaceID spaceId, PartitionID partId, std::string key) {
  // std::unique_ptr<kvstore::KVIterator> iter;
  std::string ignoreVal;
  auto rc = env->kvstore_->get(spaceId, partId, key, &ignoreVal);
  return rc == Code::SUCCEEDED;
}

class FakeChainAddEdgesProcessorLocal : public ChainAddEdgesProcessorLocal {
  FRIEND_TEST(ChainAddEdgesTest, prepareLocalSucceededTest);
  FRIEND_TEST(ChainAddEdgesTest, processRemoteSucceededTest);
  FRIEND_TEST(ChainAddEdgesTest, processRemoteFailedTest);
  FRIEND_TEST(ChainAddEdgesTest, processRemoteOutdatedTest);
  // all the above will test succeeded path of process local
  // the failed path of process local will be tested in resume test
 public:
  explicit FakeChainAddEdgesProcessorLocal(StorageEnv* env) : ChainAddEdgesProcessorLocal(env) {
    spaceVidLen_ = 32;
  }

  folly::SemiFuture<Code> prepareLocal() override {
    LOG(INFO) << "FakeChainAddEdgesProcessorLocal::" << __func__ << "()";
    if (rcPrepareLocal) {
      LOG(INFO) << "Fake return " << apache::thrift::util::enumNameSafe(*rcPrepareLocal);
      return *rcPrepareLocal;
    }
    LOG(INFO) << "forward to ChainAddEdgesProcessorLocal::prepareLocal()";
    return ChainAddEdgesProcessorLocal::prepareLocal();
  }

  folly::SemiFuture<Code> processRemote(Code code) override {
    LOG(INFO) << "FakeChainAddEdgesProcessorLocal::" << __func__ << "()";
    if (rcProcessRemote) {
      LOG(INFO) << "processRemote() fake return "
                << apache::thrift::util::enumNameSafe(*rcProcessRemote);
      LOG_IF(FATAL, code != Code::SUCCEEDED) << "cheat must base on truth";
      return *rcProcessRemote;
    }
    LOG(INFO) << "forward to ChainAddEdgesProcessorLocal::processRemote()";
    return ChainAddEdgesProcessorLocal::processRemote(code);
  }

  folly::SemiFuture<Code> processLocal(Code code) override {
    LOG(INFO) << "FakeChainAddEdgesProcessorLocal::" << __func__ << "()";
    if (rcProcessLocal) {
      LOG(INFO) << "Fake return " << apache::thrift::util::enumNameSafe(*rcProcessLocal);
      return *rcProcessLocal;
    }
    LOG(INFO) << "forward to ChainAddEdgesProcessorLocal::processLocal()";
    return ChainAddEdgesProcessorLocal::processLocal(code);
  }

  cpp2::AddEdgesRequest reverseRequestForward(const cpp2::AddEdgesRequest& req) {
    return ChainAddEdgesProcessorLocal::reverseRequest(req);
  }

  folly::Optional<Code> rcPrepareLocal;

  folly::Optional<Code> rcProcessRemote;

  folly::Optional<Code> rcProcessLocal;
};

class FakeChainUpdateProcessor : public ChainUpdateEdgeProcessorLocal {
 public:
  explicit FakeChainUpdateProcessor(StorageEnv* env) : ChainUpdateEdgeProcessorLocal(env) {
    spaceVidLen_ = 32;
  }

  folly::SemiFuture<Code> prepareLocal() override {
    LOG(INFO) << "FakeChainUpdateEdgeProcessorA::" << __func__ << "()";
    if (rcPrepareLocal) {
      LOG(INFO) << "Fake return " << apache::thrift::util::enumNameSafe(*rcPrepareLocal);
      return *rcPrepareLocal;
    }
    LOG(INFO) << "forward to ChainUpdateEdgeProcessorLocal::prepareLocal()";
    return ChainUpdateEdgeProcessorLocal::prepareLocal();
  }

  folly::SemiFuture<Code> processRemote(Code code) override {
    LOG(INFO) << "FakeChainUpdateEdgeProcessorA::" << __func__ << "()";
    if (rcProcessRemote) {
      LOG(INFO) << "processRemote() fake return "
                << apache::thrift::util::enumNameSafe(*rcProcessRemote);
      LOG_IF(FATAL, code != Code::SUCCEEDED) << "cheat must base on truth";
      return *rcProcessRemote;
    }
    LOG(INFO) << "forward to ChainUpdateEdgeProcessorLocal::processRemote()";
    return ChainUpdateEdgeProcessorLocal::processRemote(code);
  }

  folly::SemiFuture<Code> processLocal(Code code) override {
    LOG(INFO) << "FakeChainUpdateEdgeProcessorA::" << __func__ << "()";
    if (rcProcessLocal) {
      LOG(INFO) << "processLocal() fake return "
                << apache::thrift::util::enumNameSafe(*rcProcessLocal);
      return *rcProcessLocal;
    }
    LOG(INFO) << "forward to ChainUpdateEdgeProcessorLocal::processLocal()";
    return ChainUpdateEdgeProcessorLocal::processLocal(code);
  }

  void wrapAddUnfinishedEdge(ResumeType type) { addUnfinishedEdge(type); }

 public:
  folly::Optional<Code> rcPrepareLocal;
  folly::Optional<Code> rcProcessRemote;
  folly::Optional<Code> rcProcessLocal;
};

class MetaClientTestUpdater {
 public:
  MetaClientTestUpdater() = default;

  static void addLocalCache(meta::MetaClient& mClient,
                            GraphSpaceID spaceId,
                            std::shared_ptr<meta::SpaceInfoCache> spInfoCache) {
    mClient.localCache_[spaceId] = spInfoCache;
  }

  static meta::SpaceInfoCache* getLocalCache(meta::MetaClient* mClient, GraphSpaceID spaceId) {
    if (mClient->localCache_.count(spaceId) == 0) {
      return nullptr;
    }
    return mClient->localCache_[spaceId].get();
  }

  static void addPartTerm(meta::MetaClient* mClient,
                          GraphSpaceID spaceId,
                          PartitionID partId,
                          TermID termId) {
    auto* pCache = getLocalCache(mClient, spaceId);
    if (pCache == nullptr) {
      auto spCache = std::make_shared<meta::SpaceInfoCache>();
      addLocalCache(*mClient, spaceId, spCache);
      pCache = getLocalCache(mClient, spaceId);
    }
    pCache->termOfPartition_[partId] = termId;
  }

  static std::unique_ptr<meta::MetaClient> makeDefaultMetaClient() {
    auto exec = std::make_shared<folly::IOThreadPoolExecutor>(3);
    std::vector<HostAddr> addrs(1);
    meta::MetaClientOptions options;

    auto mClient = std::make_unique<meta::MetaClient>(exec, addrs, options);
    mClient->localCache_[mockSpaceId] = std::make_shared<meta::SpaceInfoCache>();
    for (int i = 0; i != mockPartNum; ++i) {
      mClient->localCache_[mockSpaceId]->termOfPartition_[i] = i;
      auto ignoreItem = mClient->localCache_[mockSpaceId]->partsAlloc_[i];
      UNUSED(ignoreItem);
    }
    meta::cpp2::ColumnTypeDef type;
    type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
    type.set_type_length(32);

    mClient->localCache_[mockSpaceId]->spaceDesc_.set_vid_type(std::move(type));
    mClient->ready_ = true;
    return mClient;
  }
};

class FakeInternalStorageClient : public InternalStorageClient {
 public:
  explicit FakeInternalStorageClient(StorageEnv* env,
                                     std::shared_ptr<folly::IOThreadPoolExecutor> pool,
                                     Code code)
      : InternalStorageClient(pool, env->metaClient_), env_(env), code_(code) {}

  void chainUpdateEdge(cpp2::UpdateEdgeRequest& req,
                       TermID termOfSrc,
                       folly::Optional<int64_t> optVersion,
                       folly::Promise<Code>&& p,
                       folly::EventBase* evb = nullptr) override {
    cpp2::ChainUpdateEdgeRequest chainReq;
    chainReq.set_update_edge_request(req);
    chainReq.set_term(termOfSrc);

    auto* proc = ChainUpdateEdgeProcessorRemote::instance(env_);
    auto f = proc->getFuture();
    proc->process(chainReq);
    auto resp = std::move(f).get();

    p.setValue(code_);
    UNUSED(optVersion);
    UNUSED(evb);
  }

  void setErrorCode(Code code) { code_ = code; }

  void chainAddEdges(cpp2::AddEdgesRequest& req,
                     TermID termId,
                     folly::Optional<int64_t> optVersion,
                     folly::Promise<::nebula::cpp2::ErrorCode>&& p,
                     folly::EventBase* evb = nullptr) override {
    UNUSED(req);
    UNUSED(termId);
    UNUSED(optVersion);
    p.setValue(code_);
    UNUSED(evb);
  }

  static FakeInternalStorageClient* instance(StorageEnv* env, Code fakeCode = Code::SUCCEEDED) {
    auto pool = std::make_shared<folly::IOThreadPoolExecutor>(3);
    return new FakeInternalStorageClient(env, pool, fakeCode);
  }

  static void hookInternalStorageClient(StorageEnv* env, InternalStorageClient* client) {
    env->txnMan_->iClient_ = client;
  }

 private:
  StorageEnv* env_{nullptr};
  Code code_{Code::SUCCEEDED};
};

struct ChainUpdateEdgeTestHelper {
  ChainUpdateEdgeTestHelper() { sEdgeType = std::to_string(std::abs(edgeType_)); }

  cpp2::EdgeKey defaultEdgeKey() {
    cpp2::EdgeKey ret;
    ret.set_src(srcId_);
    ret.set_edge_type(edgeType_);
    ret.set_ranking(rank_);
    ret.set_dst(dstId_);
    return ret;
  }

  std::vector<cpp2::UpdatedProp> defaultUpdateProps() {
    ObjectPool objPool;
    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;
    // int: 101.teamCareer = 20
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("teamCareer");
    // ConstantExpression val1(20);
    const auto& val1 = *ConstantExpression::make(&objPool, 20);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // bool: 101.type = trade
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("type");
    std::string colnew("trade");
    // ConstantExpression val2(colnew);
    const auto& val2 = *ConstantExpression::make(&objPool, colnew);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);
    return props;
  }

  std::vector<std::string> defaultRetProps() {
    ObjectPool objPool;
    std::vector<std::string> props;
    std::vector<std::string> cols{
        "playerName", "teamName", "teamCareer", "type", kSrc, kType, kRank, kDst};
    for (auto& colName : cols) {
      const auto& exp = *EdgePropertyExpression::make(&objPool, sEdgeType, colName);
      props.emplace_back(Expression::encode(exp));
    }
    return props;
  }

  cpp2::UpdateEdgeRequest makeDefaultRequest() {
    auto edgeKey = defaultEdgeKey();
    auto updateProps = defaultUpdateProps();
    auto retProps = defaultRetProps();
    return makeRequest(edgeKey, updateProps, retProps);
  }

  cpp2::UpdateEdgeRequest reverseRequest(StorageEnv* env, const cpp2::UpdateEdgeRequest& req) {
    ChainUpdateEdgeProcessorLocal proc(env);
    return proc.reverseRequest(req);
  }

  cpp2::UpdateEdgeRequest makeInvalidRequest() {
    VertexID srcInvalid{"Spurssssssss"};
    auto edgeKey = defaultEdgeKey();
    edgeKey.set_src(srcInvalid);
    auto updateProps = defaultUpdateProps();
    auto retProps = defaultRetProps();
    return makeRequest(edgeKey, updateProps, retProps);
  }

  cpp2::UpdateEdgeRequest makeRequest(const cpp2::EdgeKey& edgeKey,
                                      const std::vector<cpp2::UpdatedProp>& updateProps,
                                      const std::vector<std::string>& retCols) {
    cpp2::UpdateEdgeRequest req;
    auto partId = std::hash<std::string>()(edgeKey.get_src().getStr()) % mockPartNum + 1;
    req.set_space_id(mockSpaceId);
    req.set_part_id(partId);
    req.set_edge_key(edgeKey);
    req.set_updated_props(updateProps);
    req.set_return_props(retCols);
    req.set_insertable(false);
    return req;
  }

  bool checkResp2(cpp2::UpdateResponse& resp) {
    LOG(INFO) << "checkResp2(cpp2::UpdateResponse& resp)";
    if (!resp.props_ref()) {
      LOG(INFO) << "!resp.props_ref()";
      return false;
    } else {
      LOG(INFO) << "resp.props_ref()";
      EXPECT_EQ(9, (*resp.props_ref()).colNames.size());
      EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
      EXPECT_EQ("101.playerName", (*resp.props_ref()).colNames[1]);
      EXPECT_EQ("101.teamName", (*resp.props_ref()).colNames[2]);
      EXPECT_EQ("101.teamCareer", (*resp.props_ref()).colNames[3]);
      EXPECT_EQ("101.type", (*resp.props_ref()).colNames[4]);
      EXPECT_EQ(std::string("101.").append(kSrc), (*resp.props_ref()).colNames[5]);
      EXPECT_EQ(std::string("101.").append(kType), (*resp.props_ref()).colNames[6]);
      EXPECT_EQ(std::string("101.").append(kRank), (*resp.props_ref()).colNames[7]);
      EXPECT_EQ(std::string("101.").append(kDst), (*resp.props_ref()).colNames[8]);
      EXPECT_EQ(1, (*resp.props_ref()).rows.size());
      EXPECT_EQ(9, (*resp.props_ref()).rows[0].values.size());
      EXPECT_EQ(false, (*resp.props_ref()).rows[0].values[0].getBool());
      EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[1].getStr());
      EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[2].getStr());
      EXPECT_EQ(20, (*resp.props_ref()).rows[0].values[3].getInt());
      EXPECT_EQ("trade", (*resp.props_ref()).rows[0].values[4].getStr());
      EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[5].getStr());
      EXPECT_EQ(-101, (*resp.props_ref()).rows[0].values[6].getInt());
      EXPECT_EQ(1997, (*resp.props_ref()).rows[0].values[7].getInt());
      EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[8].getStr());
    }
    return true;
  }

  bool edgeExist(StorageEnv* env, cpp2::UpdateEdgeRequest& req) {
    auto partId = req.get_part_id();
    auto key = ConsistUtil::edgeKey(mockSpaceVidLen, partId, req.get_edge_key());
    return keyExist(env, mockSpaceId, partId, key);
  }

  bool primeExist(StorageEnv* env, cpp2::UpdateEdgeRequest& req) {
    auto partId = req.get_part_id();
    auto key = ConsistUtil::primeKey(mockSpaceVidLen, partId, req.get_edge_key());
    return keyExist(env, mockSpaceId, partId, key);
  }

  bool doublePrimeExist(StorageEnv* env, cpp2::UpdateEdgeRequest& req) {
    auto partId = req.get_part_id();
    auto key = ConsistUtil::doublePrime(mockSpaceVidLen, partId, req.get_edge_key());
    return keyExist(env, mockSpaceId, partId, key);
  }

  bool keyExist(StorageEnv* env, GraphSpaceID spaceId, PartitionID partId, const std::string& key) {
    std::string val;
    auto rc = env->kvstore_->get(spaceId, partId, key, &val);
    return rc == Code::SUCCEEDED;
  }

  bool checkRequestUpdated(StorageEnv* env, cpp2::UpdateEdgeRequest& req) {
    // get serve from kvstore directly
    bool ret = true;
    auto& key = req.get_edge_key();
    auto partId = req.get_part_id();
    auto prefix = ConsistUtil::edgeKey(mockSpaceVidLen, partId, req.get_edge_key());
    std::unique_ptr<kvstore::KVIterator> iter;
    auto rc = env->kvstore_->prefix(mockSpaceId, partId, prefix, &iter);
    EXPECT_EQ(Code::SUCCEEDED, rc);
    EXPECT_TRUE(iter && iter->valid());

    auto edgeType = key.get_edge_type();
    auto edgeReader = RowReaderWrapper::getEdgePropReader(
        env->schemaMan_, mockSpaceId, std::abs(edgeType), iter->val());

    LOG(INFO) << "req.get_updated_props().size() = " << req.get_updated_props().size();
    ObjectPool objPool;
    for (auto& prop : req.get_updated_props()) {
      LOG(INFO) << "prop name = " << prop.get_name();
      auto enVal = prop.get_value();
      auto expression = Expression::decode(&objPool, enVal);
      ConstantExpression* cexpr = static_cast<ConstantExpression*>(expression);
      auto val1 = cexpr->value();
      auto val2 = edgeReader->getValueByName(prop.get_name());

      // EXPECT_EQ(val1, val2);
      if (val1 != val2) {
        ret = false;
      }
    }

    return ret;
  }

 public:
  VertexID srcId_{"Spurs"};
  VertexID dstId_{"Tim Duncan"};
  EdgeRanking rank_{1997};
  EdgeType edgeType_{-101};
  storage::cpp2::EdgeKey updateKey_;
  std::string sEdgeType;
};

// class ChainResumeProcessorTestHelper {
// public:
//     explicit ChainResumeProcessorTestHelper(ChainResumeProcessor* proc) : proc_(proc) {}

//     void setAddEdgeProc(ChainAddEdgesProcessorLocal* proc) {
//         proc_->addProc = proc;
//     }

//     // setUpdProc
//     void setUpdProc(ChainUpdateEdgeProcessorLocal* proc) {
//         proc_->updProc = proc;
//     }

//     std::string getTxnId() {
//         return proc_->addProc->txnId_;
//     }
// public:
//     ChainResumeProcessor* proc_{nullptr};
// };

}  // namespace storage
}  // namespace nebula
