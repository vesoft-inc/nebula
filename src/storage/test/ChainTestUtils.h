/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TEST_CHAINTESTUTILS_H
#define STORAGE_TEST_CHAINTESTUTILS_H

#include "storage/CommonUtils.h"
#include "storage/transaction/ChainAddEdgesLocalProcessor.h"
#include "storage/transaction/ChainUpdateEdgeLocalProcessor.h"
#include "storage/transaction/ChainUpdateEdgeRemoteProcessor.h"

namespace nebula {
namespace storage {

extern const int32_t mockSpaceId;
extern const int32_t mockPartNum;
extern const int32_t mockSpaceVidLen;

using KeyGenerator = std::function<std::string(PartitionID partId, const cpp2::NewEdge& edge)>;

class TransactionManagerTester {
 public:
  explicit TransactionManagerTester(TransactionManager* p) : man_(p) {}

  void stop() {
    man_->stop();
    int32_t numCheckIdle = 0;
    while (numCheckIdle < 3) {
      auto stats = man_->worker_->getPoolStats();
      if (stats.threadCount == stats.idleThreadCount) {
        ++numCheckIdle;
      } else {
        numCheckIdle = 0;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
  }

  TransactionManager* man_{nullptr};
};

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

  std::vector<std::string> genEdgeKeys(const cpp2::AddEdgesRequest& req, KeyGenerator gen) {
    std::vector<std::string> ret;
    for (auto& partAndEdges : *req.parts_ref()) {
      auto partId = partAndEdges.first;
      auto& edgeVec = partAndEdges.second;
      for (auto& edge : edgeVec) {
        auto key = gen(partId, edge);
        ret.emplace_back(std::move(key));
      }
    }
    return ret;
  }

  // return the actual num of keys in nebula store.
  int32_t checkNumOfKey(StorageEnv* env,
                        GraphSpaceID spaceId,
                        const std::vector<std::string>& keys) {
    int32_t ret = 0;

    std::unique_ptr<kvstore::KVIterator> iter;
    for (auto& key : keys) {
      iter.reset();
      auto partId = NebulaKeyUtils::getPart(key);
      auto rc = env->kvstore_->prefix(spaceId, partId, key, &iter);
      if (rc == Code::SUCCEEDED && iter && iter->valid()) {
        ++ret;
      }
    }

    return ret;
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
        // LOG(INFO) << "key = " << key;
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

class FakeChainAddEdgesLocalProcessor : public ChainAddEdgesLocalProcessor {
  FRIEND_TEST(ChainAddEdgesTest, prepareLocalSucceededTest);
  FRIEND_TEST(ChainAddEdgesTest, processRemoteSucceededTest);
  FRIEND_TEST(ChainAddEdgesTest, processRemoteFailedTest);
  FRIEND_TEST(ChainAddEdgesTest, processRemoteOutdatedTest);
  // all the above will test succeeded path of process local
  // the failed path of process local will be tested in resume test
 public:
  explicit FakeChainAddEdgesLocalProcessor(StorageEnv* env) : ChainAddEdgesLocalProcessor(env) {
    spaceVidLen_ = 32;
    rcRemote_ = Code::SUCCEEDED;
    rcCommit_ = Code::SUCCEEDED;
  }

  folly::SemiFuture<Code> prepareLocal() override {
    LOG(INFO) << "FakeChainAddEdgesLocalProcessor::" << __func__ << "()";
    if (rcPrepareLocal) {
      LOG(INFO) << "Fake return " << apache::thrift::util::enumNameSafe(*rcPrepareLocal);
      return *rcPrepareLocal;
    }
    LOG(INFO) << "forward to ChainAddEdgesLocalProcessor::prepareLocal()";
    return ChainAddEdgesLocalProcessor::prepareLocal();
  }

  folly::SemiFuture<Code> processRemote(Code code) override {
    LOG(INFO) << "FakeChainAddEdgesLocalProcessor::" << __func__ << "()";
    if (rcProcessRemote) {
      LOG(INFO) << "processRemote() fake return "
                << apache::thrift::util::enumNameSafe(*rcProcessRemote);
      LOG_IF(FATAL, code != Code::SUCCEEDED) << "cheat must base on truth";
      return *rcProcessRemote;
    }
    LOG(INFO) << "forward to ChainAddEdgesLocalProcessor::processRemote()";
    return ChainAddEdgesLocalProcessor::processRemote(code);
  }

  folly::SemiFuture<Code> processLocal(Code code) override {
    LOG(INFO) << "FakeChainAddEdgesLocalProcessor::" << __func__ << "()";
    if (rcProcessLocal) {
      LOG(INFO) << "Fake return " << apache::thrift::util::enumNameSafe(*rcProcessLocal);
      return *rcProcessLocal;
    }
    LOG(INFO) << "forward to ChainAddEdgesLocalProcessor::processLocal()";
    return ChainAddEdgesLocalProcessor::processLocal(code);
  }

  cpp2::AddEdgesRequest reverseRequestForward(const cpp2::AddEdgesRequest& req) {
    return ChainAddEdgesLocalProcessor::reverseRequest(req);
  }

  std::optional<Code> rcPrepareLocal;

  std::optional<Code> rcProcessRemote;

  std::optional<Code> rcProcessLocal;

  void setPrepareCode(Code code, Code rc = Code::SUCCEEDED) {
    rcPrepareLocal = code;
    rcPrepare_ = rc;
  }

  void setRemoteCode(Code code) {
    rcProcessRemote = code;
    rcRemote_ = code;
  }

  void setCommitCode(Code code, Code rc = Code::SUCCEEDED) {
    rcProcessLocal = code;
    rcCommit_ = rc;
  }

  void finish() override {
    auto rc = (rcPrepare_ == Code::SUCCEEDED) ? rcCommit_ : rcPrepare_;
    pushResultCode(rc, localPartId_);
    finished_.setValue(rc);
    onFinished();
  }
};

class FakeChainUpdateProcessor : public ChainUpdateEdgeLocalProcessor {
 public:
  explicit FakeChainUpdateProcessor(StorageEnv* env) : ChainUpdateEdgeLocalProcessor(env) {
    spaceVidLen_ = 32;
  }

  folly::SemiFuture<Code> prepareLocal() override {
    LOG(INFO) << "FakeChainUpdateEdgeProcessorA::" << __func__ << "()";
    if (rcPrepareLocal) {
      LOG(INFO) << "Fake return " << apache::thrift::util::enumNameSafe(*rcPrepareLocal);
      return *rcPrepareLocal;
    }
    LOG(INFO) << "forward to ChainUpdateEdgeLocalProcessor::prepareLocal()";
    return ChainUpdateEdgeLocalProcessor::prepareLocal();
  }

  folly::SemiFuture<Code> processRemote(Code code) override {
    LOG(INFO) << "FakeChainUpdateEdgeProcessor::" << __func__ << "()";
    if (rcProcessRemote) {
      LOG(INFO) << "processRemote() fake return "
                << apache::thrift::util::enumNameSafe(*rcProcessRemote);
      LOG_IF(FATAL, code != Code::SUCCEEDED) << "cheat must base on truth";
      return *rcProcessRemote;
    }
    LOG(INFO) << "forward to ChainUpdateEdgeLocalProcessor::processRemote()";
    return ChainUpdateEdgeLocalProcessor::processRemote(code);
  }

  folly::SemiFuture<Code> processLocal(Code code) override {
    LOG(INFO) << "FakeChainUpdateEdgeProcessor::" << __func__ << "()";
    if (rcProcessLocal) {
      LOG(INFO) << "processLocal() fake return "
                << apache::thrift::util::enumNameSafe(*rcProcessLocal);
      return *rcProcessLocal;
    }
    LOG(INFO) << "forward to ChainUpdateEdgeLocalProcessor::processLocal()";
    return ChainUpdateEdgeLocalProcessor::processLocal(code);
  }

  void wrapAddUnfinishedEdge(ResumeType type) {
    reportFailed(type);
  }
  void setPrepareCode(Code code, Code rc = Code::SUCCEEDED) {
    rcPrepareLocal = code;
    rcPrepare_ = rc;
  }

  void setRemoteCode(Code code) {
    rcProcessRemote = code;
    rcRemote_ = code;
  }

  void setCommitCode(Code code, Code rc = Code::SUCCEEDED) {
    rcProcessLocal = code;
    rcCommit_ = rc;
  }

  void setDoRecover(bool doRecover) {
    doRecover_ = doRecover;
  }

  void finish() override {
    if (doRecover_) {
      LOG(INFO) << "do real finish()";
      ChainUpdateEdgeLocalProcessor::finish();
    } else {
      auto rc = Code::SUCCEEDED;
      do {
        if (rcPrepare_ != Code::SUCCEEDED) {
          rc = rcPrepare_;
          break;
        }

        if (rcCommit_ != Code::SUCCEEDED) {
          rc = rcCommit_;
          break;
        }

        if (rcRemote_ != Code::E_RPC_FAILURE) {
          rc = rcRemote_;
          break;
        }
      } while (0);

      pushResultCode(rc, localPartId_);
      finished_.setValue(rc);
      onFinished();
    }
  }

 public:
  std::optional<Code> rcPrepareLocal;
  std::optional<Code> rcProcessRemote;
  std::optional<Code> rcProcessLocal;
  bool doRecover_{false};
};

class MetaClientTestUpdater {
 public:
  MetaClientTestUpdater() = default;

  static void addLocalCache(meta::MetaClient& mClient,
                            GraphSpaceID spaceId,
                            std::shared_ptr<meta::SpaceInfoCache> spInfoCache) {
    mClient.metadata_.load()->localCache_[spaceId] = spInfoCache;
  }

  static meta::SpaceInfoCache* getLocalCache(meta::MetaClient* mClient, GraphSpaceID spaceId) {
    if (mClient->metadata_.load()->localCache_.count(spaceId) == 0) {
      return nullptr;
    }
    return mClient->metadata_.load()->localCache_[spaceId].get();
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

  static std::unique_ptr<meta::MetaClient> makeDefault() {
    auto exec = std::make_shared<folly::IOThreadPoolExecutor>(3);
    std::vector<HostAddr> addrs(1);
    meta::MetaClientOptions options;

    auto mClient = std::make_unique<meta::MetaClient>(exec, addrs, options);
    auto spSpaceInfoCache = std::make_shared<meta::SpaceInfoCache>();
    addLocalCache(*mClient, mockSpaceId, spSpaceInfoCache);
    auto* pCache = getLocalCache(mClient.get(), mockSpaceId);

    for (int i = 0; i != mockPartNum; ++i) {
      pCache->termOfPartition_[i] = i;
      auto ignoreItem = pCache->partsAlloc_[i];
      UNUSED(ignoreItem);
    }
    meta::cpp2::ColumnTypeDef type;
    type.type_ref() = nebula::cpp2::PropertyType::FIXED_STRING;
    type.type_length_ref() = 32;

    pCache->spaceDesc_.vid_type_ref() = std::move(type);
    mClient->ready_ = true;
    return mClient;
  }
};

class FakeInternalStorageClient : public InternalStorageClient {
 public:
  FakeInternalStorageClient(StorageEnv* env,
                            std::shared_ptr<folly::IOThreadPoolExecutor> pool,
                            Code code)
      : InternalStorageClient(pool, env->metaClient_), env_(env), code_(code) {}

  void chainUpdateEdge(cpp2::UpdateEdgeRequest& req,
                       TermID termOfSrc,
                       std::optional<int64_t> optVersion,
                       folly::Promise<Code>&& p,
                       folly::EventBase* evb = nullptr) override {
    cpp2::ChainUpdateEdgeRequest chainReq;
    chainReq.update_edge_request_ref() = req;
    chainReq.term_ref() = termOfSrc;

    auto* proc = ChainUpdateEdgeRemoteProcessor::instance(env_);
    auto f = proc->getFuture();
    proc->process(chainReq);
    auto resp = std::move(f).get();

    p.setValue(code_);
    UNUSED(optVersion);
    UNUSED(evb);
  }

  void setErrorCode(Code code) {
    code_ = code;
  }

  void chainAddEdges(cpp2::AddEdgesRequest& req,
                     TermID termId,
                     std::optional<int64_t> optVersion,
                     folly::Promise<::nebula::cpp2::ErrorCode>&& p,
                     folly::EventBase* evb = nullptr) override {
    UNUSED(req);
    UNUSED(termId);
    UNUSED(optVersion);
    p.setValue(code_);
    UNUSED(evb);
  }

  void chainDeleteEdges(cpp2::DeleteEdgesRequest& req,
                        const std::string& txnId,
                        TermID termId,
                        folly::Promise<::nebula::cpp2::ErrorCode>&& p,
                        folly::EventBase* evb = nullptr) override {
    UNUSED(req);
    UNUSED(txnId);
    UNUSED(termId);
    p.setValue(code_);
    UNUSED(evb);
  }

  static FakeInternalStorageClient* instance(StorageEnv* env, Code fakeCode = Code::SUCCEEDED) {
    auto pool = std::make_shared<folly::IOThreadPoolExecutor>(3);
    return new FakeInternalStorageClient(env, pool, fakeCode);
  }

  static void hookInternalStorageClient(StorageEnv* env, InternalStorageClient* client) {
    env->interClient_ = client;
  }

 private:
  StorageEnv* env_{nullptr};
  Code code_{Code::SUCCEEDED};
};

using UPCLT = std::unique_ptr<FakeInternalStorageClient>;

struct ChainUpdateEdgeTestHelper {
  ChainUpdateEdgeTestHelper() {
    sEdgeType = std::to_string(std::abs(edgeType_));
  }

  cpp2::EdgeKey defaultEdgeKey() {
    cpp2::EdgeKey ret;
    ret.src_ref() = srcId_;
    ret.edge_type_ref() = edgeType_;
    ret.ranking_ref() = rank_;
    ret.dst_ref() = dstId_;
    return ret;
  }

  std::vector<cpp2::UpdatedProp> defaultUpdateProps() {
    ObjectPool objPool;
    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;
    // int: 101.teamCareer = 20
    cpp2::UpdatedProp uProp1;
    uProp1.name_ref() = "teamCareer";
    // ConstantExpression val1(20);
    const auto& val1 = *ConstantExpression::make(&objPool, 20);
    uProp1.value_ref() = Expression::encode(val1);
    props.emplace_back(uProp1);

    // bool: 101.type = trade
    cpp2::UpdatedProp uProp2;
    uProp2.name_ref() = "type";
    std::string colnew("trade");
    // ConstantExpression val2(colnew);
    const auto& val2 = *ConstantExpression::make(&objPool, colnew);
    uProp2.value_ref() = Expression::encode(val2);
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
    ChainUpdateEdgeLocalProcessor proc(env);
    return proc.reverseRequest(req);
  }

  cpp2::UpdateEdgeRequest makeInvalidRequest() {
    VertexID srcInvalid{"Spurssssssss"};
    auto edgeKey = defaultEdgeKey();
    edgeKey.src_ref() = srcInvalid;
    auto updateProps = defaultUpdateProps();
    auto retProps = defaultRetProps();
    return makeRequest(edgeKey, updateProps, retProps);
  }

  cpp2::UpdateEdgeRequest makeRequest(const cpp2::EdgeKey& edgeKey,
                                      const std::vector<cpp2::UpdatedProp>& updateProps,
                                      const std::vector<std::string>& retCols) {
    cpp2::UpdateEdgeRequest req;
    auto partId = std::hash<std::string>()(edgeKey.get_src().getStr()) % mockPartNum + 1;
    req.space_id_ref() = mockSpaceId;
    req.part_id_ref() = partId;
    req.edge_key_ref() = edgeKey;
    req.updated_props_ref() = updateProps;
    req.return_props_ref() = retCols;
    req.insertable_ref() = false;
    return req;
  }

  bool checkResp(cpp2::UpdateResponse& resp) {
    LOG(INFO) << "checkResp(cpp2::UpdateResponse& resp)";
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

}  // namespace storage
}  // namespace nebula
#endif
