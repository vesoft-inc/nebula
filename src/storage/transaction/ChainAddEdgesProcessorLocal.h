/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class ChainAddEdgesProcessorLocal : public BaseProcessor<cpp2::ExecResponse>,
                                    public ChainBaseProcessor {
  friend class ChainResumeProcessorTestHelper;  // for test friendly
 public:
  static ChainAddEdgesProcessorLocal* instance(StorageEnv* env) {
    return new ChainAddEdgesProcessorLocal(env);
  }

  virtual ~ChainAddEdgesProcessorLocal() = default;

  virtual void process(const cpp2::AddEdgesRequest& req);

  folly::SemiFuture<Code> prepareLocal() override;

  folly::SemiFuture<Code> processRemote(Code code) override;

  folly::SemiFuture<Code> processLocal(Code code) override;

  void setRemotePartId(PartitionID remotePartId) { remotePartId_ = remotePartId; }

  void finish() override;

 protected:
  explicit ChainAddEdgesProcessorLocal(StorageEnv* env) : BaseProcessor<cpp2::ExecResponse>(env) {}

  bool prepareRequest(const cpp2::AddEdgesRequest& req);

  /**
   * @brief resume and set req_ & txnId_ from the val of (double)prime
   * @return true if resume succeeded
   */
  bool deserializeRequest(GraphSpaceID spaceId, PartitionID partId, folly::StringPiece val);

  void doRpc(folly::Promise<Code>&& pro, cpp2::AddEdgesRequest&& req, int retry = 0) noexcept;

  bool lockEdges(const cpp2::AddEdgesRequest& req);

  bool checkTerm(const cpp2::AddEdgesRequest& req);

  bool checkVersion(const cpp2::AddEdgesRequest& req);

  /**
   * @brief This is a call back function, to let AddEdgesProcessor so some
   *        addition thing for chain operation
   * @param batch if target edge has index
   * @param pData if target edge has no index.
   */
  void callbackOfChainOp(kvstore::BatchHolder& batch, std::vector<kvstore::KV>* pData);

  /**
   * @brief helper function to generate string form of keys of request
   */
  std::vector<std::string> sEdgeKey(const cpp2::AddEdgesRequest& req);

  /**
   * @brief normally, the prime/double prime keys will be deleted at AddEdgeProcessor
   *        (in a batch of finial edges), but sometimes ChainProcessor will stop early
   *        and we need this do the clean thing.
   */
  folly::SemiFuture<Code> abort();

  /**
   * @brief helper function to get the reversed request of the normal incoming req.
   *        (to use that for reversed edge)
   */
  cpp2::AddEdgesRequest reverseRequest(const cpp2::AddEdgesRequest& req);

  Code extractRpcError(const cpp2::ExecResponse& resp);

  /**
   * @brief   a normal AddEdgeRequest may contain multi edges
   *          even though they will fail or succeed as a batch in this time
   *          some of them may by overwrite by othere request
   *          so when resume each edge
   */
  cpp2::AddEdgesRequest makeSingleEdgeRequest(PartitionID partId, const cpp2::NewEdge& edge);

  std::vector<kvstore::KV> makePrime();

  std::vector<kvstore::KV> makeDoublePrime();

  void erasePrime();

  void eraseDoublePrime();

  folly::SemiFuture<Code> forwardToDelegateProcessor();

  /// if any operation failed or can not determined(RPC error)
  /// call this to leave a record in transaction manager
  /// the record can be scanned by the background resume thread
  /// then will do fail over logic
  void addUnfinishedEdge(ResumeType type);

  /*** consider the following case:
   *
   * create edge known(kdate datetime default datetime(), degree int);
   * insert edge known(degree) VALUES "100" -> "101":(95);
   *
   * storage will insert datetime() as default value on both
   * in/out edge, but they will calculate independent
   * which lead to inconsistance
   *
   * that why we need to replace the inconsistance prone value
   * at the monment the request comes
   * */
  void replaceNullWithDefaultValue(cpp2::AddEdgesRequest& req);

  std::string makeReadableEdge(const cpp2::AddEdgesRequest& req);

  int64_t toInt(const ::nebula::Value& val);

 protected:
  GraphSpaceID spaceId_;
  PartitionID localPartId_;
  PartitionID remotePartId_;
  cpp2::AddEdgesRequest req_;
  std::unique_ptr<TransactionManager::LockGuard> lk_{nullptr};
  int retryLimit_{10};
  TermID localTerm_{-1};
  // set to true when prime insert succeed
  // in processLocal(), we check this to determine if need to do abort()
  bool primeInserted_{false};

  std::vector<std::string> kvErased_;
  std::vector<kvstore::KV> kvAppend_;
  folly::Optional<int64_t> edgeVer_{folly::none};
  int64_t resumedEdgeVer_{-1};

  std::string uuid_;

  // for debug, edge "100"->"101" will print like 2231303022->2231303122
  // which is hard to recognize. Transform to human readable format
  std::string readableEdgeDesc_;
  ::nebula::meta::cpp2::PropertyType spaceVidType_{meta::cpp2::PropertyType::UNKNOWN};
};

}  // namespace storage
}  // namespace nebula
