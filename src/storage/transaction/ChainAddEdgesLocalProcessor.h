/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINADDEDGESPROCESSORLOCAL_H
#define STORAGE_TRANSACTION_CHAINADDEDGESPROCESSORLOCAL_H

#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class ChainAddEdgesLocalProcessor : public BaseProcessor<cpp2::ExecResponse>,
                                    public ChainBaseProcessor {
  friend class ChainResumeProcessorTestHelper;  // for test friendly
 public:
  static ChainAddEdgesLocalProcessor* instance(StorageEnv* env) {
    return new ChainAddEdgesLocalProcessor(env);
  }

  virtual ~ChainAddEdgesLocalProcessor() = default;

  virtual void process(const cpp2::AddEdgesRequest& req);

  folly::SemiFuture<Code> prepareLocal() override;

  folly::SemiFuture<Code> processRemote(Code code) override;

  folly::SemiFuture<Code> processLocal(Code code) override;

  void setRemotePartId(PartitionID remotePartId) {
    remotePartId_ = remotePartId;
  }

  void finish() override;

 protected:
  explicit ChainAddEdgesLocalProcessor(StorageEnv* env) : BaseProcessor<cpp2::ExecResponse>(env) {}

  bool prepareRequest(const cpp2::AddEdgesRequest& req);

  /**
   * @brief resume and set req_ & txnId_ from the val of (double)prime
   * @return true if resume succeeded
   */
  bool deserializeRequest(GraphSpaceID spaceId, PartitionID partId, folly::StringPiece val);

  void doRpc(folly::Promise<Code>&& pro, cpp2::AddEdgesRequest&& req, int retry = 0) noexcept;

  bool lockEdges(const cpp2::AddEdgesRequest& req);

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
  std::vector<std::string> toStrKeys(const cpp2::AddEdgesRequest& req);

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
   *          some of them may by overwrite by other request
   *          so when resume each edge
   */
  cpp2::AddEdgesRequest makeSingleEdgeRequest(PartitionID partId, const cpp2::NewEdge& edge);

  std::vector<kvstore::KV> makePrime();

  std::vector<kvstore::KV> makeDoublePrime();

  void erasePrime();

  void eraseDoublePrime();

  /**
   * @brief will call normal AddEdgesProcess to do real insert.
   *
   * @return folly::SemiFuture<Code>
   */
  folly::SemiFuture<Code> commit();

  /// if any operation failed or can not determined(RPC error)
  /// call this to leave a record in transaction manager
  /// the record can be scanned by the background resume thread
  /// then will do fail over logic
  void reportFailed(ResumeType type);

  /*** consider the following case:
   *
   * create edge known(kdate datetime default datetime(), degree int);
   * insert edge known(degree) VALUES "100" -> "101":(95);
   *
   * storage will insert datetime() as default value on both
   * in/out edge, but they will calculate independent
   * which lead to inconsistency
   *
   * that why we need to replace the inconsistency prone value
   * at the moment the request comes
   * */
  void replaceNullWithDefaultValue(cpp2::AddEdgesRequest& req);

  /**
   * @brief check is an error code belongs to kv store
   *        we can do retry / recover if we meet a kv store error
   *        but if we meet a logical error (retry will alwasy failed)
   *        we should return error directly.
   * @param code
   * @return true
   * @return false
   */
  bool isKVStoreError(Code code);

  std::string makeReadableEdge(const cpp2::AddEdgesRequest& req);

 protected:
  GraphSpaceID spaceId_;
  PartitionID localPartId_;
  PartitionID remotePartId_;
  cpp2::AddEdgesRequest req_;
  TransactionManager::SPtrLock lkCore_;
  std::unique_ptr<TransactionManager::LockGuard> lk_{nullptr};
  int retryLimit_{10};

  std::vector<std::string> kvErased_;
  std::vector<kvstore::KV> kvAppend_;
  std::optional<int64_t> edgeVer_;

  // for trace purpose
  std::string uuid_;

  // as we print all description in finish(),
  // we can log execution clue in this
  std::string execDesc_;

  // for debug, edge "100"->"101" will print like 2231303022->2231303122
  // which is hard to recognize. Transform to human readable format
  std::string readableEdgeDesc_;
};

}  // namespace storage
}  // namespace nebula
#endif
