/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINDELETEEDGESLOCALPROCESSOR_H
#define STORAGE_TRANSACTION_CHAINDELETEEDGESLOCALPROCESSOR_H

#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class ChainDeleteEdgesLocalProcessor : public BaseProcessor<cpp2::ExecResponse>,
                                       public ChainBaseProcessor {
  friend class ChainResumeProcessorTestHelper;  // for test friendly
 public:
  static ChainDeleteEdgesLocalProcessor* instance(StorageEnv* env) {
    return new ChainDeleteEdgesLocalProcessor(env);
  }

  virtual ~ChainDeleteEdgesLocalProcessor() = default;

  virtual void process(const cpp2::DeleteEdgesRequest& req);

  folly::SemiFuture<Code> prepareLocal() override;

  folly::SemiFuture<Code> processRemote(Code code) override;

  folly::SemiFuture<Code> processLocal(Code code) override;

  void finish() override;

 protected:
  explicit ChainDeleteEdgesLocalProcessor(StorageEnv* env)
      : BaseProcessor<cpp2::ExecResponse>(env) {}

  Code checkRequest(const cpp2::DeleteEdgesRequest& req);

  void doRpc(folly::Promise<Code>&& pro, cpp2::DeleteEdgesRequest&& req, int retry = 0) noexcept;

  bool lockEdges(const cpp2::DeleteEdgesRequest& req);

  /**
   * @brief This is a hook function, inject to DeleteEdgesProcessor,
   *        called before DeleteEdgesProcessor ready to commit something
   */
  void hookFunc(HookFuncPara& para);

  /**
   * @brief if remote side explicit reported faild, called this
   */
  folly::SemiFuture<Code> abort();

  /**
   * @brief call DeleteEdgesProcessor to do the real thing
   */
  folly::SemiFuture<Code> commitLocal();

  std::vector<kvstore::KV> makePrime(const cpp2::DeleteEdgesRequest& req);

  /**
   * @brief generate reversed request of the incoming req.
   */
  cpp2::DeleteEdgesRequest reverseRequest(const cpp2::DeleteEdgesRequest& req);

  /**
   * @brief wrapper function to get error code from ExecResponse
   */
  Code extractRpcError(const cpp2::ExecResponse& resp);

  /**
   * @brief if any operation failed or can not determined(RPC error)
   * call this to leave a record in transaction manager
   * the record can be scanned by the background resume thread
   * then will do fail over logic
   */
  void reportFailed(ResumeType type);

 protected:
  GraphSpaceID spaceId_;
  PartitionID localPartId_;
  PartitionID remotePartId_;
  cpp2::DeleteEdgesRequest req_;
  TransactionManager::SPtrLock lkCore_;
  std::unique_ptr<TransactionManager::LockGuard> lk_{nullptr};
  int retryLimit_{10};
  /**
   * @brief this is the term when prepare called,
   * and must be kept during the whole execution
   * if not, will return OUT_OF_TERM ERROR
   */
  TermID term_{-1};

  // set to true when prime insert succeed
  // in processLocal(), we check this to determine if need to do abort()
  bool setPrime_{false};

  bool setDoublePrime_{false};

  std::vector<kvstore::KV> primes_;

  std::vector<kvstore::KV> doublePrimes_;

  std::string txnId_;

  // for debug, edge "100"->"101" will print like 2231303022->2231303122
  // which is hard to recognize. Transform to human readable format
  std::string readableEdgeDesc_;
};

}  // namespace storage
}  // namespace nebula
#endif
