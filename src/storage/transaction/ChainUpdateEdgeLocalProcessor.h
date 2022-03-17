/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINUPDATEEDGEPROCESSORLOCAL_H
#define STORAGE_TRANSACTION_CHAINUPDATEEDGEPROCESSORLOCAL_H

#include <boost/stacktrace.hpp>

#include "common/utils/MemoryLockWrapper.h"
#include "storage/query/QueryBaseProcessor.h"
#include "storage/transaction/ChainBaseProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class ChainUpdateEdgeLocalProcessor
    : public QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse>,
      public ChainBaseProcessor {
  friend struct ChainUpdateEdgeTestHelper;

 public:
  using Code = ::nebula::cpp2::ErrorCode;
  static ChainUpdateEdgeLocalProcessor* instance(StorageEnv* env) {
    return new ChainUpdateEdgeLocalProcessor(env);
  }

  void process(const cpp2::UpdateEdgeRequest& req) override;

  folly::SemiFuture<Code> prepareLocal() override;

  folly::SemiFuture<Code> processRemote(Code code) override;

  folly::SemiFuture<Code> processLocal(Code code) override;

  void onProcessFinished() override {}

  void finish() override;

  virtual ~ChainUpdateEdgeLocalProcessor() = default;

 protected:
  explicit ChainUpdateEdgeLocalProcessor(StorageEnv* env)
      : QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse>(env, nullptr) {}

  std::string edgeKey(const cpp2::UpdateEdgeRequest& req);

  void doRpc(folly::Promise<Code>&& promise, int retry = 0) noexcept;

  folly::SemiFuture<Code> commit();

  folly::SemiFuture<Code> abort();

  bool prepareRequest(const cpp2::UpdateEdgeRequest& req);

  void erasePrime();

  void appendDoublePrime();

  std::string sEdgeKey(const cpp2::UpdateEdgeRequest& req);

  cpp2::UpdateEdgeRequest reverseRequest(const cpp2::UpdateEdgeRequest& req);

  bool setLock();

  void reportFailed(ResumeType type);

  int64_t getVersion(const cpp2::UpdateEdgeRequest& req);

  nebula::cpp2::ErrorCode getErrorCode(const cpp2::UpdateResponse& resp);

  Code checkAndBuildContexts(const cpp2::UpdateEdgeRequest& req) override;

  bool isKVStoreError(nebula::cpp2::ErrorCode code);

 protected:
  cpp2::UpdateEdgeRequest req_;
  TransactionManager::SPtrLock lkCore_;
  std::unique_ptr<TransactionManager::LockGuard> lk_;
  PartitionID localPartId_;
  int retryLimit_{10};
  TermID term_{-1};

  // set to true when prime insert succeed
  // in processLocal(), we check this to determine if need to do abort()
  bool primeInserted_{false};
  std::vector<std::string> kvErased_;
  std::vector<kvstore::KV> kvAppend_;
  std::optional<int64_t> ver_;
};

}  // namespace storage
}  // namespace nebula
#endif
