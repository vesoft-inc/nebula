/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_INTERNALSTORAGEClient_H_
#define CLIENTS_STORAGE_INTERNALSTORAGEClient_H_

#include "clients/storage/StorageClientBase.h"
#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "common/thrift/ThriftClientManager.h"
#include "interface/gen-cpp2/InternalStorageServiceAsyncClient.h"

namespace nebula {
namespace storage {

/**
 * A wrapper class for InternalStorageServiceAsyncClient thrift API
 *
 * The class is NOT reentrant
 */
class InternalStorageClient
    : public StorageClientBase<
          cpp2::InternalStorageServiceAsyncClient,
          thrift::ThriftClientManager<cpp2::InternalStorageServiceAsyncClient>> {
  using Parent =
      StorageClientBase<cpp2::InternalStorageServiceAsyncClient,
                        thrift::ThriftClientManager<cpp2::InternalStorageServiceAsyncClient>>;

 public:
  InternalStorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                        meta::MetaClient* metaClient)
      : Parent(ioThreadPool, metaClient) {}
  virtual ~InternalStorageClient() = default;

  virtual void chainUpdateEdge(cpp2::UpdateEdgeRequest& reversedRequest,
                               TermID termOfSrc,
                               std::optional<int64_t> optVersion,
                               folly::Promise<::nebula::cpp2::ErrorCode>&& p,
                               folly::EventBase* evb = nullptr);

  virtual void chainAddEdges(cpp2::AddEdgesRequest& req,
                             TermID termId,
                             std::optional<int64_t> optVersion,
                             folly::Promise<::nebula::cpp2::ErrorCode>&& p,
                             folly::EventBase* evb = nullptr);

  virtual void chainDeleteEdges(cpp2::DeleteEdgesRequest& req,
                                const std::string& txnId,
                                TermID termId,
                                folly::Promise<::nebula::cpp2::ErrorCode>&& p,
                                folly::EventBase* evb = nullptr);

 private:
  cpp2::ChainAddEdgesRequest makeChainAddReq(const cpp2::AddEdgesRequest& req,
                                             TermID termId,
                                             std::optional<int64_t> optVersion);
};

}  // namespace storage
}  // namespace nebula

#endif  // CLIENTS_STORAGE_INTERNALSTORAGEClient_H_
