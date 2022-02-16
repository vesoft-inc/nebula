/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_INTERNALSTORAGEClient_H_
#define CLIENTS_STORAGE_INTERNALSTORAGEClient_H_

#include <folly/Optional.h>  // for Optional
#include <gtest/gtest_prod.h>
#include <stdint.h>  // for int64_t

#include <memory>   // for shared_ptr
#include <string>   // for string
#include <utility>  // for move
#include <vector>   // for vector

#include "clients/storage/StorageClientBase.h"  // for StorageClientBase
#include "clients/storage/StorageClientBase.h"  // for StorageClientBase...
#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "common/thrift/ThriftClientManager.h"  // for ThriftClientManager
#include "common/thrift/ThriftTypes.h"          // for TermID
#include "interface/gen-cpp2/InternalStorageServiceAsyncClient.h"
#include "interface/gen-cpp2/common_types.h"   // for ErrorCode
#include "interface/gen-cpp2/storage_types.h"  // for AddEdgesRequest (...

namespace nebula {
namespace meta {
class MetaClient;
}  // namespace meta
namespace storage {
namespace cpp2 {
class InternalStorageServiceAsyncClient;
}  // namespace cpp2
}  // namespace storage
}  // namespace nebula

namespace folly {
class EventBase;
class IOThreadPoolExecutor;
template <class>
class Promise;

class EventBase;
class IOThreadPoolExecutor;
template <class>
class Promise;
}  // namespace folly

namespace nebula {
namespace meta {
class MetaClient;
}  // namespace meta

namespace storage {
namespace cpp2 {
class InternalStorageServiceAsyncClient;
}  // namespace cpp2

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
                               folly::Optional<int64_t> optVersion,
                               folly::Promise<::nebula::cpp2::ErrorCode>&& p,
                               folly::EventBase* evb = nullptr);

  virtual void chainAddEdges(cpp2::AddEdgesRequest& req,
                             TermID termId,
                             folly::Optional<int64_t> optVersion,
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
                                             folly::Optional<int64_t> optVersion);
};

}  // namespace storage
}  // namespace nebula

#endif  // CLIENTS_STORAGE_INTERNALSTORAGEClient_H_
