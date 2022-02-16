/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_HTTP_STORAGEHTTPINGESTHANDLER_H_
#define STORAGE_HTTP_STORAGEHTTPINGESTHANDLER_H_

#include <proxygen/httpserver/RequestHandler.h>   // for RequestHandler
#include <proxygen/lib/http/HTTPConstants.h>      // for UpgradeProtocol
#include <proxygen/lib/http/ProxygenErrorEnum.h>  // for ProxygenError

#include <memory>  // for unique_ptr

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"  // for GraphSpaceID
#include "kvstore/KVStore.h"
#include "webservice/Common.h"  // for HttpCode, HttpCode:...

namespace nebula {
namespace kvstore {
class KVStore;
}  // namespace kvstore
}  // namespace nebula
namespace proxygen {
class HTTPMessage;
}  // namespace proxygen

namespace folly {
class IOBuf;

class IOBuf;
}  // namespace folly
namespace proxygen {
class HTTPMessage;
}  // namespace proxygen

namespace nebula {
namespace kvstore {
class KVStore;
}  // namespace kvstore

namespace storage {

using nebula::HttpCode;

class StorageHttpIngestHandler : public proxygen::RequestHandler {
 public:
  StorageHttpIngestHandler() = default;

  void init(nebula::kvstore::KVStore *kvstore);

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError error) noexcept override;

  bool ingestSSTFiles(GraphSpaceID space);

 private:
  HttpCode err_{HttpCode::SUCCEEDED};
  nebula::kvstore::KVStore *kvstore_;
  GraphSpaceID space_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_HTTP_STORAGEHTTPINGESTHANDLER_H_
