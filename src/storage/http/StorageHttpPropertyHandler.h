/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_HTTP_STORAGEHTTPPROPERTYHANDLER_H
#define STORAGE_HTTP_STORAGEHTTPPROPERTYHANDLER_H

#include <proxygen/httpserver/RequestHandler.h>   // for RequestHandler
#include <proxygen/lib/http/HTTPConstants.h>      // for UpgradeProtocol
#include <proxygen/lib/http/ProxygenErrorEnum.h>  // for ProxygenError

#include <memory>  // for unique_ptr
#include <string>  // for string, basic_string
#include <vector>  // for vector

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"  // for GraphSpaceID
#include "kvstore/KVStore.h"
#include "webservice/Common.h"  // for HttpCode, HttpCode:...

namespace nebula {
namespace kvstore {
class KVStore;
}  // namespace kvstore
namespace meta {
class SchemaManager;
}  // namespace meta
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
namespace meta {
class SchemaManager;
}  // namespace meta

namespace storage {

class StorageHttpPropertyHandler : public proxygen::RequestHandler {
 public:
  StorageHttpPropertyHandler(meta::SchemaManager* schemaMan, kvstore::KVStore* kv)
      : schemaMan_(schemaMan), kv_(kv) {}

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 private:
  meta::SchemaManager* schemaMan_ = nullptr;
  kvstore::KVStore* kv_ = nullptr;
  HttpCode err_{HttpCode::SUCCEEDED};
  std::string resp_;
  GraphSpaceID spaceId_;
  std::vector<std::string> properties_;
};

}  // namespace storage
}  // namespace nebula
#endif
