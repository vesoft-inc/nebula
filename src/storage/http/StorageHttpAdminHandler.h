/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_HTTP_STORAGEHTTPADMINHANDLER_H_
#define STORAGE_HTTP_STORAGEHTTPADMINHANDLER_H_

#include <proxygen/httpserver/RequestHandler.h>   // for RequestHandler
#include <proxygen/lib/http/HTTPConstants.h>      // for UpgradeProtocol
#include <proxygen/lib/http/ProxygenErrorEnum.h>  // for ProxygenError

#include <memory>  // for unique_ptr
#include <string>  // for string

#include "common/base/Base.h"
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

using nebula::HttpCode;

class StorageHttpAdminHandler : public proxygen::RequestHandler {
 public:
  StorageHttpAdminHandler(meta::SchemaManager* schemaMan, kvstore::KVStore* kv)
      : schemaMan_(schemaMan), kv_(kv) {}

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError error) noexcept override;

 private:
  HttpCode err_{HttpCode::SUCCEEDED};
  std::string resp_;
  meta::SchemaManager* schemaMan_ = nullptr;
  kvstore::KVStore* kv_ = nullptr;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_HTTP_STORAGEHTTPADMINHANDLER_H_
