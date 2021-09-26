/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <proxygen/httpserver/RequestHandler.h>

#include "common/base/Base.h"
#include "kvstore/KVStore.h"
#include "webservice/Common.h"

namespace nebula {
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
