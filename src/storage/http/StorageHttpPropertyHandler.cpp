/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/http/StorageHttpPropertyHandler.h"

#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>

#include "common/base/Base.h"

namespace nebula {
namespace storage {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::ResponseBuilder;
using proxygen::UpgradeProtocol;

void StorageHttpPropertyHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
  if (headers->getMethod().value() != HTTPMethod::GET) {
    // Unsupported method
    resp_ = "Not supported";
    err_ = HttpCode::E_UNSUPPORTED_METHOD;
    return;
  }

  do {
    if (headers->hasQueryParam("space")) {
      auto spaceName = headers->getQueryParam("space");
      auto ret = schemaMan_->toGraphSpaceID(spaceName);
      if (!ret.ok()) {
        resp_ = "Space not found: " + spaceName;
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        break;
      }
      spaceId_ = ret.value();
    } else {
      resp_ =
          "Space should not be empty. "
          "Usage: http://ip:port/rocksdb_property?space=xxx&property=yyy";
      err_ = HttpCode::E_ILLEGAL_ARGUMENT;
      break;
    }

    if (headers->hasQueryParam("property")) {
      folly::split(",", headers->getQueryParam("property"), properties_, true);
    } else {
      resp_ =
          "Property should not be empty. "
          "Usage: http://ip:port/rocksdb_property?space=xxx&property=yyy";
      err_ = HttpCode::E_ILLEGAL_ARGUMENT;
      break;
    }

    auto result = folly::dynamic::array();
    for (const auto& property : properties_) {
      auto ret = kv_->getProperty(spaceId_, property);
      if (!ok(ret)) {
        resp_ = "Property not found: " + property;
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
      } else {
        result.push_back(folly::parseJson(value(ret)));
      }
    }
    resp_ = folly::toPrettyJson(result);
  } while (false);
}

void StorageHttpPropertyHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
  // Do nothing, we only support GET
}

void StorageHttpPropertyHandler::onEOM() noexcept {
  switch (err_) {
    case HttpCode::E_UNSUPPORTED_METHOD:
      ResponseBuilder(downstream_).status(405, "Method not allowed").body(resp_).sendWithEOM();
      return;
    case HttpCode::E_ILLEGAL_ARGUMENT:
      ResponseBuilder(downstream_).status(400, "Illegal argument").body(resp_).sendWithEOM();
      return;
    default:
      break;
  }

  ResponseBuilder(downstream_).status(200, "OK").body(resp_).sendWithEOM();
}

void StorageHttpPropertyHandler::onUpgrade(UpgradeProtocol) noexcept {
  // Do nothing
}

void StorageHttpPropertyHandler::requestComplete() noexcept {
  delete this;
}

void StorageHttpPropertyHandler::onError(ProxygenError error) noexcept {
  LOG(ERROR) << "Web service StorageHttpHandler got error: " << proxygen::getErrorString(error);
}

}  // namespace storage
}  // namespace nebula
