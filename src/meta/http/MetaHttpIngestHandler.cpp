/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/http/MetaHttpIngestHandler.h"

#include <folly/ExceptionWrapper.h>  // for operator<<
#include <folly/FBString.h>          // for fbstring_core::cate...
#include <folly/Optional.h>          // for Optional
#include <folly/String.h>            // for stringPrintf
#include <folly/Try.h>               // for Try
#include <folly/futures/Future.h>    // for SemiFuture, collectAll
#include <folly/futures/Promise.h>   // for PromiseException::P...
#include <folly/lang/Assume.h>       // for assume_unreachable
#include <gflags/gflags_declare.h>   // for DECLARE_int32
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/lib/http/HTTPMessage.h>        // for HTTPMessage
#include <proxygen/lib/http/HTTPMethod.h>         // for HTTPMethod, HTTPMet...
#include <proxygen/lib/http/ProxygenErrorEnum.h>  // for getErrorString, Pro...

#include <istream>      // for operator<<, basic_i...
#include <set>          // for set, operator!=
#include <string>       // for string, basic_string
#include <type_traits>  // for remove_reference<>:...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/Logging.h"              // for CheckNotNull, LOG
#include "common/base/StatusOr.h"             // for StatusOr
#include "common/datatypes/HostAddr.h"        // for HostAddr
#include "common/http/HttpClient.h"           // for HttpClient
#include "common/thread/GenericThreadPool.h"  // for GenericThreadPool
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCod...
#include "kvstore/KVIterator.h"               // for KVIterator
#include "kvstore/KVStore.h"                  // for KVStore
#include "webservice/Common.h"                // for HttpStatusCode, Web...

namespace folly {
class IOBuf;

class IOBuf;
}  // namespace folly

DECLARE_int32(ws_storage_http_port);

namespace nebula {
namespace meta {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::ResponseBuilder;
using proxygen::UpgradeProtocol;

void MetaHttpIngestHandler::init(nebula::kvstore::KVStore *kvstore,
                                 nebula::thread::GenericThreadPool *pool) {
  kvstore_ = kvstore;
  pool_ = pool;
  CHECK_NOTNULL(kvstore_);
  CHECK_NOTNULL(pool_);
}

void MetaHttpIngestHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
  if (headers->getMethod().value() != HTTPMethod::GET) {
    // Unsupported method
    err_ = HttpCode::E_UNSUPPORTED_METHOD;
    return;
  }

  if (!headers->hasQueryParam("space")) {
    err_ = HttpCode::E_ILLEGAL_ARGUMENT;
    return;
  }

  space_ = headers->getIntQueryParam("space");
}

void MetaHttpIngestHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
  // Do nothing, we only support GET
}

void MetaHttpIngestHandler::onEOM() noexcept {
  switch (err_) {
    case HttpCode::E_UNSUPPORTED_METHOD:
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                  WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
          .sendWithEOM();
      return;
    case HttpCode::E_ILLEGAL_ARGUMENT:
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::BAD_REQUEST),
                  WebServiceUtils::toString(HttpStatusCode::BAD_REQUEST))
          .sendWithEOM();
      return;
    default:
      break;
  }

  if (ingestSSTFiles(space_)) {
    LOG(INFO) << "SSTFile ingest successfully ";
    ResponseBuilder(downstream_)
        .status(WebServiceUtils::to(HttpStatusCode::OK),
                WebServiceUtils::toString(HttpStatusCode::OK))
        .body("SSTFile ingest successfully")
        .sendWithEOM();
  } else {
    LOG(INFO) << "SSTFile ingest failed";
    ResponseBuilder(downstream_)
        .status(WebServiceUtils::to(HttpStatusCode::FORBIDDEN),
                WebServiceUtils::toString(HttpStatusCode::FORBIDDEN))
        .body("SSTFile ingest failed")
        .sendWithEOM();
  }
}

void MetaHttpIngestHandler::onUpgrade(UpgradeProtocol) noexcept {
  // Do nothing
}

void MetaHttpIngestHandler::requestComplete() noexcept {
  delete this;
}

void MetaHttpIngestHandler::onError(ProxygenError error) noexcept {
  LOG(INFO) << "Web Service MetaHttpIngestHandler got error : " << proxygen::getErrorString(error);
}

bool MetaHttpIngestHandler::ingestSSTFiles(GraphSpaceID space) {
  std::unique_ptr<kvstore::KVIterator> iter;
  auto prefix = MetaKeyUtils::partPrefix(space);

  static const GraphSpaceID metaSpaceId = 0;
  static const PartitionID metaPartId = 0;
  auto ret = kvstore_->prefix(metaSpaceId, metaPartId, prefix, &iter);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Fetch Parts Failed";
    return false;
  }

  std::set<std::string> storageIPs;
  while (iter->valid()) {
    for (auto &host : MetaKeyUtils::parsePartVal(iter->val())) {
      if (storageIPs.count(host.host) == 0) {
        storageIPs.insert(std::move(host.host));
      }
    }
    iter->next();
  }

  std::vector<folly::SemiFuture<bool>> futures;

  for (auto &storageIP : storageIPs) {
    auto dispatcher = [storageIP, space]() {
      static const char *tmp = "http://%s:%d/ingest?space=%d";
      auto url = folly::stringPrintf(tmp, storageIP.c_str(), FLAGS_ws_storage_http_port, space);
      auto ingestResult = nebula::http::HttpClient::get(url);
      return ingestResult.ok() && ingestResult.value() == "SSTFile ingest successfully";
    };
    auto future = pool_->addTask(dispatcher);
    futures.push_back(std::move(future));
  }

  bool successfully{true};
  auto tries = folly::collectAll(std::move(futures)).get();
  for (const auto &t : tries) {
    if (t.hasException()) {
      LOG(INFO) << "Ingest Failed: " << t.exception();
      successfully = false;
      break;
    }
    if (!t.value()) {
      successfully = false;
      break;
    }
  }
  LOG(INFO) << "Ingest tasks have finished";
  return successfully;
}

}  // namespace meta
}  // namespace nebula
