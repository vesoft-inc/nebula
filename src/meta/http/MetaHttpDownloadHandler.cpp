/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/http/MetaHttpDownloadHandler.h"

#include <folly/ExceptionWrapper.h>               // for operator<<
#include <folly/FBString.h>                       // for fbstring_core::cate...
#include <folly/Optional.h>                       // for Optional
#include <folly/Range.h>                          // for Range
#include <folly/String.h>                         // for join, split, string...
#include <folly/Try.h>                            // for Try::~Try<T>, Try::...
#include <folly/Try.h>                            // for Try
#include <folly/futures/Future.h>                 // for SemiFuture, collectAll
#include <folly/futures/Promise.h>                // for Promise::setWith
#include <folly/lang/Assume.h>                    // for assume_unreachable
#include <gflags/gflags_declare.h>                // for DECLARE_int32
#include <proxygen/httpserver/ResponseBuilder.h>  // for HTTPMethod, HTTPMet...
#include <proxygen/lib/http/HTTPMessage.h>        // for HTTPMessage
#include <proxygen/lib/http/HTTPMethod.h>         // for HTTPMethod, HTTPMet...
#include <proxygen/lib/http/ProxygenErrorEnum.h>  // for getErrorString, Pro...
#include <string.h>                               // for memcpy

#include <istream>        // for operator<<, basic_i...
#include <type_traits>    // for remove_reference<>:...
#include <unordered_map>  // for unordered_map, _Nod...
#include <utility>        // for move, pair
#include <vector>         // for vector

#include "common/base/Logging.h"              // for CheckNotNull, LOG
#include "common/base/StatusOr.h"             // for StatusOr
#include "common/datatypes/HostAddr.h"        // for HostAddr, hash
#include "common/hdfs/HdfsHelper.h"           // for HdfsHelper
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

void MetaHttpDownloadHandler::init(nebula::kvstore::KVStore *kvstore,
                                   nebula::hdfs::HdfsHelper *helper,
                                   nebula::thread::GenericThreadPool *pool) {
  kvstore_ = kvstore;
  helper_ = helper;
  pool_ = pool;
  CHECK_NOTNULL(kvstore_);
  CHECK_NOTNULL(helper_);
  CHECK_NOTNULL(pool_);
}

void MetaHttpDownloadHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
  if (headers->getMethod().value() != HTTPMethod::GET) {
    // Unsupported method
    err_ = HttpCode::E_UNSUPPORTED_METHOD;
    return;
  }

  if (!headers->hasQueryParam("host") || !headers->hasQueryParam("port") ||
      !headers->hasQueryParam("path") || !headers->hasQueryParam("space")) {
    LOG(INFO) << "Illegal Argument";
    err_ = HttpCode::E_ILLEGAL_ARGUMENT;
    return;
  }

  hdfsHost_ = headers->getQueryParam("host");
  hdfsPort_ = headers->getIntQueryParam("port");
  hdfsPath_ = headers->getQueryParam("path");
  spaceID_ = headers->getIntQueryParam("space");
}

void MetaHttpDownloadHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
  // Do nothing, we only support GET
}

void MetaHttpDownloadHandler::onEOM() noexcept {
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

  if (helper_->checkHadoopPath()) {
    if (dispatchSSTFiles(hdfsHost_, hdfsPort_, hdfsPath_)) {
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::OK),
                  WebServiceUtils::toString(HttpStatusCode::OK))
          .body("SSTFile dispatch successfully")
          .sendWithEOM();
    } else {
      LOG(INFO) << "SSTFile dispatch failed";
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::FORBIDDEN),
                  WebServiceUtils::toString(HttpStatusCode::FORBIDDEN))
          .body("SSTFile dispatch failed")
          .sendWithEOM();
    }
  } else {
    LOG(INFO) << "Hadoop Home not exist";
    ResponseBuilder(downstream_)
        .status(WebServiceUtils::to(HttpStatusCode::NOT_FOUND),
                WebServiceUtils::toString(HttpStatusCode::NOT_FOUND))
        .sendWithEOM();
  }
}

void MetaHttpDownloadHandler::onUpgrade(UpgradeProtocol) noexcept {
  // Do nothing
}

void MetaHttpDownloadHandler::requestComplete() noexcept {
  delete this;
}

void MetaHttpDownloadHandler::onError(ProxygenError error) noexcept {
  LOG(INFO) << "Web Service MetaHttpDownloadHandler got error : "
            << proxygen::getErrorString(error);
}

bool MetaHttpDownloadHandler::dispatchSSTFiles(const std::string &hdfsHost,
                                               int hdfsPort,
                                               const std::string &hdfsPath) {
  auto result = helper_->ls(hdfsHost, hdfsPort, hdfsPath);
  if (!result.ok()) {
    LOG(INFO) << "Dispatch SSTFile Failed";
    return false;
  }
  std::vector<std::string> files;
  folly::split("\n", result.value(), files, true);

  std::unique_ptr<kvstore::KVIterator> iter;
  auto prefix = MetaKeyUtils::partPrefix(spaceID_);
  auto ret = kvstore_->prefix(0, 0, prefix, &iter);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Fetch Parts Failed";
    return false;
  }

  int32_t partSize{0};
  std::unordered_map<HostAddr, std::vector<PartitionID>> hostPartition;
  while (iter->valid()) {
    auto key = iter->key();
    PartitionID partId;
    memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
    for (auto host : MetaKeyUtils::parsePartVal(iter->val())) {
      auto address = HostAddr(host.host, host.port);
      auto addressIter = hostPartition.find(address);
      if (addressIter == hostPartition.end()) {
        std::vector<PartitionID> partitions;
        hostPartition.emplace(address, partitions);
      }
      hostPartition[address].emplace_back(partId);
    }
    partSize++;
    iter->next();
  }

  std::vector<folly::SemiFuture<bool>> futures;
  for (auto &pair : hostPartition) {
    std::string partsStr;
    folly::join(",", pair.second, partsStr);

    auto hostaddr = pair.first.host;
    auto dispatcher = [hostaddr, hdfsHost, hdfsPort, hdfsPath, partsStr, this]() {
      static const char *tmp = "http://%s:%d/%s?host=%s&port=%d&path=%s&parts=%s&space=%d";
      std::string url = folly::stringPrintf(tmp,
                                            hostaddr.c_str(),
                                            FLAGS_ws_storage_http_port,
                                            "download",
                                            hdfsHost.c_str(),
                                            hdfsPort,
                                            hdfsPath.c_str(),
                                            partsStr.c_str(),
                                            spaceID_);
      auto downloadResult = nebula::http::HttpClient::get(url);
      return downloadResult.ok() && downloadResult.value() == "SSTFile download successfully";
    };
    auto future = pool_->addTask(dispatcher);
    futures.push_back(std::move(future));
  }

  bool successfully{true};
  auto tries = folly::collectAll(std::move(futures)).get();
  for (const auto &t : tries) {
    if (t.hasException()) {
      LOG(INFO) << "Download Failed: " << t.exception();
      successfully = false;
      break;
    }
    if (!t.value()) {
      successfully = false;
      break;
    }
  }

  LOG(INFO) << "Download tasks have finished";
  return successfully;
}

}  // namespace meta
}  // namespace nebula
