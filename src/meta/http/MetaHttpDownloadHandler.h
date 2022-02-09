/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_HTTP_METAHTTPDOWNLOADHANDLER_H_
#define META_HTTP_METAHTTPDOWNLOADHANDLER_H_

#include <proxygen/httpserver/RequestHandler.h>

#include "common/base/Base.h"
#include "common/hdfs/HdfsHelper.h"
#include "common/thread/GenericThreadPool.h"
#include "kvstore/KVStore.h"
#include "webservice/Common.h"

namespace nebula {
namespace meta {

using nebula::HttpCode;

/**
 * @brief Download sst files from hdfs to every storaged download folder.
 *        It will send download http request to every storaged, letting them
 *        download the corressponding sst files.
 *        Functions such as onRequest, onBody... and requestComplete are inherited
 *        from RequestHandler, we will check request parameters in onRequest and
 *        call main logic in onEOM.
 */
class MetaHttpDownloadHandler : public proxygen::RequestHandler {
 public:
  MetaHttpDownloadHandler() = default;

  void init(nebula::kvstore::KVStore *kvstore,
            nebula::hdfs::HdfsHelper *helper,
            nebula::thread::GenericThreadPool *pool);

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError error) noexcept override;

 private:
  bool dispatchSSTFiles(const std::string &host, int32_t port, const std::string &path);

 private:
  HttpCode err_{HttpCode::SUCCEEDED};
  std::string hdfsHost_;
  int32_t hdfsPort_;
  std::string hdfsPath_;
  GraphSpaceID spaceID_;
  nebula::kvstore::KVStore *kvstore_;
  nebula::hdfs::HdfsHelper *helper_;
  nebula::thread::GenericThreadPool *pool_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_HTTP_METAHTTPDOWNLOADHANDLER_H_
