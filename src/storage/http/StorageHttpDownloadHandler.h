/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_HTTP_STORAGEHTTPDOWNLOADHANDLER_H_
#define STORAGE_HTTP_STORAGEHTTPDOWNLOADHANDLER_H_

#include <proxygen/httpserver/RequestHandler.h>   // for RequestHandler
#include <proxygen/lib/http/HTTPConstants.h>      // for UpgradeProtocol
#include <proxygen/lib/http/ProxygenErrorEnum.h>  // for ProxygenError
#include <stdint.h>                               // for int32_t

#include <memory>  // for unique_ptr
#include <string>  // for string, basic_string
#include <vector>  // for vector

#include "common/base/Base.h"
#include "common/hdfs/HdfsHelper.h"
#include "common/thread/GenericThreadPool.h"
#include "common/thrift/ThriftTypes.h"  // for GraphSpaceID
#include "kvstore/KVStore.h"
#include "webservice/Common.h"  // for HttpCode, HttpCode:...

namespace nebula {
namespace hdfs {
class HdfsHelper;
}  // namespace hdfs
namespace kvstore {
class KVStore;
}  // namespace kvstore
namespace thread {
class GenericThreadPool;
}  // namespace thread
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
namespace hdfs {
class HdfsHelper;
}  // namespace hdfs
namespace kvstore {
class KVStore;
}  // namespace kvstore
namespace thread {
class GenericThreadPool;
}  // namespace thread

namespace storage {

using nebula::HttpCode;

class StorageHttpDownloadHandler : public proxygen::RequestHandler {
 public:
  StorageHttpDownloadHandler() = default;

  void init(nebula::hdfs::HdfsHelper *helper,
            nebula::thread::GenericThreadPool *pool,
            nebula::kvstore::KVStore *kvstore,
            std::vector<std::string> paths);

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError error) noexcept override;

 private:
  bool downloadSSTFiles(const std::string &url,
                        int port,
                        const std::string &path,
                        const std::vector<std::string> &parts);

 private:
  HttpCode err_{HttpCode::SUCCEEDED};
  GraphSpaceID spaceID_;
  std::string hdfsHost_;
  int32_t hdfsPort_;
  std::string hdfsPath_;
  std::string partitions_;
  nebula::hdfs::HdfsHelper *helper_;
  nebula::thread::GenericThreadPool *pool_;
  nebula::kvstore::KVStore *kvstore_;
  std::vector<std::string> paths_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_HTTP_STORAGEHTTPDOWNLOADHANDLER_H_
