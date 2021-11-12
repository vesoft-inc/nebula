/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_PLUGINS_HBASE_HBASECLIENT_H_
#define KVSTORE_PLUGINS_HBASE_HBASECLIENT_H_

#include <folly/executors/IOThreadPoolExecutor.h>

#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "common/base/Status.h"
#include "common/thrift/ThriftClientManager.h"
#include "gen-cpp2/THBaseServiceAsyncClient.h"
#include "gen-cpp2/hbase_types.h"
#include "kvstore/Common.h"

namespace nebula {
namespace kvstore {

using namespace apache::hadoop::hbase::thrift2::cpp2;  // NOLINT

class HBaseClient final {
 public:
  explicit HBaseClient(const HostAddr& host);
  ~HBaseClient();

  ResultCode get(const std::string& tableName, const std::string& rowKey, KVMap& data);

  std::pair<ResultCode, std::vector<Status>> multiGet(
      const std::string& tableName,
      const std::vector<std::string>& rowKeys,
      std::vector<std::pair<std::string, KVMap>>& dataList);

  ResultCode put(const std::string& tableName, std::string& rowKey, std::vector<KV>& data);

  ResultCode multiPut(const std::string& tableName,
                      std::vector<std::pair<std::string, std::vector<KV>>>& dataList);

  ResultCode range(const std::string& tableName,
                   const std::string& start,
                   const std::string& end,
                   std::vector<std::pair<std::string, KVMap>>& dataList);

  ResultCode remove(const std::string& tableName, const std::string& rowKey);

  ResultCode multiRemove(const std::string& tableName, std::vector<std::string>& rowKeys);

 private:
  std::shared_ptr<thrift::ThriftClientManager<THBaseServiceAsyncClient>> clientsMan_;

  std::shared_ptr<THBaseServiceAsyncClient> client_;

  const int32_t kScanRowNum = {1024};
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PLUGINS_HBASE_HBASECLIENT_H_
