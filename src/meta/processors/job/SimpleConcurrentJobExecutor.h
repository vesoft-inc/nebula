/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_SIMPLECONCURRENTJOBEXECUTOR_H_
#define META_SIMPLECONCURRENTJOBEXECUTOR_H_

#include <string>  // for string
#include <vector>  // for vector

#include "common/thrift/ThriftTypes.h"               // for JobID
#include "interface/gen-cpp2/common_types.h"         // for ErrorCode
#include "meta/processors/job/StorageJobExecutor.h"  // for StorageJobExecutor

namespace nebula {
namespace meta {
class AdminClient;
}  // namespace meta

namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {
class AdminClient;

class SimpleConcurrentJobExecutor : public StorageJobExecutor {
 public:
  SimpleConcurrentJobExecutor(JobID jobId,
                              kvstore::KVStore* kvstore,
                              AdminClient* adminClient,
                              const std::vector<std::string>& params);

  bool check() override;

  nebula::cpp2::ErrorCode prepare() override;

  nebula::cpp2::ErrorCode stop() override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SIMPLECONCURRENTJOBEXECUTOR_H_
