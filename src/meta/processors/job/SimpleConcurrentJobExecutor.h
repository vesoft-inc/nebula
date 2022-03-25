/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_SIMPLECONCURRENTJOBEXECUTOR_H_
#define META_SIMPLECONCURRENTJOBEXECUTOR_H_

#include "interface/gen-cpp2/common_types.h"
#include "meta/processors/job/StorageJobExecutor.h"

namespace nebula {
namespace meta {

class SimpleConcurrentJobExecutor : public StorageJobExecutor {
 public:
  SimpleConcurrentJobExecutor(GraphSpaceID space,
                              JobID jobId,
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
