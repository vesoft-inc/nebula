/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_MUTATE_DELETEVERTICESPROCESSOR_H_
#define STORAGE_MUTATE_DELETEVERTICESPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Prom...

#include <memory>   // for shared_ptr
#include <string>   // for string
#include <utility>  // for move
#include <vector>   // for vector

#include "common/base/Base.h"
#include "common/base/ErrorOr.h"               // for ErrorOr
#include "common/thrift/ThriftTypes.h"         // for GraphSpaceID, PartitionID
#include "interface/gen-cpp2/common_types.h"   // for ErrorCode
#include "interface/gen-cpp2/storage_types.h"  // for ExecResponse, DeleteVe...
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"  // for BaseProcessor
#include "storage/CommonUtils.h"    // for StorageEnv (ptr only)

namespace nebula {
struct Value;

namespace meta {
namespace cpp2 {
class IndexItem;

class IndexItem;
}  // namespace cpp2
}  // namespace meta
struct Value;

namespace storage {

extern ProcessorCounters kDelVerticesCounters;

class DeleteVerticesProcessor : public BaseProcessor<cpp2::ExecResponse> {
 public:
  static DeleteVerticesProcessor* instance(
      StorageEnv* env, const ProcessorCounters* counters = &kDelVerticesCounters) {
    return new DeleteVerticesProcessor(env, counters);
  }

  void process(const cpp2::DeleteVerticesRequest& req);

 private:
  DeleteVerticesProcessor(StorageEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::ExecResponse>(env, counters) {}

  ErrorOr<nebula::cpp2::ErrorCode, std::string> deleteVertices(PartitionID partId,
                                                               const std::vector<Value>& vertices,
                                                               std::vector<VMLI>& target);

 private:
  GraphSpaceID spaceId_;
  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_DELETEVERTICESPROCESSOR_H_
