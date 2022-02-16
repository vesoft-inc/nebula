/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_MUTATE_ADDVERTICESPROCESSOR_H_
#define STORAGE_MUTATE_ADDVERTICESPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Prom...

#include <memory>   // for shared_ptr
#include <string>   // for string
#include <utility>  // for move
#include <vector>   // for vector

#include "common/base/Base.h"
#include "common/base/ConcurrentLRUCache.h"
#include "common/base/ErrorOr.h"               // for ErrorOr
#include "common/thrift/ThriftTypes.h"         // for PartitionID, VertexID
#include "interface/gen-cpp2/common_types.h"   // for ErrorCode
#include "interface/gen-cpp2/storage_types.h"  // for ExecResponse, AddVerti...
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"  // for BaseProcessor
#include "storage/CommonUtils.h"    // for StorageEnv (ptr only)

namespace nebula {
class RowReader;
namespace meta {
class SchemaProviderIf;
namespace cpp2 {
class IndexItem;
}  // namespace cpp2
}  // namespace meta

class RowReader;
namespace meta {
class SchemaProviderIf;
namespace cpp2 {
class IndexItem;
}  // namespace cpp2
}  // namespace meta

namespace storage {

extern ProcessorCounters kAddVerticesCounters;

class AddVerticesProcessor : public BaseProcessor<cpp2::ExecResponse> {
 public:
  static AddVerticesProcessor* instance(StorageEnv* env,
                                        const ProcessorCounters* counters = &kAddVerticesCounters) {
    return new AddVerticesProcessor(env, counters);
  }

  void process(const cpp2::AddVerticesRequest& req);

  void doProcess(const cpp2::AddVerticesRequest& req);

  void doProcessWithIndex(const cpp2::AddVerticesRequest& req);

 private:
  AddVerticesProcessor(StorageEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::ExecResponse>(env, counters) {}

  ErrorOr<nebula::cpp2::ErrorCode, std::string> findOldValue(PartitionID partId,
                                                             const VertexID& vId,
                                                             TagID tagId);

  std::vector<std::string> indexKeys(PartitionID partId,
                                     const VertexID& vId,
                                     RowReader* reader,
                                     std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
                                     const meta::SchemaProviderIf* latestSchema);

  void deleteDupVid(std::vector<cpp2::NewVertex>& vertices);

 private:
  GraphSpaceID spaceId_;
  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;
  bool ifNotExists_{false};
  bool ignoreExistedIndex_{false};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_ADDVERTICESPROCESSOR_H_
