/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_MUTATE_DELETETAGSPROCESSOR_H_
#define STORAGE_MUTATE_DELETETAGSPROCESSOR_H_

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
#include "interface/gen-cpp2/storage_types.h"  // for ExecResponse, DelTags ...
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"  // for BaseProcessor
#include "storage/CommonUtils.h"    // for StorageEnv (ptr only)

namespace nebula {
namespace meta {
namespace cpp2 {
class IndexItem;

class IndexItem;
}  // namespace cpp2
}  // namespace meta

namespace storage {

extern ProcessorCounters kDelTagsCounters;

class DeleteTagsProcessor : public BaseProcessor<cpp2::ExecResponse> {
 public:
  static DeleteTagsProcessor* instance(StorageEnv* env,
                                       const ProcessorCounters* counters = &kDelTagsCounters) {
    return new DeleteTagsProcessor(env, counters);
  }

  void process(const cpp2::DeleteTagsRequest& req);

 private:
  DeleteTagsProcessor(StorageEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::ExecResponse>(env, counters) {}

  ErrorOr<nebula::cpp2::ErrorCode, std::string> deleteTags(
      PartitionID partId, const std::vector<cpp2::DelTags>& delTags, std::vector<VMLI>& lockKeys);

 private:
  GraphSpaceID spaceId_;
  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_DELETETAGSPROCESSOR_H_
