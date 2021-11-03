/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_MUTATE_DELETETAGSPROCESSOR_H_
#define STORAGE_MUTATE_DELETETAGSPROCESSOR_H_

#include "common/base/Base.h"
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"
#include "storage/CommonUtils.h"

namespace nebula {
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
