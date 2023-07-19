/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_MUTATE_DELETEEDGESPROCESSOR_H_
#define STORAGE_MUTATE_DELETEEDGESPROCESSOR_H_

#include "common/base/Base.h"
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"
#include "storage/transaction/ConsistTypes.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kDelEdgesCounters;

class DeleteEdgesProcessor : public BaseProcessor<cpp2::ExecResponse> {
 public:
  static DeleteEdgesProcessor* instance(StorageEnv* env,
                                        const ProcessorCounters* counters = &kDelEdgesCounters) {
    return new DeleteEdgesProcessor(env, counters);
  }

  void process(const cpp2::DeleteEdgesRequest& req);

  using HookFunction = std::function<void(HookFuncPara&)>;
  void setHookFunc(HookFunction func) {
    tossHookFunc_ = func;
  }

 private:
  DeleteEdgesProcessor(StorageEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::ExecResponse>(env, counters) {}

  ErrorOr<nebula::cpp2::ErrorCode, std::string> deleteEdges(
      PartitionID partId, const std::vector<cpp2::EdgeKey>& edges);

 private:
  GraphSpaceID spaceId_;
  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;

 protected:
  // TOSS use this hook function to append some delete operation
  // or may append some put operation
  std::optional<HookFunction> tossHookFunc_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_DELETEEDGESPROCESSOR_H_
