/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_KV_REMOVEPROCESSOR_H_
#define STORAGE_KV_REMOVEPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kRemoveCounters;

/**
 * @brief this is a simple remove() interface when storage run in KV mode.
 */
class RemoveProcessor : public BaseProcessor<cpp2::ExecResponse> {
 public:
  static RemoveProcessor* instance(StorageEnv* env,
                                   const ProcessorCounters* counters = &kRemoveCounters) {
    return new RemoveProcessor(env, counters);
  }

  void process(const cpp2::KVRemoveRequest& req);

 private:
  RemoveProcessor(StorageEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::ExecResponse>(env, counters) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_KV_REMOVEPROCESSOR_H_
