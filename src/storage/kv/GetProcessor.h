/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_KV_GETPROCESSOR_H_
#define STORAGE_KV_GETPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kGetCounters;

/**
 * @brief this is a simple get() interface when storage run in KV mode.
 */
class GetProcessor : public BaseProcessor<cpp2::KVGetResponse> {
 public:
  static GetProcessor* instance(StorageEnv* env,
                                const ProcessorCounters* counters = &kGetCounters) {
    return new GetProcessor(env, counters);
  }

  void process(const cpp2::KVGetRequest& req);

 protected:
  GetProcessor(StorageEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::KVGetResponse>(env, counters) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_KV_GETPROCESSOR_H_
