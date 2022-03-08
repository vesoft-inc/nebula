/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_KV_PUTPROCESSOR_H_
#define STORAGE_KV_PUTPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kPutCounters;
/**
 * @brief this is a simple put() interface when storage run in KV mode.
 */
class PutProcessor : public BaseProcessor<cpp2::ExecResponse> {
 public:
  static PutProcessor* instance(StorageEnv* env,
                                const ProcessorCounters* counters = &kPutCounters) {
    return new PutProcessor(env, counters);
  }

  void process(const cpp2::KVPutRequest& req);

 private:
  PutProcessor(StorageEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::ExecResponse>(env, counters) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_KV_PUTPROCESSOR_H_
