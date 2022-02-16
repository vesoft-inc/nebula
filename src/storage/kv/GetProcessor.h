/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_KV_GETPROCESSOR_H_
#define STORAGE_KV_GETPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Prom...

#include <utility>  // for move

#include "common/base/Base.h"
#include "interface/gen-cpp2/storage_types.h"  // for KVGetResponse, KVGetRe...
#include "storage/BaseProcessor.h"             // for BaseProcessor
#include "storage/CommonUtils.h"               // for StorageEnv (ptr only)

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
  explicit GetProcessor(StorageEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::KVGetResponse>(env, counters) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_KV_GETPROCESSOR_H_
