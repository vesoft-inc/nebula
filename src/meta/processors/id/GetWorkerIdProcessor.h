/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETWORKERIDPROCESSOR_H_
#define META_GETWORKERIDPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <algorithm>  // for max
#include <string>     // for allocator, string
#include <utility>    // for move
#include <vector>     // for vector

#include "common/base/Base.h"               // for UNUSED
#include "interface/gen-cpp2/meta_types.h"  // for GetWorkerIdResp, GetWorke...
#include "kvstore/Common.h"                 // for KV
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

/**
 * @brief Assign an incremental work id for each host in cluster, used to
 *        generate snowflake uuid
 *
 */
class GetWorkerIdProcessor : public BaseProcessor<cpp2::GetWorkerIdResp> {
 public:
  static GetWorkerIdProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetWorkerIdProcessor(kvstore);
  }

  void process(const cpp2::GetWorkerIdReq& req);

 private:
  explicit GetWorkerIdProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetWorkerIdResp>(kvstore) {
    // initialize worker id in kvstore just once
    static bool once = [this]() {
      std::vector<kvstore::KV> data = {{kIdKey, "0"}};
      doPut(data);
      return true;
    }();
    UNUSED(once);
  }

  void doPut(std::vector<kvstore::KV> data);

  inline static const string kIdKey = "snowflake_worker_id";
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETWORKERIDPROCESSOR_H_
