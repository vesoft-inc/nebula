/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETWORKERIDPROCESSOR_H_
#define META_GETWORKERIDPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
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
      auto code = doSyncPut(data);
      handleErrorCode(code);
      return true;
    }();
    UNUSED(once);
  }

  inline static const std::string kIdKey = "snowflake_worker_id";
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETWORKERIDPROCESSOR_H_
