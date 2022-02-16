/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_ADMINJOBPROCESSOR_H_
#define META_ADMINJOBPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "common/stats/StatsManager.h"
#include "interface/gen-cpp2/meta_types.h"  // for AdminJobResp, AdminJobReq...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {
class AdminClient;
}  // namespace meta

namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {
class AdminClient;

/**
 * @brief Make admin job operation, including ADD SHOW_All SHOW STOP RECOVER
 */
class AdminJobProcessor : public BaseProcessor<cpp2::AdminJobResp> {
 public:
  static AdminJobProcessor* instance(kvstore::KVStore* kvstore, AdminClient* adminClient) {
    return new AdminJobProcessor(kvstore, adminClient);
  }

  void process(const cpp2::AdminJobReq& req);

 protected:
  AdminJobProcessor(kvstore::KVStore* kvstore, AdminClient* adminClient)
      : BaseProcessor<cpp2::AdminJobResp>(kvstore), adminClient_(adminClient) {}

 protected:
  AdminClient* adminClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMINJOBPROCESSOR_H_
