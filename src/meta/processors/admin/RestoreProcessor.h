/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_RESTOREPROCESSOR_H_
#define META_RESTOREPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...
#include <gtest/gtest_prod.h>

#include <utility>  // for move

#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"    // for ExecResp, RestoreMetaRe...
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor

namespace nebula {
struct HostAddr;

namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore
struct HostAddr;

namespace meta {

/**
 * @brief Rebuild the host relative info after ingesting the table backup data to
 *        the new cluster metad KV store.
 *
 */
class RestoreProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static RestoreProcessor* instance(kvstore::KVStore* kvstore) {
    return new RestoreProcessor(kvstore);
  }
  void process(const cpp2::RestoreMetaReq& req);

 private:
  explicit RestoreProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  // A direct value of true means that data will not be written to follow via
  // the raft protocol, but will be written directly to local disk
  nebula::cpp2::ErrorCode replaceHostInPartition(const HostAddr& ipv4From,
                                                 const HostAddr& ipv4To,
                                                 bool direct = false);

  nebula::cpp2::ErrorCode replaceHostInZone(const HostAddr& ipv4From,
                                            const HostAddr& ipv4To,
                                            bool direct = false);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_RESTOREPROCESSOR_H_
