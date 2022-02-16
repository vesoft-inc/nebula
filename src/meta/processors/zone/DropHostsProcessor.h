/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPHOSTSPROCESSOR_H
#define META_DROPHOSTSPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...

#include <string>   // for string
#include <utility>  // for move

#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"    // for ExecResp, DropHostsReq ...
#include "kvstore/LogEncoder.h"
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class BatchHolder;
class KVStore;

class BatchHolder;
class KVStore;
}  // namespace kvstore

namespace meta {

/**
 * @brief Drop hosts from this cluster
 * The hosts should not hold any parts
 * It will remove the hosts from zones they belong to,
 * and detach the machine from cluster
 */
class DropHostsProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropHostsProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropHostsProcessor(kvstore);
  }

  void process(const cpp2::DropHostsReq& req);

 private:
  explicit DropHostsProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  /**
   * @brief check all spaces to find the zone, and remove it from the space
   *
   * @param zoneName
   * @param holder
   * @return
   */
  nebula::cpp2::ErrorCode checkRelatedSpaceAndCollect(const std::string& zoneName,
                                                      kvstore::BatchHolder* holder);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPHOSTSPROCESSOR_H
