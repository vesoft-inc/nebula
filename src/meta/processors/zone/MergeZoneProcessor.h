/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_MERGEZONEPROCESSOR_H
#define META_MERGEZONEPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...

#include <unordered_map>  // for unordered_map
#include <utility>        // for move
#include <vector>         // for vector

#include "common/base/ErrorOr.h"              // for ErrorOr
#include "common/datatypes/HostAddr.h"        // for HostAddr, hash
#include "common/thrift/ThriftTypes.h"        // for PartitionID, GraphSpaceID
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"    // for ExecResp, MergeZoneReq ...
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

using HostParts = std::unordered_map<HostAddr, std::vector<PartitionID>>;

/**
 * @brief Merge several zones to one zone
 */
class MergeZoneProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static MergeZoneProcessor* instance(kvstore::KVStore* kvstore) {
    return new MergeZoneProcessor(kvstore);
  }

  void process(const cpp2::MergeZoneReq& req);

 private:
  explicit MergeZoneProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  /**
   * @brief Get all parts of a space in each host
   *
   * @param spaceId Which space's parts to get
   * @return ErrorCode or parts of each host
   */
  ErrorOr<nebula::cpp2::ErrorCode, HostParts> assembleHostParts(GraphSpaceID spaceId);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_MERGEZONEPROCESSOR_H
