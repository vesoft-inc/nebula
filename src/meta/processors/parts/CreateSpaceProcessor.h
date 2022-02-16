/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_CREATESPACEPROCESSOR_H_
#define META_CREATESPACEPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...

#include <algorithm>      // for max
#include <cstdint>        // for int32_t
#include <string>         // for string, basic_string, hash
#include <unordered_map>  // for unordered_map
#include <utility>        // for move
#include <vector>         // for vector

#include "common/base/ErrorOr.h"              // for ErrorOr
#include "common/base/StatusOr.h"             // for StatusOr
#include "common/datatypes/HostAddr.h"        // for HostAddr, hash
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"    // for ExecResp, CreateSpaceRe...
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

using Hosts = std::vector<HostAddr>;

class CreateSpaceProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static CreateSpaceProcessor* instance(kvstore::KVStore* kvstore) {
    return new CreateSpaceProcessor(kvstore);
  }

  void process(const cpp2::CreateSpaceReq& req);

 private:
  explicit CreateSpaceProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  // Get the host with the least load in the zone
  ErrorOr<nebula::cpp2::ErrorCode, Hosts> pickHostsWithZone(
      const std::vector<std::string>& zones,
      const std::unordered_map<std::string, Hosts>& zoneHosts);

  // Get the zones with the least load
  StatusOr<std::vector<std::string>> pickLightLoadZones(int32_t replicaFactor);

 private:
  std::unordered_map<std::string, int32_t> zoneLoading_;
  std::unordered_map<HostAddr, int32_t> hostLoading_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATESPACEPROCESSOR_H_
