/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTHOSTSPROCESSOR_H_
#define META_LISTHOSTSPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...

#include <string>         // for string, basic_string
#include <unordered_map>  // for unordered_map
#include <utility>        // for move
#include <vector>         // for vector

#include "common/thrift/ThriftTypes.h"        // for GraphSpaceID, PartitionID
#include "common/utils/MetaKeyUtils.h"        // for LeaderParts
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"    // for ListHostsResp, HostItem
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class ListHostsProcessor : public BaseProcessor<cpp2::ListHostsResp> {
 public:
  static ListHostsProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListHostsProcessor(kvstore);
  }

  void process(const cpp2::ListHostsReq& req);

 private:
  explicit ListHostsProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListHostsResp>(kvstore) {}

  /**
   *  return online/offline, gitInfoSHA for the specific HostRole
   * */
  nebula::cpp2::ErrorCode allHostsWithStatus(cpp2::HostRole type);

  nebula::cpp2::ErrorCode fillLeaders();

  nebula::cpp2::ErrorCode fillAllParts();

  /**
   * Get gitInfoSHA from all meta hosts gitInfoSHA
   * now, assume of of them are equal
   * */
  nebula::cpp2::ErrorCode allMetaHostsStatus();

  // Get map of spaceId -> spaceName
  nebula::cpp2::ErrorCode getSpaceIdNameMap();

  std::unordered_map<std::string, std::vector<PartitionID>> getLeaderPartsWithSpaceName(
      const LeaderParts& leaderParts);

  void removeExpiredHosts(std::vector<std::string>&& removeHostsKey);

  void removeInvalidLeaders(std::vector<std::string>&& removeLeadersKey);

 private:
  std::vector<GraphSpaceID> spaceIds_;
  std::unordered_map<GraphSpaceID, std::string> spaceIdNameMap_;
  std::vector<cpp2::HostItem> hostItems_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTHOSTSPROCESSOR_H_
