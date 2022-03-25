/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_TEST_MOCKADMINCLIENT_H_
#define META_TEST_MOCKADMINCLIENT_H_

#include <gmock/gmock.h>

#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

class MockAdminClient : public AdminClient {
 public:
  MOCK_METHOD4(transLeader,
               folly::Future<Status>(GraphSpaceID, PartitionID, const HostAddr&, const HostAddr&));
  MOCK_METHOD4(addPart, folly::Future<Status>(GraphSpaceID, PartitionID, const HostAddr&, bool));
  MOCK_METHOD3(addLearner, folly::Future<Status>(GraphSpaceID, PartitionID, const HostAddr&));
  MOCK_METHOD3(waitingForCatchUpData,
               folly::Future<Status>(GraphSpaceID, PartitionID, const HostAddr&));
  MOCK_METHOD4(memberChange,
               folly::Future<Status>(GraphSpaceID, PartitionID, const HostAddr&, bool));
  MOCK_METHOD4(updateMeta,
               folly::Future<Status>(GraphSpaceID, PartitionID, const HostAddr&, const HostAddr&));
  MOCK_METHOD3(removePart, folly::Future<Status>(GraphSpaceID, PartitionID, const HostAddr&));
  MOCK_METHOD2(checkPeers, folly::Future<Status>(GraphSpaceID, PartitionID));
  MOCK_METHOD1(getLeaderDist, folly::Future<Status>(HostLeaderMap*));
  MOCK_METHOD3(createSnapshot,
               folly::Future<StatusOr<cpp2::HostBackupInfo>>(const std::set<GraphSpaceID>&,
                                                             const std::string&,
                                                             const HostAddr&));
  MOCK_METHOD3(dropSnapshot,
               folly::Future<StatusOr<bool>>(const std::set<GraphSpaceID>&,
                                             const std::string&,
                                             const HostAddr&));
  MOCK_METHOD3(blockingWrites,
               folly::Future<StatusOr<bool>>(const std::set<GraphSpaceID>&,
                                             storage::cpp2::EngineSignType,
                                             const HostAddr&));
  MOCK_METHOD7(addTask,
               folly::Future<StatusOr<bool>>(cpp2::JobType,
                                             int32_t,
                                             int32_t,
                                             GraphSpaceID,
                                             const HostAddr&,
                                             const std::vector<std::string>&,
                                             std::vector<PartitionID>));
  MOCK_METHOD3(stopTask, folly::Future<StatusOr<bool>>(const HostAddr&, int32_t, int32_t));
};

}  // namespace meta
}  // namespace nebula

#endif  // META_TEST_MOCKADMINCLIENT_H_
