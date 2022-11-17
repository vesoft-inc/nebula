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
  MOCK_METHOD(folly::Future<Status>,
              transLeader,
              (GraphSpaceID, PartitionID, const HostAddr&, const HostAddr&),
              (override));
  MOCK_METHOD(folly::Future<Status>,
              addPart,
              (GraphSpaceID, PartitionID, const HostAddr&, bool),
              (override));
  MOCK_METHOD(folly::Future<Status>,
              addLearner,
              (GraphSpaceID, PartitionID, const HostAddr&),
              (override));
  MOCK_METHOD(folly::Future<Status>,
              waitingForCatchUpData,
              (GraphSpaceID, PartitionID, const HostAddr&),
              (override));
  MOCK_METHOD(folly::Future<Status>,
              memberChange,
              (GraphSpaceID, PartitionID, const HostAddr&, bool),
              (override));
  MOCK_METHOD(folly::Future<Status>,
              updateMeta,
              (GraphSpaceID, PartitionID, const HostAddr&, const HostAddr&),
              (override));
  MOCK_METHOD(folly::Future<Status>,
              removePart,
              (GraphSpaceID, PartitionID, const HostAddr&),
              (override));
  MOCK_METHOD(folly::Future<Status>, checkPeers, (GraphSpaceID, PartitionID), (override));
  MOCK_METHOD(folly::Future<Status>, getLeaderDist, (HostLeaderMap*), (override));
  MOCK_METHOD(folly::Future<StatusOr<cpp2::HostBackupInfo>>,
              createSnapshot,
              (const std::set<GraphSpaceID>&, const std::string&, const HostAddr&),
              (override));
  MOCK_METHOD(folly::Future<StatusOr<bool>>,
              dropSnapshot,
              (const std::set<GraphSpaceID>&, const std::string&, const HostAddr&),
              (override));
  MOCK_METHOD(folly::Future<StatusOr<bool>>,
              blockingWrites,
              (const std::set<GraphSpaceID>&, storage::cpp2::EngineSignType, const HostAddr&),
              (override));
  MOCK_METHOD(folly::Future<StatusOr<bool>>,
              addTask,
              (cpp2::JobType,
               int32_t,
               int32_t,
               GraphSpaceID,
               const HostAddr&,
               const std::vector<std::string>&,
               std::vector<PartitionID>),
              (override));
  MOCK_METHOD(folly::Future<StatusOr<bool>>,
              stopTask,
              (const HostAddr&, int32_t, int32_t),
              (override));
};

}  // namespace meta
}  // namespace nebula

#endif  // META_TEST_MOCKADMINCLIENT_H_
