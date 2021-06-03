/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_TEST_MOCKADMINCLIENT_H_
#define META_TEST_MOCKADMINCLIENT_H_

#include <gmock/gmock.h>
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

class MockAdminClient : public AdminClient {
public:
    MOCK_METHOD4(transLeader, folly::Future<Status>(GraphSpaceID, PartitionID,
                                                    const HostAddr&, const HostAddr&));
    MOCK_METHOD4(addPart, folly::Future<Status>(GraphSpaceID, PartitionID, const HostAddr&, bool));
    MOCK_METHOD3(addLearner, folly::Future<Status>(GraphSpaceID, PartitionID, const HostAddr&));
    MOCK_METHOD3(waitingForCatchUpData, folly::Future<Status>(GraphSpaceID, PartitionID,
                                                              const HostAddr&));
    MOCK_METHOD4(memberChange, folly::Future<Status>(GraphSpaceID, PartitionID,
                                                     const HostAddr&, bool));
    MOCK_METHOD4(updateMeta, folly::Future<Status>(GraphSpaceID, PartitionID,
                                                   const HostAddr&, const HostAddr&));
    MOCK_METHOD3(removePart, folly::Future<Status>(GraphSpaceID, PartitionID, const HostAddr&));
    MOCK_METHOD2(checkPeers, folly::Future<Status>(GraphSpaceID, PartitionID));
    MOCK_METHOD1(getLeaderDist, folly::Future<Status>(HostLeaderMap*));
    MOCK_METHOD3(createSnapshot,
                 folly::Future<StatusOr<cpp2::BackupInfo>>(GraphSpaceID,
                                                           const std::string&,
                                                           const HostAddr&));
    MOCK_METHOD3(dropSnapshot, folly::Future<Status>(GraphSpaceID, const std::string&,
                                                     const HostAddr&));
    MOCK_METHOD3(blockingWrites, folly::Future<Status>(GraphSpaceID, storage::cpp2::EngineSignType,
                                                       const HostAddr&));
    MOCK_METHOD9(addTask, folly::Future<Status>(cpp2::AdminCmd, int32_t, int32_t, GraphSpaceID,
                                                const std::vector<HostAddr>&,
                                                const std::vector<std::string>&,
                                                std::vector<PartitionID>,
                                                int,
                                                cpp2::StatisItem*));
    MOCK_METHOD3(stopTask, folly::Future<Status>(const std::vector<HostAddr>&, int32_t, int32_t));
};

}  // namespace meta
}  // namespace nebula

#endif  // META_TEST_MOCKADMINCLIENT_H_
