/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_PROCESSORS_ADMIN_STORAGEADMINCLIENT_H_
#define META_PROCESSORS_ADMIN_STORAGEADMINCLIENT_H_

#include "base/Base.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "base/Status.h"
#include "thrift/ThriftClientManager.h"

namespace nebula {
namespace meta {

class FaultInjector {
public:
    virtual ~FaultInjector() = default;
    virtual folly::Future<Status> transLeader() = 0;
    virtual folly::Future<Status> addPart() = 0;
    virtual folly::Future<Status> addLearner() = 0;
    virtual folly::Future<Status> waitingForCatchUpData() = 0;
    virtual folly::Future<Status> memberChange() = 0;
    virtual folly::Future<Status> updateMeta() = 0;
    virtual folly::Future<Status> removePart() = 0;
};

class AdminClient {
public:
    AdminClient() = default;

    explicit AdminClient(std::unique_ptr<FaultInjector> injector)
        : injector_(std::move(injector)) {}

    ~AdminClient() = default;

    folly::Future<Status> transLeader(GraphSpaceID spaceId,
                                      PartitionID partId,
                                      const HostAddr& leader,
                                      const HostAddr& dst);

    folly::Future<Status> addPart(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  const HostAddr& host,
                                  bool asLearner);

    folly::Future<Status> addLearner(GraphSpaceID spaceId, PartitionID partId);

    folly::Future<Status> waitingForCatchUpData(GraphSpaceID spaceId, PartitionID partId);

    folly::Future<Status> memberChange(GraphSpaceID spaceId, PartitionID partId);

    folly::Future<Status> updateMeta(GraphSpaceID spaceId,
                                     PartitionID partId,
                                     const HostAddr& leader,
                                     const HostAddr& dst);

    folly::Future<Status> removePart(GraphSpaceID spaceId,
                                     PartitionID partId,
                                     const HostAddr& host);

    FaultInjector* faultInjector() {
        return injector_.get();
    }

private:
    std::unique_ptr<FaultInjector> injector_{nullptr};
};
}  // namespace meta
}  // namespace nebula

#endif  // META_PROCESSORS_ADMIN_STORAGEADMINCLIENT_H_
