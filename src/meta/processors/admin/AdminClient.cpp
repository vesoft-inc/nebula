/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

folly::Future<Status> AdminClient::transLeader(GraphSpaceID spaceId,
                                                      PartitionID partId,
                                                      const HostAddr& leader,
                                                      const HostAddr& dst) {
    UNUSED(spaceId);
    UNUSED(partId);
    UNUSED(leader);
    UNUSED(dst);
    if (injector_) {
        return injector_->transLeader();
    }
    return Status::OK();
}

folly::Future<Status> AdminClient::addPart(GraphSpaceID spaceId,
                                                  PartitionID partId,
                                                  const HostAddr& host,
                                                  bool asLearner) {
    UNUSED(spaceId);
    UNUSED(partId);
    UNUSED(host);
    UNUSED(asLearner);
    if (injector_) {
        return injector_->addPart();
    }
    return Status::OK();
}

folly::Future<Status> AdminClient::addLearner(GraphSpaceID spaceId, PartitionID partId) {
    UNUSED(spaceId);
    UNUSED(partId);
    if (injector_) {
        return injector_->addLearner();
    }
    return Status::OK();
}

folly::Future<Status> AdminClient::waitingForCatchUpData(GraphSpaceID spaceId,
                                                                PartitionID partId) {
    UNUSED(spaceId);
    UNUSED(partId);
    if (injector_) {
        return injector_->waitingForCatchUpData();
    }
    return Status::OK();
}

folly::Future<Status> AdminClient::memberChange(GraphSpaceID spaceId, PartitionID partId) {
    UNUSED(spaceId);
    UNUSED(partId);
    if (injector_) {
        return injector_->memberChange();
    }
    return Status::OK();
}

folly::Future<Status> AdminClient::updateMeta(GraphSpaceID spaceId,
                                              PartitionID partId,
                                              const HostAddr& leader,
                                              const HostAddr& dst) {
    UNUSED(spaceId);
    UNUSED(partId);
    UNUSED(leader);
    UNUSED(dst);
    if (injector_) {
        return injector_->updateMeta();
    }
    return Status::OK();
}

folly::Future<Status> AdminClient::removePart(GraphSpaceID spaceId,
                                                     PartitionID partId,
                                                     const HostAddr& host) {
    UNUSED(spaceId);
    UNUSED(partId);
    UNUSED(host);
    if (injector_) {
        return injector_->removePart();
    }
    return Status::OK();
}

}  // namespace meta
}  // namespace nebula

