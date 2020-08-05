/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REBUILDINDEXPROCESSOR_H
#define META_REBUILDINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class RebuildIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
protected:
    void processInternal(const cpp2::RebuildIndexReq& req);

    virtual folly::Future<Status> caller(const HostAddr& address,
                                         GraphSpaceID spaceId,
                                         IndexID indexID,
                                         std::vector<PartitionID> parts,
                                         bool isOffline) = 0;

    void handleRebuildIndexResult(std::vector<folly::Future<Status>> results,
                                  kvstore::KVStore* kvstore,
                                  std::string statusKey);

    explicit RebuildIndexProcessor(kvstore::KVStore* kvstore,
                                   AdminClient* adminClient,
                                   char category)
            : BaseProcessor<cpp2::ExecResp>(kvstore)
            , adminClient_(adminClient)
            , category_(category) {}

protected:
    AdminClient* adminClient_;
    char category_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDINDEXPROCESSOR_H
