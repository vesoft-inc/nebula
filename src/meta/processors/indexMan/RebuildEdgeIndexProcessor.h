/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REBUILDEDGEINDEXPROCESSOR_H
#define META_REBUILDEDGEINDEXPROCESSOR_H

#include "meta/processors/indexMan/RebuildIndexProcessor.h"
#include "meta/processors/indexMan/RebuildEdgeIndexProcessor.h"

namespace nebula {
namespace meta {

class RebuildEdgeIndexProcessor : public RebuildIndexProcessor {
public:
    static RebuildEdgeIndexProcessor* instance(kvstore::KVStore* kvstore,
                                               AdminClient* adminClient) {
        return new RebuildEdgeIndexProcessor(kvstore, adminClient);
    }

    void process(const cpp2::RebuildIndexReq& req);

protected:
    folly::Future<Status> caller(const HostAddr& address,
                                 GraphSpaceID spaceId,
                                 IndexID indexID,
                                 std::vector<PartitionID> parts,
                                 bool isOffline) override;

private:
    explicit RebuildEdgeIndexProcessor(kvstore::KVStore* kvstore,
                                       AdminClient* adminClient)
            : RebuildIndexProcessor(kvstore, adminClient, 'E') {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDEDGEINDEXPROCESSOR_H
