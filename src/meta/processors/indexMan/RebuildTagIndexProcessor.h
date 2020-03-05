/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REBUILDTAGINDEXPROCESSOR_H
#define META_REBUILDTAGINDEXPROCESSOR_H

#include "meta/processors/indexMan/RebuildIndexProcessor.h"
#include "meta/processors/indexMan/RebuildTagIndexProcessor.h"

namespace nebula {
namespace meta {

class RebuildTagIndexProcessor : public RebuildIndexProcessor {
public:
    static RebuildTagIndexProcessor* instance(kvstore::KVStore* kvstore,
                                              AdminClient* adminClient) {
        return new RebuildTagIndexProcessor(kvstore, adminClient);
    }

    void process(const cpp2::RebuildIndexReq& req);

protected:
    folly::Future<Status> caller(const HostAddr& address,
                                 GraphSpaceID spaceId,
                                 IndexID indexID,
                                 std::vector<PartitionID> parts,
                                 bool isOffline) override;
private:
    explicit RebuildTagIndexProcessor(kvstore::KVStore* kvstore,
                                      AdminClient* adminClient)
            : RebuildIndexProcessor(kvstore, adminClient, 'T') {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDTAGINDEXPROCESSOR_H
