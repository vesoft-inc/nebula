/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CREATESPACEPROCESSOR_H_
#define META_CREATESPACEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class CreateSpaceProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static CreateSpaceProcessor* instance(kvstore::KVStore* kvstore) {
        return new CreateSpaceProcessor(kvstore);
    }

    void process(const cpp2::CreateSpaceReq& req);

protected:
    std::vector<nebula::cpp2::HostAddr> pickHosts(
                                            PartitionID partId,
                                            const std::vector<network::InetAddress>& hosts,
                                            int32_t replicaFactor);

    int32_t GetPartsNumbers();
private:
    explicit CreateSpaceProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATESPACEPROCESSOR_H_
