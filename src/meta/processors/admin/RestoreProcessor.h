/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_RESTOREPROCESSOR_H_
#define META_RESTOREPROCESSOR_H_

#include <gtest/gtest_prod.h>
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class RestoreProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static RestoreProcessor* instance(kvstore::KVStore* kvstore) {
        return new RestoreProcessor(kvstore);
    }
    void process(const cpp2::RestoreMetaReq& req);

private:
    explicit RestoreProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    // A direct value of true means that data will not be written to follow via the raft protocol,
    // but will be written directly to local disk
    nebula::cpp2::ErrorCode
    replaceHostInPartition(const HostAddr& ipv4From,
                           const HostAddr& ipv4To,
                           bool direct = false);

    nebula::cpp2::ErrorCode
    replaceHostInZone(const HostAddr& ipv4From,
                      const HostAddr& ipv4To,
                      bool direct = false);
};

}   // namespace meta
}   // namespace nebula

#endif   // META_RESTOREPROCESSOR_H_
