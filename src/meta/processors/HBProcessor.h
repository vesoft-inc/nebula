/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_HBPROCESSOR_H_
#define META_HBPROCESSOR_H_

#include <gtest/gtest_prod.h>
#include "meta/processors/BaseProcessor.h"
#include "meta/ActiveHostsMan.h"
#include "time/TimeUtils.h"

DECLARE_int32(expired_hosts_check_interval_sec);
DECLARE_int32(expired_threshold_sec);

namespace nebula {
namespace meta {

class HBProcessor : public BaseProcessor<cpp2::HBResp> {
    FRIEND_TEST(HBProcessorTest, HBTest);
    FRIEND_TEST(MetaClientTest, HeartbeatTest);

public:
    static HBProcessor* instance(kvstore::KVStore* kvstore) {
        return new HBProcessor(kvstore);
    }

    void process(const cpp2::HBReq& req) {
        HostAddr host(req.host.ip, req.host.port);
        if (hostExist(MetaServiceUtils::hostKey(host.first, host.second))
                    == Status::HostNotFound()) {
            LOG(INFO) << "Reject unregistered host " << host << "!";
            resp_.set_code(cpp2::ErrorCode::E_INVALID_HOST);
            onFinished();
            return;
        }

        LOG(INFO) << "Receive heartbeat from " << host;
        HostInfo info;
        info.lastHBTimeInSec_ = time::TimeUtils::nowInSeconds();
        hostsMan()->updateHostInfo(host, info);
        onFinished();
    }

private:
    explicit HBProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::HBResp>(kvstore) {}

    static ActiveHostsMan* hostsMan() {
        static auto hostsMan
            = std::make_unique<ActiveHostsMan>(FLAGS_expired_hosts_check_interval_sec,
                                               FLAGS_expired_threshold_sec);
        return hostsMan.get();
    }
};

}  // namespace meta
}  // namespace nebula

#endif  // META_HBPROCESSOR_H_
