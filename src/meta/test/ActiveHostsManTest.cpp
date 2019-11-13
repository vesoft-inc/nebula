/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include "meta/ActiveHostsMan.h"
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(expired_threshold_sec);

namespace nebula {
namespace meta {

TEST(ActiveHostsManTest, NormalTest) {
    fs::TempDir rootPath("/tmp/ActiveHostsManTest.XXXXXX");
    FLAGS_expired_threshold_sec = 2;
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto now = time::WallClock::fastNowInMilliSec();
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr(0, 0), HostInfo(now));
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr(0, 1), HostInfo(now));
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr(0, 2), HostInfo(now));
    ASSERT_EQ(3, ActiveHostsMan::getActiveHosts(kv.get()).size());

    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr(0, 0), HostInfo(now + 2000));
    ASSERT_EQ(3, ActiveHostsMan::getActiveHosts(kv.get()).size());
    {
        const auto& prefix = MetaServiceUtils::hostPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        CHECK_EQ(kvstore::ResultCode::SUCCEEDED, ret);
        int i = 0;
        while (iter->valid()) {
            auto host = MetaServiceUtils::parseHostKey(iter->key());
            HostInfo info = HostInfo::decode(iter->val());
            ASSERT_EQ(HostAddr(0, i), HostAddr(host.ip, host.port));
            if (i == 0) {
                ASSERT_EQ(HostInfo(now + 2000), info);
            } else {
                ASSERT_EQ(HostInfo(now), info);
            }
            iter->next();
            i++;
        }
        ASSERT_EQ(3, i);
    }

    sleep(3);
    ASSERT_EQ(1, ActiveHostsMan::getActiveHosts(kv.get()).size());
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


