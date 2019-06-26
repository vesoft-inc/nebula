/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include "meta/ActiveHostsMan.h"
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

TEST(ActiveHostsManTest, NormalTest) {
    fs::TempDir rootPath("/tmp/ActiveHostsManTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto now = time::TimeUtils::nowInSeconds();
    ActiveHostsMan ahMan(1, 1);
    ahMan.updateHostInfo(HostAddr(0, 0), HostInfo(now));
    ahMan.updateHostInfo(HostAddr(0, 1), HostInfo(now));
    ahMan.updateHostInfo(HostAddr(0, 2), HostInfo(now));
    ASSERT_EQ(3, ahMan.getActiveHosts().size());

    ahMan.updateHostInfo(HostAddr(0, 0), HostInfo(now + 2));
    ASSERT_EQ(3, ahMan.getActiveHosts().size());
    {
        folly::RWSpinLock::ReadHolder rh(&ahMan.lock_);
        ASSERT_EQ(HostInfo(now + 2),  ahMan.hostsMap_[HostAddr(0, 0)]);
    }

    sleep(3);
    ASSERT_EQ(1, ahMan.getActiveHosts().size());
    {
        folly::RWSpinLock::ReadHolder rh(&ahMan.lock_);
        ASSERT_EQ(HostInfo(now + 2),  ahMan.hostsMap_[HostAddr(0, 0)]);
    }
}

TEST(ActiveHostsManTest, NormalTest2) {
    fs::TempDir rootPath("/tmp/ActiveHostsMergeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto now = time::TimeUtils::nowInSeconds();
    ActiveHostsMan ahMan(1, 2, kv.get());
    ahMan.updateHostInfo(HostAddr(0, 0), HostInfo(now));
    ahMan.updateHostInfo(HostAddr(0, 1), HostInfo(now));
    ahMan.updateHostInfo(HostAddr(0, 2), HostInfo(now));
    ASSERT_EQ(3, ahMan.getActiveHosts().size());
    usleep(2200);
    ASSERT_EQ(3, ahMan.getActiveHosts().size());
    {
        std::vector<kvstore::KV> data;
        for (auto i = 0; i < 3; i++) {
            data.emplace_back(MetaServiceUtils::hostKey(0, i),
                MetaServiceUtils::hostValKVStoreOnline());
        }
        kv->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
            [] (kvstore::ResultCode code) {
            CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
        });
    }
    usleep(200);
    ASSERT_EQ(3, ahMan.getActiveHosts().size());
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


