/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include "meta/ActiveHostsMan.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

TEST(ActiveHostsManTest, EncodeDecodeHostInfoV2) {
    auto now = time::WallClock::fastNowInMilliSec();
    auto role = cpp2::HostRole::STORAGE;
    std::string strGitInfoSHA = gitInfoSha();
    {
        HostInfo hostInfo(now, role, strGitInfoSHA);
        auto encodeHostInfo = HostInfo::encodeV2(hostInfo);

        auto decodeHostInfo = HostInfo::decode(encodeHostInfo);
        ASSERT_EQ(hostInfo.lastHBTimeInMilliSec_, decodeHostInfo.lastHBTimeInMilliSec_);
        ASSERT_EQ(hostInfo.role_, decodeHostInfo.role_);
        ASSERT_EQ(hostInfo.gitInfoSha_, decodeHostInfo.gitInfoSha_);
    }
    {
        HostInfo hostInfo(now);
        auto encodeHostInfo = HostInfo::encodeV2(hostInfo);

        auto decodeHostInfo = HostInfo::decode(encodeHostInfo);
        ASSERT_EQ(hostInfo.lastHBTimeInMilliSec_, decodeHostInfo.lastHBTimeInMilliSec_);
        ASSERT_EQ(hostInfo.role_, decodeHostInfo.role_);
        ASSERT_EQ(hostInfo.gitInfoSha_, decodeHostInfo.gitInfoSha_);
    }
    {
        HostInfo hostInfo(now, role, strGitInfoSHA);
        auto encodeHostInfo = HostInfo::encodeV2(hostInfo);

        auto decodeHostInfo = HostInfo::decode(encodeHostInfo);
        ASSERT_EQ(hostInfo.lastHBTimeInMilliSec_, decodeHostInfo.lastHBTimeInMilliSec_);
    }
}

TEST(ActiveHostsManTest, NormalTest) {
    fs::TempDir rootPath("/tmp/ActiveHostsManTest.XXXXXX");
    FLAGS_heartbeat_interval_secs = 1;
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    auto now = time::WallClock::fastNowInMilliSec();
    HostInfo info1(now, cpp2::HostRole::STORAGE, gitInfoSha());
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 0), info1);
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 1), info1);
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 2), info1);
    ASSERT_EQ(3, ActiveHostsMan::getActiveHosts(kv.get()).size());

    HostInfo info2(now + 2000, cpp2::HostRole::STORAGE, gitInfoSha());
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 0), info2);
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
            ASSERT_EQ(HostAddr("0", i), HostAddr(host.host, host.port));
            if (i != 0) {
                ASSERT_EQ(info1, info);
            } else {
                ASSERT_EQ(info2, info);
            }
            iter->next();
            i++;
        }
        ASSERT_EQ(3, i);
    }

    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    ASSERT_EQ(1, ActiveHostsMan::getActiveHosts(kv.get()).size());
}

TEST(ActiveHostsManTest, LeaderTest) {
    fs::TempDir rootPath("/tmp/ActiveHostsManTest.XXXXXX");
    FLAGS_heartbeat_interval_secs = 1;
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    auto now = time::WallClock::fastNowInMilliSec();

    HostInfo hInfo1(now, cpp2::HostRole::STORAGE, gitInfoSha());
    HostInfo hInfo2(now + 2000, cpp2::HostRole::STORAGE, gitInfoSha());
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 0), hInfo1);
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 1), hInfo1);
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 2), hInfo1);
    ASSERT_EQ(3, ActiveHostsMan::getActiveHosts(kv.get()).size());

    std::unordered_map<GraphSpaceID, std::vector<PartitionID>> leaderIds;
    leaderIds.emplace(1, std::vector<PartitionID>{1, 2, 3});
    ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 0), hInfo2, &leaderIds);
    ASSERT_EQ(3, ActiveHostsMan::getActiveHosts(kv.get()).size());
    {
        const auto& prefix = MetaServiceUtils::leaderPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        CHECK_EQ(kvstore::ResultCode::SUCCEEDED, ret);
        int i = 0;
        while (iter->valid()) {
            auto host = MetaServiceUtils::parseLeaderKey(iter->key());
            LeaderParts leaderParts = MetaServiceUtils::parseLeaderVal(iter->val());
            ASSERT_EQ(HostAddr("0", i), HostAddr(host.host, host.port));
            if (i == 0) {
                ASSERT_EQ(1, leaderParts.size());
                ASSERT_TRUE(leaderParts.find(1) != leaderParts.end());
                ASSERT_EQ(3, leaderParts[1].size());
                ASSERT_EQ(1, leaderParts[1][0]);
                ASSERT_EQ(2, leaderParts[1][1]);
                ASSERT_EQ(3, leaderParts[1][2]);
            } else {
                ASSERT_EQ(0, leaderParts.size());
            }

            iter->next();
            i++;
        }
        ASSERT_EQ(1, i);
    }

    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    ASSERT_EQ(1, ActiveHostsMan::getActiveHosts(kv.get()).size());
}

TEST(LastUpdateTimeManTest, NormalTest) {
    fs::TempDir rootPath("/tmp/LastUpdateTimeManTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

    ASSERT_EQ(0, LastUpdateTimeMan::get(kv.get()));
    int64_t now = time::WallClock::fastNowInMilliSec();

    LastUpdateTimeMan::update(kv.get(), now);
    ASSERT_EQ(now, LastUpdateTimeMan::get(kv.get()));
    LastUpdateTimeMan::update(kv.get(), now + 100);
    ASSERT_EQ(now + 100, LastUpdateTimeMan::get(kv.get()));

    LastUpdateTimeMan::update(kv.get(), now - 100);
    {
        auto key = MetaServiceUtils::lastUpdateTimeKey();
        std::string val;
        auto ret = kv->get(kDefaultSpaceId, kDefaultPartId, key, &val);
        ASSERT_EQ(ret, kvstore::ResultCode::SUCCEEDED);
        int64_t lastUpdateTime = *reinterpret_cast<const int64_t*>(val.data());
        ASSERT_EQ(now - 100, lastUpdateTime);
    }
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
