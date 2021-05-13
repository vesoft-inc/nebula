/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/admin/RestoreProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

TEST(RestoreProcessorTest, RestoreTest) {
    fs::TempDir rootPath("/tmp/RestoreOriginTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    auto now = time::WallClock::fastNowInMilliSec();

    HostAddr host1("127.0.0.1", 3360);
    HostAddr host2("127.0.0.2", 3360);
    HostAddr host3("127.0.0.3", 3360);

    std::vector<HostAddr> hosts;
    hosts.emplace_back(host1);
    hosts.emplace_back(host2);
    hosts.emplace_back(host3);

    for (auto h : hosts) {
        ActiveHostsMan::updateHostInfo(
            kv.get(), h, HostInfo(now, meta::cpp2::HostRole::STORAGE, ""));
    }

    meta::TestUtils::registerHB(kv.get(), hosts);

    // mock admin client
    bool ret = false;
    cpp2::SpaceDesc properties;
    GraphSpaceID id = 1;
    std::string groupName = "test_group";
    properties.set_space_name("test_space");
    int partNum = 10;
    properties.set_partition_num(partNum);
    properties.set_replica_factor(3);
    properties.set_group_name(groupName);

    auto spaceVal = MetaServiceUtils::spaceVal(properties);
    std::vector<nebula::kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexSpaceKey("test_space"),
                      std::string(reinterpret_cast<const char*>(&id), sizeof(GraphSpaceID)));
    data.emplace_back(MetaServiceUtils::spaceKey(id), MetaServiceUtils::spaceVal(properties));

    std::unordered_map<HostAddr, std::vector<size_t>> partInfo;

    for (auto partId = 1; partId <= partNum; partId++) {
        std::vector<HostAddr> hosts4;
        size_t idx = partId;
        for (int32_t i = 0; i < 3; i++, idx++) {
            auto h = hosts[idx % 3];
            hosts4.emplace_back(h);
            partInfo[h].emplace_back(partId);
        }
        data.emplace_back(MetaServiceUtils::partKey(id, partId), MetaServiceUtils::partVal(hosts4));
    }

    auto groupId = 1;
    std::string zoneName = "test_zone";
    std::vector<std::string> zoneNames = {zoneName};
    data.emplace_back(MetaServiceUtils::indexGroupKey(groupName),
                      std::string(reinterpret_cast<const char*>(&groupId), sizeof(GroupID)));
    data.emplace_back(MetaServiceUtils::groupKey(groupName), MetaServiceUtils::groupVal(zoneNames));

    auto zoneId = 1;
    data.emplace_back(MetaServiceUtils::indexZoneKey(zoneName),
                      std::string(reinterpret_cast<const char*>(&zoneId), sizeof(ZoneID)));
    data.emplace_back(MetaServiceUtils::zoneKey(zoneName), MetaServiceUtils::zoneVal(hosts));

    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(0, 0, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        ret = (code == nebula::cpp2::ErrorCode::SUCCEEDED);
        baton.post();
    });
    baton.wait();

    std::unordered_set<GraphSpaceID> spaces = {id};
    auto backupName = folly::format("BACKUP_{}", MetaServiceUtils::genTimestampStr()).str();
    auto spaceNames = std::make_unique<std::vector<std::string>>();
    spaceNames->emplace_back("test_space");
    auto backupFiles = MetaServiceUtils::backup(kv.get(), spaces, backupName, spaceNames.get());
    DCHECK(backupFiles.hasValue());
    {
        cpp2::RestoreMetaReq req;
        std::vector<std::string> files = backupFiles.value();
        req.set_files(std::move(files));
        std::vector<cpp2::HostPair> hostPairs;
        HostAddr host4("127.0.0.4", 3360);
        HostAddr host5("127.0.0.5", 3360);
        HostAddr host6("127.0.0.6", 3360);
        std::unordered_map<HostAddr, HostAddr> hostMap = {
            {host1, host4}, {host2, host5}, {host3, host6}};

        for (auto hm : hostMap) {
            hostPairs.emplace_back(apache::thrift::FragileConstructor(), hm.first, hm.second);
        }

        req.set_hosts(std::move(hostPairs));

        auto* processor = RestoreProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        nebula::cpp2::ErrorCode result;
        std::unique_ptr<kvstore::KVIterator> iter;

        const auto& partPrefix = MetaServiceUtils::partPrefix(id);
        result = kv->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);

        std::unordered_map<HostAddr, std::vector<size_t>> toPartInfo;

        while (iter->valid()) {
            auto key = iter->key();
            auto partId = MetaServiceUtils::parsePartKeyPartId(key);
            auto partHosts = MetaServiceUtils::parsePartVal(iter->val());
            for (auto& host : partHosts) {
                LOG(INFO) << "partHost: " << host.toString();
                toPartInfo[host].emplace_back(partId);
            }
            iter->next();
        }

        for (auto pi : partInfo) {
            auto parts = toPartInfo[hostMap[pi.first]];
            ASSERT_EQ(parts.size(), pi.second.size());
            ASSERT_TRUE(std::equal(parts.cbegin(), parts.cend(), pi.second.cbegin()));
        }

        auto prefix = MetaServiceUtils::zonePrefix();
        result = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);

        std::vector<cpp2::Zone> zones;
        std::vector<HostAddr> restoredHosts = {host4, host5, host6};
        while (iter->valid()) {
            auto zn = MetaServiceUtils::parseZoneName(iter->key());
            auto zoneHosts = MetaServiceUtils::parseZoneHosts(iter->val());
            cpp2::Zone zone;
            ASSERT_EQ(zoneName, zn);
            ASSERT_EQ(zoneHosts.size(), restoredHosts.size());
            for (std::vector<nebula::HostAddr>::size_type i = 0; i < zoneHosts.size(); ++i) {
                ASSERT_EQ(zoneHosts[i], restoredHosts[i]);
            }
            iter->next();
        }
    }
}

}   // namespace meta
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
