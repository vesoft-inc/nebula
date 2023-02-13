/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/admin/RestoreProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

// backup given space
TEST(RestoreProcessorTest, RestoreTest) {
  fs::TempDir rootPath("/tmp/RestoreOriginTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  auto now = time::WallClock::fastNowInMilliSec();

  // initial three storaged
  HostAddr host1("127.0.0.1", 3360);
  HostAddr host2("127.0.0.2", 3360);
  HostAddr host3("127.0.0.3", 3360);
  std::vector<HostAddr> hosts{host1, host2, host3};

  // register them
  std::vector<kvstore::KV> times;
  for (auto h : hosts) {
    ActiveHostsMan::updateHostInfo(
        kv.get(), h, HostInfo(now, meta::cpp2::HostRole::STORAGE, ""), times);
  }
  TestUtils::doPut(kv.get(), times);
  TestUtils::registerHB(kv.get(), hosts);

  // mock two spaces
  cpp2::SpaceDesc properties;
  GraphSpaceID id = 1;
  properties.space_name_ref() = "test_space";
  int partNum = 10;
  properties.partition_num_ref() = partNum;
  properties.replica_factor_ref() = 3;

  cpp2::SpaceDesc properties2;
  GraphSpaceID id2 = 2;
  properties2.space_name_ref() = "test_space2";
  properties2.partition_num_ref() = partNum;
  properties2.replica_factor_ref() = 3;

  auto spaceVal = MetaKeyUtils::spaceVal(properties);
  std::vector<nebula::kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::indexSpaceKey("test_space"),
                    std::string(reinterpret_cast<const char*>(&id), sizeof(GraphSpaceID)));
  data.emplace_back(MetaKeyUtils::spaceKey(id), MetaKeyUtils::spaceVal(properties));

  data.emplace_back(MetaKeyUtils::indexSpaceKey("test_space2"),
                    std::string(reinterpret_cast<const char*>(&id2), sizeof(GraphSpaceID)));
  data.emplace_back(MetaKeyUtils::spaceKey(id2), MetaKeyUtils::spaceVal(properties2));

  // mock part distribution for "test_space"
  std::unordered_map<HostAddr, std::vector<size_t>> fromHostParts;
  for (auto partId = 1; partId <= partNum; partId++) {
    std::vector<HostAddr> partHosts;
    size_t idx = partId;
    for (int32_t i = 0; i < 3; i++, idx++) {
      auto h = hosts[idx % 3];
      partHosts.emplace_back(h);
      fromHostParts[h].emplace_back(partId);
    }
    data.emplace_back(MetaKeyUtils::partKey(id, partId), MetaKeyUtils::partVal(partHosts));
  }

  // mock an root user
  data.emplace_back(MetaKeyUtils::userKey("root"), MetaKeyUtils::userVal("root"));

  // mock three zones, each zone each host
  for (auto host : hosts) {
    std::string zoneName = folly::stringPrintf("default_zone_%s_%d", host.host.c_str(), host.port);
    data.emplace_back(MetaKeyUtils::zoneKey(zoneName), MetaKeyUtils::zoneVal({host}));
  }

  int32_t autoId = 666;
  data.emplace_back(MetaKeyUtils::idKey(),
                    std::string(reinterpret_cast<const char*>(&autoId), sizeof(autoId)));

  auto lastUpdateTime = time::WallClock::fastNowInMilliSec();
  data.emplace_back(MetaKeyUtils::lastUpdateTimeKey(),
                    MetaKeyUtils::lastUpdateTimeVal(lastUpdateTime));

  TestUtils::doPut(kv.get(), data);

  // mock an meta backup files, only backup specified space instead of full space
  std::unordered_set<GraphSpaceID> spaces = {id};
  auto backupName = folly::sformat("BACKUP_{}", MetaKeyUtils::genTimestampStr());
  auto spaceNames = std::make_unique<std::vector<std::string>>();
  spaceNames->emplace_back("test_space");
  auto backupFiles = MetaServiceUtils::backupTables(kv.get(), spaces, backupName, spaceNames.get());
  DCHECK(nebula::hasValue(backupFiles));
  {
    cpp2::RestoreMetaReq req;
    std::vector<std::string> files = nebula::value(backupFiles);
    auto it = std::find_if(files.cbegin(), files.cend(), [](auto& f) {
      auto const pos = f.find_last_of("/");
      auto name = f.substr(pos + 1);
      if (name == MetaKeyUtils::zonePrefix() + ".sst") {
        return true;
      }
      return false;
    });
    ASSERT_EQ(it, files.cend());  // should not include system tables
    req.files_ref() = std::move(files);

    // mock an new cluster {host4, host5. host6}, and construct restore pairs
    std::vector<cpp2::HostPair> hostPairs;
    HostAddr host4("127.0.0.4", 3360);
    HostAddr host5("127.0.0.5", 3360);
    HostAddr host6("127.0.0.6", 3360);
    std::unordered_map<HostAddr, HostAddr> hostMap = {
        {host1, host4}, {host2, host5}, {host3, host6}};
    for (auto [from, to] : hostMap) {
      hostPairs.emplace_back(apache::thrift::FragileConstructor(), from, to);
    }
    req.hosts_ref() = std::move(hostPairs);

    // mock an new cluster to restore
    fs::TempDir restoreTootPath("/tmp/RestoreTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kvRestore(MockCluster::initMetaKV(restoreTootPath.path()));
    std::vector<nebula::kvstore::KV> restoreData;
    restoreData.emplace_back(MetaKeyUtils::userKey("root"), MetaKeyUtils::userVal("password"));
    TestUtils::doPut(kvRestore.get(), restoreData);

    // call the restore processor
    auto* processor = RestoreProcessor::instance(kvRestore.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    // verify host->parts info
    const auto& partPrefix = MetaKeyUtils::partPrefix(id);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);

    std::unordered_map<HostAddr, std::vector<size_t>> toHostParts;
    while (iter->valid()) {
      auto key = iter->key();
      auto partId = MetaKeyUtils::parsePartKeyPartId(key);
      auto partHosts = MetaKeyUtils::parsePartVal(iter->val());
      for (auto& host : partHosts) {
        LOG(INFO) << "partHost: " << host.toString();
        toHostParts[host].emplace_back(partId);
      }
      iter->next();
    }
    for (auto pi : fromHostParts) {
      auto parts = toHostParts[hostMap[pi.first]];
      ASSERT_EQ(parts.size(), pi.second.size());
      ASSERT_TRUE(std::equal(parts.cbegin(), parts.cend(), pi.second.cbegin()));
    }

    // verify zone info, zone info has not been replaced with ingesting
    auto prefix = MetaKeyUtils::zonePrefix();
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    std::vector<cpp2::Zone> zones;
    std::vector<HostAddr> restoredHosts = {host4, host5, host6};

    std::unordered_map<std::string, HostAddr> restoreZoneNames;
    // mock three zones, each zone each host
    for (auto host : restoredHosts) {
      std::string zoneName =
          folly::stringPrintf("default_zone_%s_%d", host.host.c_str(), host.port);
      restoreZoneNames.emplace(zoneName, host);
    }
    while (iter->valid()) {
      auto zn = MetaKeyUtils::parseZoneName(iter->key());
      auto zoneHosts = MetaKeyUtils::parseZoneHosts(iter->val());
      ASSERT_TRUE(restoreZoneNames.find(zn) != restoreZoneNames.end());
      ASSERT_EQ(1, zoneHosts.size());
      ASSERT_EQ(restoreZoneNames[zn], zoneHosts[0]);
      iter->next();
    }

    prefix = MetaKeyUtils::userPrefix();
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    while (iter->valid()) {
      auto user = MetaKeyUtils::parseUser(iter->key());
      auto password = MetaKeyUtils::parseUserPwd(iter->val());
      ASSERT_EQ(user, std::string("root"));
      ASSERT_EQ(password, std::string("password"));
      iter->next();
    }

    prefix = MetaKeyUtils::indexPrefix(id);
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    while (iter->valid()) {
      auto sid = MetaKeyUtils::parseIndexesKeySpaceID(iter->key());
      ASSERT_EQ(sid, id);
      iter->next();
    }

    prefix = MetaKeyUtils::indexPrefix(id2);
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    ASSERT_FALSE(iter->valid());

    std::string key = "__id__";
    std::string value;
    result = kvRestore->get(kDefaultSpaceId, kDefaultPartId, key, &value);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    ASSERT_EQ(autoId, *reinterpret_cast<const int32_t*>(value.data()));

    key = "__last_update_time__";
    result = kvRestore->get(kDefaultSpaceId, kDefaultPartId, key, &value);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    ASSERT_EQ(lastUpdateTime, *reinterpret_cast<const int64_t*>(value.data()));
  }
}

// backup full space
TEST(RestoreProcessorTest, RestoreFullTest) {
  fs::TempDir rootPath("/tmp/RestoreFullOriginTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  auto now = time::WallClock::fastNowInMilliSec();

  // mock three storaged and register them
  HostAddr host1("127.0.0.1", 3360);
  HostAddr host2("127.0.0.2", 3360);
  HostAddr host3("127.0.0.3", 3360);
  std::vector<HostAddr> hosts{host1, host2, host3};
  std::vector<kvstore::KV> times;
  for (auto h : hosts) {
    ActiveHostsMan::updateHostInfo(
        kv.get(), h, HostInfo(now, meta::cpp2::HostRole::STORAGE, ""), times);
  }
  TestUtils::doPut(kv.get(), times);
  TestUtils::registerHB(kv.get(), hosts);

  // mock two space with 10 partitions and 3 replicas for each
  cpp2::SpaceDesc properties;
  GraphSpaceID id = 1;
  properties.space_name_ref() = "test_space";
  int partNum = 10;
  properties.partition_num_ref() = partNum;
  properties.replica_factor_ref() = 3;

  cpp2::SpaceDesc properties2;
  GraphSpaceID id2 = 2;
  properties2.space_name_ref() = "test_space2";
  properties2.partition_num_ref() = partNum;
  properties2.replica_factor_ref() = 3;

  auto spaceVal = MetaKeyUtils::spaceVal(properties);
  std::vector<nebula::kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::indexSpaceKey("test_space"),
                    std::string(reinterpret_cast<const char*>(&id), sizeof(GraphSpaceID)));
  data.emplace_back(MetaKeyUtils::spaceKey(id), MetaKeyUtils::spaceVal(properties));

  data.emplace_back(MetaKeyUtils::indexSpaceKey("test_space2"),
                    std::string(reinterpret_cast<const char*>(&id2), sizeof(GraphSpaceID)));
  data.emplace_back(MetaKeyUtils::spaceKey(id2), MetaKeyUtils::spaceVal(properties2));

  std::unordered_map<HostAddr, std::vector<size_t>> fromHostParts;
  for (auto partId = 1; partId <= partNum; partId++) {
    std::vector<HostAddr> partHosts;
    size_t idx = partId;
    for (int32_t i = 0; i < 3; i++, idx++) {
      auto h = hosts[idx % 3];
      partHosts.emplace_back(h);
      fromHostParts[h].emplace_back(partId);
    }
    data.emplace_back(MetaKeyUtils::partKey(id, partId), MetaKeyUtils::partVal(partHosts));
  }

  // mock three zones, each zone each host
  for (auto host : hosts) {
    std::string zoneName = folly::stringPrintf("default_zone_%s_%d", host.host.c_str(), host.port);
    data.emplace_back(MetaKeyUtils::zoneKey(zoneName), MetaKeyUtils::zoneVal({host}));
  }

  // mock an root user
  data.emplace_back(MetaKeyUtils::userKey("root"), MetaKeyUtils::userVal("root"));

  TestUtils::doPut(kv.get(), data);

  // check part data
  std::unique_ptr<kvstore::KVIterator> iter1;
  const auto& partPrefix = MetaKeyUtils::partPrefix(id);
  auto result = kv->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter1);
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
  while (iter1->valid()) {
    auto key = iter1->key();
    auto partId = MetaKeyUtils::parsePartKeyPartId(key);
    auto partHosts = MetaKeyUtils::parsePartVal(iter1->val());

    std::set<HostAddr> sortedHosts;
    for (const auto& host : partHosts) {
      sortedHosts.insert(host);
    }
    int idx = 0;
    for (const auto& host : sortedHosts) {
      LOG(INFO) << "partId:" << partId << ", host: " << host.toString();
      ASSERT_EQ(host, hosts[idx++]);
    }
    iter1->next();
  }

  // mock backuping full meta tables
  std::unordered_set<GraphSpaceID> spaces = {id};
  auto backupName = folly::sformat("BACKUP_{}", MetaKeyUtils::genTimestampStr());
  auto backupFiles = MetaServiceUtils::backupTables(kv.get(), spaces, backupName, nullptr);
  DCHECK(nebula::hasValue(backupFiles));
  {
    cpp2::RestoreMetaReq req;
    std::vector<std::string> files = nebula::value(backupFiles);
    auto it = std::find_if(files.cbegin(), files.cend(), [](auto& f) {
      auto const pos = f.find_last_of("/");
      auto name = f.substr(pos + 1);
      if (name == MetaKeyUtils::zonePrefix() + ".sst") {
        return true;
      }
      return false;
    });
    ASSERT_NE(it, files.cend());
    req.files_ref() = std::move(files);
    std::vector<cpp2::HostPair> hostPairs;
    HostAddr host4("127.0.0.4", 3360);
    HostAddr host5("127.0.0.5", 3360);
    HostAddr host6("127.0.0.6", 3360);
    std::unordered_map<HostAddr, HostAddr> hostMap = {
        {host1, host4}, {host2, host5}, {host3, host6}};
    for (auto [from, to] : hostMap) {
      hostPairs.emplace_back(apache::thrift::FragileConstructor(), from, to);
    }

    req.hosts_ref() = std::move(hostPairs);
    fs::TempDir restoreTootPath("/tmp/RestoreFullTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kvRestore(MockCluster::initMetaKV(restoreTootPath.path()));
    std::vector<nebula::kvstore::KV> restoreData;
    restoreData.emplace_back(MetaKeyUtils::userKey("root"), MetaKeyUtils::userVal("password"));
    TestUtils::doPut(kvRestore.get(), restoreData);

    auto* processor = RestoreProcessor::instance(kvRestore.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    // verify part distribution
    std::unique_ptr<kvstore::KVIterator> iter;
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    std::unordered_map<HostAddr, std::vector<size_t>> toHostParts;
    while (iter->valid()) {
      auto key = iter->key();
      auto partId = MetaKeyUtils::parsePartKeyPartId(key);
      auto partHosts = MetaKeyUtils::parsePartVal(iter->val());
      for (auto& host : partHosts) {
        LOG(INFO) << "partId:" << partId << ", host: " << host.toString();
        toHostParts[host].emplace_back(partId);
      }
      iter->next();
    }
    for (auto [host, fromParts] : fromHostParts) {
      auto toParts = toHostParts[hostMap[host]];
      ASSERT_EQ(fromParts.size(), toParts.size());
      ASSERT_TRUE(std::equal(fromParts.cbegin(), fromParts.cend(), toParts.cbegin()));
    }

    // verify zone hosts
    auto prefix = MetaKeyUtils::zonePrefix();
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    std::vector<cpp2::Zone> zones;
    std::vector<HostAddr> restoredHosts = {host4, host5, host6};

    std::unordered_map<std::string, HostAddr> restoreZoneNames;
    // mock three zones, each zone each host
    for (auto host : restoredHosts) {
      std::string zoneName =
          folly::stringPrintf("default_zone_%s_%d", host.host.c_str(), host.port);
      restoreZoneNames.emplace(zoneName, host);
    }

    while (iter->valid()) {
      auto zn = MetaKeyUtils::parseZoneName(iter->key());
      auto zoneHosts = MetaKeyUtils::parseZoneHosts(iter->val());
      ASSERT_TRUE(restoreZoneNames.find(zn) != restoreZoneNames.end());
      ASSERT_EQ(1, zoneHosts.size());
      ASSERT_EQ(restoreZoneNames[zn], zoneHosts[0]);
      iter->next();
    }

    prefix = MetaKeyUtils::userPrefix();
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    while (iter->valid()) {
      auto user = MetaKeyUtils::parseUser(iter->key());
      auto password = MetaKeyUtils::parseUserPwd(iter->val());
      ASSERT_EQ(user, std::string("root"));
      ASSERT_EQ(password, std::string("root"));
      iter->next();
    }

    prefix = MetaKeyUtils::indexPrefix(id);
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    while (iter->valid()) {
      auto sid = MetaKeyUtils::parseIndexesKeySpaceID(iter->key());
      ASSERT_EQ(sid, id);
      iter->next();
    }

    prefix = MetaKeyUtils::indexPrefix(id2);
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);
    while (iter->valid()) {
      auto sid = MetaKeyUtils::parseIndexesKeySpaceID(iter->key());
      ASSERT_EQ(sid, id2);
      iter->next();
    }
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
