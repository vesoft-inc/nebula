/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Format.h>                 // for sformat
#include <folly/futures/Future.h>         // for Future::get
#include <folly/init/Init.h>              // for init
#include <folly/synchronization/Baton.h>  // for Baton
#include <glog/logging.h>                 // for INFO
#include <gtest/gtest.h>                  // for Message
#include <gtest/gtest.h>                  // for TestPartResult
#include <gtest/gtest.h>                  // for Message
#include <gtest/gtest.h>                  // for TestPartResult
#include <stddef.h>                       // for size_t
#include <stdint.h>                       // for uint32_t, int32_t
#include <thrift/lib/cpp2/FieldRef.h>     // for field_ref
#include <thrift/lib/cpp2/Thrift.h>       // for FragileConstructor

#include <algorithm>      // for equal, find_if
#include <atomic>         // for atomic
#include <memory>         // for unique_ptr, allo...
#include <string>         // for string, basic_st...
#include <type_traits>    // for remove_reference...
#include <unordered_map>  // for unordered_map
#include <unordered_set>  // for unordered_set
#include <utility>        // for move, pair
#include <vector>         // for vector, vector<>...

#include "common/base/ErrorOr.h"                     // for hasValue, value
#include "common/base/Logging.h"                     // for SetStderrLogging
#include "common/datatypes/HostAddr.h"               // for HostAddr, hash
#include "common/fs/TempDir.h"                       // for TempDir
#include "common/thrift/ThriftTypes.h"               // for GraphSpaceID
#include "common/time/WallClock.h"                   // for WallClock
#include "common/utils/MetaKeyUtils.h"               // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"         // for ErrorCode, Error...
#include "interface/gen-cpp2/meta_types.h"           // for HostPair, SpaceDesc
#include "kvstore/Common.h"                          // for KV
#include "kvstore/KVIterator.h"                      // for KVIterator
#include "kvstore/KVStore.h"                         // for KVStore
#include "kvstore/NebulaStore.h"                     // for NebulaStore
#include "meta/ActiveHostsMan.h"                     // for ActiveHostsMan
#include "meta/MetaServiceUtils.h"                   // for MetaServiceUtils
#include "meta/processors/admin/RestoreProcessor.h"  // for RestoreProcessor
#include "meta/test/TestUtils.h"                     // for MockCluster, Tes...
#include "mock/MockCluster.h"                        // for MockCluster

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
    ActiveHostsMan::updateHostInfo(kv.get(), h, HostInfo(now, meta::cpp2::HostRole::STORAGE, ""));
  }

  meta::TestUtils::registerHB(kv.get(), hosts);

  // mock admin client
  bool ret = false;
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

  std::unordered_map<HostAddr, std::vector<size_t>> partInfo;

  for (auto partId = 1; partId <= partNum; partId++) {
    std::vector<HostAddr> hosts4;
    size_t idx = partId;
    for (int32_t i = 0; i < 3; i++, idx++) {
      auto h = hosts[idx % 3];
      hosts4.emplace_back(h);
      partInfo[h].emplace_back(partId);
    }
    data.emplace_back(MetaKeyUtils::partKey(id, partId), MetaKeyUtils::partVal(hosts4));
  }

  std::string zoneName = "test_zone";
  std::vector<std::string> zoneNames = {zoneName};

  data.emplace_back(MetaKeyUtils::userKey("root"), MetaKeyUtils::userVal("root"));

  auto zoneId = 1;
  data.emplace_back(MetaKeyUtils::indexZoneKey(zoneName),
                    std::string(reinterpret_cast<const char*>(&zoneId), sizeof(ZoneID)));
  data.emplace_back(MetaKeyUtils::zoneKey(zoneName), MetaKeyUtils::zoneVal(hosts));

  int32_t autoId = 666;
  data.emplace_back(MetaKeyUtils::idKey(),
                    std::string(reinterpret_cast<const char*>(&autoId), sizeof(autoId)));

  auto lastUpdateTime = time::WallClock::fastNowInMilliSec();
  data.emplace_back(MetaKeyUtils::lastUpdateTimeKey(),
                    MetaKeyUtils::lastUpdateTimeVal(lastUpdateTime));

  folly::Baton<true, std::atomic> baton;
  kv->asyncMultiPut(0, 0, std::move(data), [&](nebula::cpp2::ErrorCode code) {
    ret = (code == nebula::cpp2::ErrorCode::SUCCEEDED);
    baton.post();
  });
  baton.wait();

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
      if (name == MetaKeyUtils::zonePrefix()) {
        return true;
      }
      return false;
    });
    ASSERT_EQ(it, files.cend());
    req.files_ref() = std::move(files);
    std::vector<cpp2::HostPair> hostPairs;
    HostAddr host4("127.0.0.4", 3360);
    HostAddr host5("127.0.0.5", 3360);
    HostAddr host6("127.0.0.6", 3360);
    std::unordered_map<HostAddr, HostAddr> hostMap = {
        {host1, host4}, {host2, host5}, {host3, host6}};

    for (auto hm : hostMap) {
      hostPairs.emplace_back(apache::thrift::FragileConstructor(), hm.first, hm.second);
    }

    req.hosts_ref() = std::move(hostPairs);
    fs::TempDir restoreTootPath("/tmp/RestoreTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kvRestore(MockCluster::initMetaKV(restoreTootPath.path()));
    std::vector<nebula::kvstore::KV> restoreData;
    restoreData.emplace_back(MetaKeyUtils::userKey("root"), MetaKeyUtils::userVal("password"));

    folly::Baton<true, std::atomic> restoreBaton;
    kvRestore->asyncMultiPut(0, 0, std::move(restoreData), [&](nebula::cpp2::ErrorCode code) {
      ret = (code == nebula::cpp2::ErrorCode::SUCCEEDED);
      restoreBaton.post();
    });
    restoreBaton.wait();

    auto* processor = RestoreProcessor::instance(kvRestore.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    nebula::cpp2::ErrorCode result;
    std::unique_ptr<kvstore::KVIterator> iter;

    const auto& partPrefix = MetaKeyUtils::partPrefix(id);
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);

    std::unordered_map<HostAddr, std::vector<size_t>> toPartInfo;

    while (iter->valid()) {
      auto key = iter->key();
      auto partId = MetaKeyUtils::parsePartKeyPartId(key);
      auto partHosts = MetaKeyUtils::parsePartVal(iter->val());
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

    auto prefix = MetaKeyUtils::zonePrefix();
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);

    std::vector<cpp2::Zone> zones;
    std::vector<HostAddr> restoredHosts = {host4, host5, host6};
    while (iter->valid()) {
      auto zn = MetaKeyUtils::parseZoneName(iter->key());
      auto zoneHosts = MetaKeyUtils::parseZoneHosts(iter->val());
      cpp2::Zone zone;
      ASSERT_EQ(zoneName, zn);
      ASSERT_EQ(zoneHosts.size(), restoredHosts.size());
      for (std::vector<nebula::HostAddr>::size_type i = 0; i < zoneHosts.size(); ++i) {
        ASSERT_EQ(zoneHosts[i], restoredHosts[i]);
      }
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

TEST(RestoreProcessorTest, RestoreFullTest) {
  fs::TempDir rootPath("/tmp/RestoreFullOriginTest.XXXXXX");
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
    ActiveHostsMan::updateHostInfo(kv.get(), h, HostInfo(now, meta::cpp2::HostRole::STORAGE, ""));
  }

  meta::TestUtils::registerHB(kv.get(), hosts);

  // mock admin client
  bool ret = false;
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

  std::unordered_map<HostAddr, std::vector<size_t>> partInfo;

  for (auto partId = 1; partId <= partNum; partId++) {
    std::vector<HostAddr> hosts4;
    size_t idx = partId;
    for (int32_t i = 0; i < 3; i++, idx++) {
      auto h = hosts[idx % 3];
      hosts4.emplace_back(h);
      partInfo[h].emplace_back(partId);
    }
    data.emplace_back(MetaKeyUtils::partKey(id, partId), MetaKeyUtils::partVal(hosts4));
  }

  std::string zoneName = "test_zone";
  std::vector<std::string> zoneNames = {zoneName};
  data.emplace_back(MetaKeyUtils::userKey("root"), MetaKeyUtils::userVal("root"));

  auto zoneId = 1;
  data.emplace_back(MetaKeyUtils::indexZoneKey(zoneName),
                    std::string(reinterpret_cast<const char*>(&zoneId), sizeof(ZoneID)));
  data.emplace_back(MetaKeyUtils::zoneKey(zoneName), MetaKeyUtils::zoneVal(hosts));

  folly::Baton<true, std::atomic> baton;
  kv->asyncMultiPut(0, 0, std::move(data), [&](nebula::cpp2::ErrorCode code) {
    ret = (code == nebula::cpp2::ErrorCode::SUCCEEDED);
    baton.post();
  });
  baton.wait();

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
      if (name == MetaKeyUtils::zonePrefix()) {
        return true;
      }
      return false;
    });
    ASSERT_EQ(it, files.cend());
    req.files_ref() = std::move(files);
    std::vector<cpp2::HostPair> hostPairs;
    HostAddr host4("127.0.0.4", 3360);
    HostAddr host5("127.0.0.5", 3360);
    HostAddr host6("127.0.0.6", 3360);
    std::unordered_map<HostAddr, HostAddr> hostMap = {
        {host1, host4}, {host2, host5}, {host3, host6}};

    for (auto hm : hostMap) {
      hostPairs.emplace_back(apache::thrift::FragileConstructor(), hm.first, hm.second);
    }

    req.hosts_ref() = std::move(hostPairs);
    fs::TempDir restoreTootPath("/tmp/RestoreFullTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kvRestore(MockCluster::initMetaKV(restoreTootPath.path()));
    std::vector<nebula::kvstore::KV> restoreData;
    restoreData.emplace_back(MetaKeyUtils::userKey("root"), MetaKeyUtils::userVal("password"));

    folly::Baton<true, std::atomic> restoreBaton;
    kvRestore->asyncMultiPut(0, 0, std::move(restoreData), [&](nebula::cpp2::ErrorCode code) {
      ret = (code == nebula::cpp2::ErrorCode::SUCCEEDED);
      restoreBaton.post();
    });
    restoreBaton.wait();

    auto* processor = RestoreProcessor::instance(kvRestore.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    nebula::cpp2::ErrorCode result;
    std::unique_ptr<kvstore::KVIterator> iter;

    const auto& partPrefix = MetaKeyUtils::partPrefix(id);
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);

    std::unordered_map<HostAddr, std::vector<size_t>> toPartInfo;

    while (iter->valid()) {
      auto key = iter->key();
      auto partId = MetaKeyUtils::parsePartKeyPartId(key);
      auto partHosts = MetaKeyUtils::parsePartVal(iter->val());
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

    auto prefix = MetaKeyUtils::zonePrefix();
    result = kvRestore->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, result);

    std::vector<cpp2::Zone> zones;
    std::vector<HostAddr> restoredHosts = {host4, host5, host6};
    while (iter->valid()) {
      auto zn = MetaKeyUtils::parseZoneName(iter->key());
      auto zoneHosts = MetaKeyUtils::parseZoneHosts(iter->val());
      cpp2::Zone zone;
      ASSERT_EQ(zoneName, zn);
      ASSERT_EQ(zoneHosts.size(), restoredHosts.size());
      for (std::vector<nebula::HostAddr>::size_type i = 0; i < zoneHosts.size(); ++i) {
        ASSERT_EQ(zoneHosts[i], restoredHosts[i]);
      }
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
