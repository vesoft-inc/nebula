/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_TEST_TESTUTILS_H_
#define META_TEST_TESTUTILS_H_

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp/concurrency/ThreadManager.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "common/base/Base.h"
#include "common/base/CommonMacro.h"
#include "common/base/ObjectPool.h"
#include "common/expression/ConstantExpression.h"
#include "common/time/WallClock.h"
#include "common/utils/DefaultValueContext.h"
#include "interface/gen-cpp2/common_types.h"
#include "kvstore/KVStore.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceHandler.h"
#include "meta/processors/parts/ListHostsProcessor.h"
#include "meta/processors/user/AuthenticationProcessor.h"
#include "mock/MockCluster.h"
#include "version/Version.h"

DECLARE_string(part_man_type);

namespace nebula {
namespace meta {

using mock::MockCluster;
using nebula::cpp2::PropertyType;

using ZoneInfo = std::unordered_map<std::string, std::vector<HostAddr>>;

ObjectPool metaTestPool;
auto metaPool = &metaTestPool;

class TestUtils {
 public:
  static cpp2::ColumnDef columnDef(int32_t index,
                                   PropertyType type,
                                   Value defaultValue = Value(),
                                   bool isNull = true,
                                   int16_t typeLen = 0) {
    cpp2::ColumnDef column;
    column.name_ref() = folly::stringPrintf("col_%d", index);
    if (!defaultValue.empty()) {
      const auto& defaultExpr = *ConstantExpression::make(metaPool, defaultValue);
      column.default_value_ref() = Expression::encode(defaultExpr);
    }
    column.nullable_ref() = isNull;
    cpp2::ColumnTypeDef typeDef;
    typeDef.type_ref() = type;
    if (type == PropertyType::FIXED_STRING) {
      typeDef.type_length_ref() = typeLen;
    }
    column.type_ref() = std::move(typeDef);
    return column;
  }

  static void registerHB(kvstore::KVStore* kv, const std::vector<HostAddr>& hosts) {
    return setupHB(kv, hosts, cpp2::HostRole::STORAGE, gitInfoSha());
  }

  static void setupHB(kvstore::KVStore* kv,
                      const std::vector<HostAddr>& hosts,
                      cpp2::HostRole role,
                      const std::string& gitInfoSha) {
    auto now = time::WallClock::fastNowInMilliSec();
    std::vector<kvstore::KV> data;
    for (auto& h : hosts) {
      auto ret = ActiveHostsMan::updateHostInfo(kv, h, HostInfo(now, role, gitInfoSha), data);
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    }
    doPut(kv, data);
  }

  static void doPut(kvstore::KVStore* kv, std::vector<kvstore::KV> data) {
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](auto) { baton.post(); });
    baton.wait();
  }

  static void doRemove(kvstore::KVStore* kv, std::vector<std::string>&& keys) {
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiRemove(
        kDefaultSpaceId, kDefaultPartId, std::move(keys), [&](auto) { baton.post(); });
    baton.wait();
  }

  static std::unique_ptr<kvstore::KVIterator> doPrefix(kvstore::KVStore* kv,
                                                       const std::string& prefix) {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter, true);
    CHECK(code == nebula::cpp2::ErrorCode::SUCCEEDED);
    return iter;
  }

  static int32_t createSomeHosts(kvstore::KVStore* kv,
                                 std::vector<HostAddr> hosts = {
                                     {"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}}) {
    // Record machine information
    std::vector<kvstore::KV> machines;
    for (auto& host : hosts) {
      VLOG(3) << "Register machine: " << host;
      machines.emplace_back(nebula::MetaKeyUtils::machineKey(host.host, host.port), "");
    }
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(machines), [&](auto) { baton.post(); });
    baton.wait();

    std::vector<HostAddr> thriftHosts(hosts);
    registerHB(kv, hosts);
    {
      cpp2::ListHostsReq req;
      req.type_ref() = cpp2::ListHostType::STORAGE;

      auto* processor = ListHostsProcessor::instance(kv);
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      CHECK_EQ(hosts.size(), (*resp.hosts_ref()).size());
      for (decltype(hosts.size()) i = 0; i < hosts.size(); i++) {
        CHECK_EQ(hosts[i], *(*resp.hosts_ref())[i].hostAddr_ref());
      }
    }
    return hosts.size();
  }

  static void assembleZone(kvstore::KVStore* kv, const ZoneInfo& zoneInfo) {
    std::vector<kvstore::KV> data;
    int32_t id = 10;
    for (auto iter = zoneInfo.begin(); iter != zoneInfo.end(); iter++) {
      data.emplace_back(MetaKeyUtils::indexZoneKey(iter->first),
                        std::string(reinterpret_cast<const char*>(&id), sizeof(ZoneID)));
      data.emplace_back(MetaKeyUtils::zoneKey(iter->first), MetaKeyUtils::zoneVal(iter->second));
      id += 1;
    }

    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  static bool modifyHostAboutZone(kvstore::KVStore* kv,
                                  const std::string& zoneName,
                                  HostAddr host,
                                  bool isAdd) {
    auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
    std::string zoneValue;
    auto retCode = kv->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneValue);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Get zone " << zoneName << " failed";
      return false;
    }

    auto hosts = MetaKeyUtils::parseZoneHosts(std::move(zoneValue));
    auto iter = std::find(hosts.begin(), hosts.end(), host);
    if (isAdd) {
      if (iter != hosts.end()) {
        LOG(ERROR) << "Host " << host << " have existed";
        return false;
      }
      hosts.emplace_back(std::move(host));
    } else {
      if (iter == hosts.end()) {
        LOG(ERROR) << "Host " << host << " not existed";
        return false;
      }
      hosts.erase(iter);
    }

    std::vector<kvstore::KV> data;
    data.emplace_back(zoneKey, MetaKeyUtils::zoneVal(std::move(hosts)));

    bool ret = false;
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
    return ret;
  }

  static void assembleSpace(kvstore::KVStore* kv,
                            GraphSpaceID id,
                            int32_t partitionNum,
                            int32_t replica = 1,
                            int32_t totalHost = 1,
                            bool multispace = false) {
    // mock the part distribution like create space
    cpp2::SpaceDesc properties;
    if (multispace) {
      properties.space_name_ref() = folly::stringPrintf("test_space_%d", id);
    } else {
      properties.space_name_ref() = "test_space";
    }
    properties.partition_num_ref() = partitionNum;
    properties.replica_factor_ref() = replica;
    auto spaceVal = MetaKeyUtils::spaceVal(properties);
    std::vector<nebula::kvstore::KV> data;
    if (multispace) {
      data.emplace_back(MetaKeyUtils::indexSpaceKey(folly::stringPrintf("test_space_%d", id)),
                        std::string(reinterpret_cast<const char*>(&id), sizeof(GraphSpaceID)));
    } else {
      data.emplace_back(MetaKeyUtils::indexSpaceKey("test_space"),
                        std::string(reinterpret_cast<const char*>(&id), sizeof(GraphSpaceID)));
    }
    data.emplace_back(MetaKeyUtils::spaceKey(id), MetaKeyUtils::spaceVal(properties));

    std::vector<HostAddr> allHosts;
    for (int i = 0; i < totalHost; i++) {
      allHosts.emplace_back(std::to_string(i), i);
    }

    for (auto partId = 1; partId <= partitionNum; partId++) {
      std::vector<HostAddr> hosts;
      size_t idx = partId;
      for (int32_t i = 0; i < replica; i++, idx++) {
        hosts.emplace_back(allHosts[idx % totalHost]);
      }
      data.emplace_back(MetaKeyUtils::partKey(id, partId), MetaKeyUtils::partVal(hosts));
    }
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  static void addZoneToSpace(kvstore::KVStore* kv,
                             GraphSpaceID id,
                             const std::vector<std::string>& zones) {
    std::string spaceKey = MetaKeyUtils::spaceKey(id);
    std::string spaceVal;
    kv->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &spaceVal);
    meta::cpp2::SpaceDesc properties = MetaKeyUtils::parseSpace(spaceVal);
    std::vector<std::string> curZones = properties.get_zone_names();
    curZones.insert(curZones.end(), zones.begin(), zones.end());
    properties.zone_names_ref() = curZones;
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaKeyUtils::spaceKey(id), MetaKeyUtils::spaceVal(properties));
    folly::Baton<true, std::atomic> baton;
    auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
    kv->asyncMultiPut(kDefaultSpaceId,
                      kDefaultPartId,
                      std::move(data),
                      [&ret, &baton](nebula::cpp2::ErrorCode code) {
                        if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
                          ret = code;
                          LOG(INFO) << "Put data error on meta server";
                        }
                        baton.post();
                      });
    baton.wait();
  }

  static void assembleSpaceWithZone(kvstore::KVStore* kv,
                                    GraphSpaceID id,
                                    int32_t partitionNum,
                                    int32_t replica,
                                    int32_t zoneNum,
                                    int32_t totalHost) {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "test_space";
    properties.partition_num_ref() = partitionNum;
    properties.replica_factor_ref() = replica;
    auto spaceVal = MetaKeyUtils::spaceVal(properties);
    std::vector<nebula::kvstore::KV> data;
    data.emplace_back(MetaKeyUtils::indexSpaceKey("test_space"),
                      std::string(reinterpret_cast<const char*>(&id), sizeof(GraphSpaceID)));
    std::vector<std::pair<std::string, std::vector<HostAddr>>> zones;
    std::vector<std::string> zoneNames;
    std::map<std::string, int32_t> zonePartNum;
    for (int32_t i = 0; i < zoneNum; i++) {
      zones.push_back({std::to_string(i + 1), {}});
      zonePartNum[std::to_string(i + 1)] = 0;
      zoneNames.push_back(std::to_string(i + 1));
    }
    properties.zone_names_ref() = zoneNames;
    data.emplace_back(MetaKeyUtils::spaceKey(id), MetaKeyUtils::spaceVal(properties));
    std::vector<HostAddr> allHosts;
    for (int32_t i = 0; i < totalHost; i++) {
      zones[i % zoneNum].second.emplace_back("127.0.0.1", i + 1);
      allHosts.emplace_back("127.0.0.1", i + 1);
      data.emplace_back(nebula::MetaKeyUtils::machineKey("127.0.0.1", i + 1), "");
      data.emplace_back(nebula::MetaKeyUtils::hostKey("127.0.0.1", i + 1),
                        HostInfo::encodeV2(HostInfo(
                            time::WallClock::fastNowInMilliSec(), cpp2::HostRole::STORAGE, "")));
    }
    for (auto& p : zones) {
      data.emplace_back(MetaKeyUtils::zoneKey(p.first), MetaKeyUtils::zoneVal(p.second));
    }
    for (auto partId = 1; partId <= partitionNum; partId++) {
      std::vector<HostAddr> hosts;
      size_t idx = partId;
      for (int32_t i = 0; i < replica; i++, idx++) {
        std::string zoneName = zones[idx % zoneNum].first;
        std::vector<HostAddr>& zoneHosts = zones[idx % zoneNum].second;
        int32_t hostIndex = zonePartNum[zoneName] % zoneHosts.size();
        hosts.push_back(zoneHosts[hostIndex]);
        zonePartNum[zoneName]++;
      }
      data.emplace_back(MetaKeyUtils::partKey(id, partId), MetaKeyUtils::partVal(hosts));
    }
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  static void mockTag(kvstore::KVStore* kv,
                      int32_t tagNum,
                      SchemaVer version = 0,
                      bool nullable = false,
                      TagID startId = 0,
                      GraphSpaceID spaceId = 1) {
    std::vector<nebula::kvstore::KV> tags;
    SchemaVer ver = version;
    for (auto t = 0; t < tagNum; t++) {
      TagID tagId = t + startId;
      cpp2::Schema srcsch;
      for (auto i = 0; i < 2; i++) {
        cpp2::ColumnDef column;
        column.name_ref() = folly::stringPrintf("tag_%d_col_%d", tagId, i);
        if (i < 1) {
          column.type.type_ref() = PropertyType::INT64;
        } else {
          column.type.type_ref() = PropertyType::FIXED_STRING;
          column.type.type_length_ref() = MAX_INDEX_TYPE_LENGTH;
        }
        if (nullable) {
          column.nullable_ref() = nullable;
        }
        (*srcsch.columns_ref()).emplace_back(std::move(column));
      }
      auto tagName = folly::stringPrintf("tag_%d", tagId);
      auto tagIdVal = std::string(reinterpret_cast<const char*>(&tagId), sizeof(tagId));
      tags.emplace_back(MetaKeyUtils::indexTagKey(spaceId, tagName), tagIdVal);
      tags.emplace_back(MetaKeyUtils::schemaTagKey(spaceId, tagId, ver++),
                        MetaKeyUtils::schemaVal(tagName, srcsch));
    }
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(tags), [&](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  static void mockTagIndex(kvstore::KVStore* kv,
                           TagID tagID,
                           std::string tagName,
                           IndexID indexID,
                           std::string indexName,
                           std::vector<cpp2::ColumnDef> columns) {
    GraphSpaceID space = 1;
    cpp2::IndexItem item;
    item.index_id_ref() = indexID;
    item.index_name_ref() = indexName;
    nebula::cpp2::SchemaID schemaID;
    schemaID.tag_id_ref() = tagID;
    item.schema_id_ref() = std::move(schemaID);
    item.schema_name_ref() = std::move(tagName);
    item.fields_ref() = std::move(columns);

    std::vector<nebula::kvstore::KV> data;
    data.emplace_back(MetaKeyUtils::indexIndexKey(space, indexName),
                      std::string(reinterpret_cast<const char*>(&indexID), sizeof(IndexID)));
    data.emplace_back(MetaKeyUtils::indexKey(space, indexID), MetaKeyUtils::indexVal(item));
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  static void mockEdgeIndex(kvstore::KVStore* kv,
                            EdgeType edgeType,
                            std::string edgeName,
                            IndexID indexID,
                            std::string indexName,
                            std::vector<cpp2::ColumnDef> columns) {
    GraphSpaceID space = 1;
    cpp2::IndexItem item;
    item.index_id_ref() = indexID;
    item.index_name_ref() = indexName;
    nebula::cpp2::SchemaID schemaID;
    schemaID.edge_type_ref() = edgeType;
    item.schema_id_ref() = std::move(schemaID);
    item.schema_name_ref() = std::move(edgeName);
    item.fields_ref() = std::move(columns);

    std::vector<nebula::kvstore::KV> data;
    data.emplace_back(MetaKeyUtils::indexIndexKey(space, indexName),
                      std::string(reinterpret_cast<const char*>(&indexID), sizeof(IndexID)));
    data.emplace_back(MetaKeyUtils::indexKey(space, indexID), MetaKeyUtils::indexVal(item));
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  static void mockEdge(kvstore::KVStore* kv,
                       int32_t edgeNum,
                       SchemaVer version = 0,
                       bool nullable = false,
                       EdgeType startEdgeType = 0,
                       GraphSpaceID spaceId = 1) {
    std::vector<nebula::kvstore::KV> edges;
    SchemaVer ver = version;
    for (auto t = 0; t < edgeNum; t++) {
      EdgeType edgeType = t + startEdgeType;
      cpp2::Schema srcsch;
      for (auto i = 0; i < 2; i++) {
        cpp2::ColumnDef column;
        column.name = folly::stringPrintf("edge_%d_col_%d", edgeType, i);
        if (i < 1) {
          column.type.type_ref() = PropertyType::INT64;
        } else {
          column.type.type_ref() = PropertyType::FIXED_STRING;
          column.type.type_length_ref() = MAX_INDEX_TYPE_LENGTH;
        }
        if (nullable) {
          column.nullable_ref() = nullable;
        }
        (*srcsch.columns_ref()).emplace_back(std::move(column));
      }
      auto edgeName = folly::stringPrintf("edge_%d", edgeType);
      auto edgeTypeVal = std::string(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType));
      edges.emplace_back(MetaKeyUtils::indexEdgeKey(spaceId, edgeName), edgeTypeVal);
      edges.emplace_back(MetaKeyUtils::schemaEdgeKey(spaceId, edgeType, ver++),
                         MetaKeyUtils::schemaVal(edgeName, srcsch));
    }

    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(edges), [&](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  static cpp2::Schema mockSchemaWithAllType() {
    cpp2::Schema schema;
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    cols.emplace_back(TestUtils::columnDef(0, PropertyType::BOOL, Value(true), false));
    cols.emplace_back(TestUtils::columnDef(1, PropertyType::INT8, Value(NullType::__NULL__), true));
    cols.emplace_back(TestUtils::columnDef(2, PropertyType::INT16, Value(20), false));
    cols.emplace_back(TestUtils::columnDef(3, PropertyType::INT32, Value(200), false));
    cols.emplace_back(TestUtils::columnDef(4, PropertyType::INT64, Value(2000), false));
    cols.emplace_back(TestUtils::columnDef(5, PropertyType::FLOAT, Value(10.0), false));
    cols.emplace_back(TestUtils::columnDef(6, PropertyType::DOUBLE, Value(20.0), false));
    cols.emplace_back(TestUtils::columnDef(7, PropertyType::STRING, Value("string"), false));
    cols.emplace_back(
        TestUtils::columnDef(8, PropertyType::FIXED_STRING, Value("longlongstring"), false, 10));
    cols.emplace_back(TestUtils::columnDef(9, PropertyType::TIMESTAMP, Value(123456), true));
    cols.emplace_back(TestUtils::columnDef(10, PropertyType::DATE, Value(Date()), true));
    cols.emplace_back(TestUtils::columnDef(11, PropertyType::DATETIME, Value(DateTime()), true));
    schema.columns_ref() = std::move(cols);
    return schema;
  }

  static void checkSchemaWithAllType(const cpp2::Schema& schema) {
    DefaultValueContext defaultContext;
    ASSERT_EQ((*schema.columns_ref()).size(), 12);
    ASSERT_EQ((*schema.columns_ref())[0].get_name(), "col_0");
    ASSERT_EQ((*schema.columns_ref())[0].get_type().get_type(), PropertyType::BOOL);
    auto expr = Expression::decode(metaPool, *(*schema.columns_ref())[0].get_default_value());
    ASSERT_EQ(Expression::eval(expr, defaultContext), Value(true));
    ASSERT_EQ(*(*schema.columns_ref())[0].get_nullable(), false);

    ASSERT_EQ((*schema.columns_ref())[1].get_name(), "col_1");
    ASSERT_EQ((*schema.columns_ref())[1].get_type().get_type(), PropertyType::INT8);
    expr = Expression::decode(metaPool, *(*schema.columns_ref())[1].get_default_value());
    ASSERT_EQ(Expression::eval(expr, defaultContext), Value(NullType::__NULL__));
    ASSERT_EQ(*(*schema.columns_ref())[1].get_nullable(), true);

    ASSERT_EQ((*schema.columns_ref())[2].get_name(), "col_2");
    ASSERT_EQ((*schema.columns_ref())[2].get_type().get_type(), PropertyType::INT16);
    expr = Expression::decode(metaPool, *(*schema.columns_ref())[2].get_default_value());
    ASSERT_EQ(Expression::eval(expr, defaultContext), Value(20));
    ASSERT_EQ(*(*schema.columns_ref())[2].get_nullable(), false);

    ASSERT_EQ((*schema.columns_ref())[3].get_name(), "col_3");
    ASSERT_EQ((*schema.columns_ref())[3].get_type().get_type(), PropertyType::INT32);
    expr = Expression::decode(metaPool, *(*schema.columns_ref())[3].get_default_value());
    ASSERT_EQ(Expression::eval(expr, defaultContext), Value(200));
    ASSERT_EQ(*(*schema.columns_ref())[3].get_nullable(), false);

    ASSERT_EQ((*schema.columns_ref())[4].get_name(), "col_4");
    ASSERT_EQ((*schema.columns_ref())[4].get_type().get_type(), PropertyType::INT64);
    expr = Expression::decode(metaPool, *(*schema.columns_ref())[4].get_default_value());
    ASSERT_EQ(Expression::eval(expr, defaultContext), Value(2000));
    ASSERT_EQ(*(*schema.columns_ref())[4].get_nullable(), false);

    ASSERT_EQ((*schema.columns_ref())[5].get_name(), "col_5");
    ASSERT_EQ((*schema.columns_ref())[5].get_type().get_type(), PropertyType::FLOAT);
    expr = Expression::decode(metaPool, *(*schema.columns_ref())[5].get_default_value());
    ASSERT_EQ(Expression::eval(expr, defaultContext), Value(10.0));
    ASSERT_EQ(*(*schema.columns_ref())[5].get_nullable(), false);

    ASSERT_EQ((*schema.columns_ref())[6].get_name(), "col_6");
    ASSERT_EQ((*schema.columns_ref())[6].get_type().get_type(), PropertyType::DOUBLE);
    expr = Expression::decode(metaPool, *(*schema.columns_ref())[6].get_default_value());
    ASSERT_EQ(Expression::eval(expr, defaultContext), Value(20.0));
    ASSERT_EQ(*(*schema.columns_ref())[6].get_nullable(), false);

    ASSERT_EQ((*schema.columns_ref())[7].get_name(), "col_7");
    ASSERT_EQ((*schema.columns_ref())[7].get_type().get_type(), PropertyType::STRING);
    expr = Expression::decode(metaPool, *(*schema.columns_ref())[7].get_default_value());
    ASSERT_EQ(Expression::eval(expr, defaultContext), Value("string"));
    ASSERT_EQ(*(*schema.columns_ref())[7].get_nullable(), false);

    ASSERT_EQ((*schema.columns_ref())[8].get_name(), "col_8");
    ASSERT_EQ((*schema.columns_ref())[8].get_type().get_type(), PropertyType::FIXED_STRING);
    expr = Expression::decode(metaPool, *(*schema.columns_ref())[8].get_default_value());
    ASSERT_EQ(Expression::eval(expr, defaultContext), Value("longlongst"));
    ASSERT_EQ(*(*schema.columns_ref())[8].get_nullable(), false);
    ASSERT_EQ(*(*schema.columns_ref())[8].get_type().get_type_length(), 10);

    ASSERT_EQ((*schema.columns_ref())[9].get_name(), "col_9");
    ASSERT_EQ((*schema.columns_ref())[9].get_type().get_type(), PropertyType::TIMESTAMP);
    expr = Expression::decode(metaPool, *(*schema.columns_ref())[9].get_default_value());
    ASSERT_EQ(Expression::eval(expr, defaultContext), Value(123456));
    ASSERT_EQ(*(*schema.columns_ref())[9].get_nullable(), true);

    ASSERT_EQ((*schema.columns_ref())[10].get_name(), "col_10");
    ASSERT_EQ((*schema.columns_ref())[10].get_type().get_type(), PropertyType::DATE);
    expr = Expression::decode(metaPool, *(*schema.columns_ref())[10].get_default_value());
    ASSERT_EQ(Expression::eval(expr, defaultContext), Value(Date()));
    ASSERT_EQ(*(*schema.columns_ref())[10].get_nullable(), true);

    ASSERT_EQ((*schema.columns_ref())[11].get_name(), "col_11");
    ASSERT_EQ((*schema.columns_ref())[11].get_type().get_type(), PropertyType::DATETIME);
    expr = Expression::decode(metaPool, *(*schema.columns_ref())[11].get_default_value());
    ASSERT_EQ(Expression::eval(expr, defaultContext), Value(DateTime()));
    ASSERT_EQ(*(*schema.columns_ref())[11].get_nullable(), true);
  }

  static bool verifySchema(cpp2::Schema& result, cpp2::Schema& expected) {
    if (result.get_columns().size() != expected.get_columns().size()) {
      return false;
    }

    std::vector<cpp2::ColumnDef> resultColumns = result.get_columns();
    std::vector<cpp2::ColumnDef> expectedColumns = expected.get_columns();
    std::sort(
        resultColumns.begin(), resultColumns.end(), [](cpp2::ColumnDef col0, cpp2::ColumnDef col1) {
          return col0.get_name() < col1.get_name();
        });
    std::sort(expectedColumns.begin(),
              expectedColumns.end(),
              [](cpp2::ColumnDef col0, cpp2::ColumnDef col1) {
                return col0.get_name() < col1.get_name();
              });

    int32_t size = resultColumns.size();
    for (auto i = 0; i < size; i++) {
      if (resultColumns[i].get_name() != expectedColumns[i].get_name() ||
          resultColumns[i].get_type() != expectedColumns[i].get_type()) {
        return false;
      }
    }
    return true;
  }

  static bool verifyMap(std::map<std::string, std::vector<std::string>>& result,
                        std::map<std::string, std::vector<std::string>>& expected) {
    if (result.size() != expected.size()) {
      return false;
    }

    for (auto resultIter = result.begin(), expectedIter = expected.begin();
         resultIter != result.end() && expectedIter != expected.end();
         resultIter++, expectedIter++) {
      if (resultIter->first != expectedIter->first &&
          !verifyResult(resultIter->second, expectedIter->second)) {
        return false;
      }
    }

    return true;
  }

  static bool verifyMap(std::map<std::string, std::vector<cpp2::ColumnDef>>& result,
                        std::map<std::string, std::vector<cpp2::ColumnDef>>& expected) {
    if (result.size() != expected.size()) {
      return false;
    }

    for (auto resultIter = result.begin(), expectedIter = expected.begin();
         resultIter != result.end() && expectedIter != expected.end();
         resultIter++, expectedIter++) {
      if (resultIter->first != expectedIter->first &&
          !verifyResult(resultIter->second, expectedIter->second)) {
        return false;
      }
    }
    return true;
  }

  static bool verifyResult(std::vector<std::string>& result, std::vector<std::string>& expected) {
    if (result.size() != expected.size()) {
      return false;
    }

    std::sort(result.begin(), result.end());
    std::sort(expected.begin(), expected.end());
    int32_t size = result.size();
    for (int32_t i = 0; i < size; i++) {
      if (result[i] != expected[i]) {
        return false;
      }
    }

    return true;
  }

  static bool verifyResult(std::vector<cpp2::ColumnDef>& result,
                           std::vector<cpp2::ColumnDef>& expected) {
    if (result.size() != expected.size()) {
      return false;
    }

    std::sort(result.begin(), result.end(), [](cpp2::ColumnDef& x, cpp2::ColumnDef& y) {
      return x.get_name().compare(y.get_name());
    });
    std::sort(expected.begin(), expected.end(), [](cpp2::ColumnDef& x, cpp2::ColumnDef& y) {
      return x.get_name().compare(y.get_name());
    });
    int32_t size = result.size();
    for (int32_t i = 0; i < size; i++) {
      if (result[i].get_name() != expected[i].get_name() ||
          result[i].get_type() != expected[i].get_type()) {
        return false;
      }
      if (result[i].nullable_ref().has_value() != expected[i].nullable_ref().has_value() ||
          (result[i].nullable_ref().has_value() &&
           *result[i].nullable_ref() != *expected[i].nullable_ref())) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace meta
}  // namespace nebula

#endif  // META_TEST_TESTUTILS_H_
