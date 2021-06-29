/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_TEST_TESTUTILS_H_
#define META_TEST_TESTUTILS_H_

#include "common/base/Base.h"
#include "common/base/ObjectPool.h"
#include "common/base/CommonMacro.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "common/time/WallClock.h"
#include "common/expression/ConstantExpression.h"
#include "common/version/Version.h"
#include "mock/MockCluster.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/partsMan/ListHostsProcessor.h"
#include "meta/MetaServiceHandler.h"
#include "meta/processors/usersMan/AuthenticationProcessor.h"
#include "meta/ActiveHostsMan.h"
#include "utils/DefaultValueContext.h"
#include <gtest/gtest.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp/concurrency/ThreadManager.h>
#include <folly/synchronization/Baton.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

DECLARE_string(part_man_type);

namespace nebula {
namespace meta {

using nebula::meta::cpp2::PropertyType;
using mock::MockCluster;

using ZoneInfo  = std::unordered_map<std::string, std::vector<HostAddr>>;
using GroupInfo = std::unordered_map<std::string, std::vector<std::string>>;

ObjectPool metaTestPool;
auto metaPool = &metaTestPool;

class TestUtils {
public:
    static cpp2::ColumnDef columnDef(int32_t index, cpp2::PropertyType type,
            Value defaultValue = Value(), bool isNull = true, int16_t typeLen = 0) {
        cpp2::ColumnDef column;
        column.set_name(folly::stringPrintf("col_%d", index));
        if (!defaultValue.empty()) {
            const auto& defaultExpr = *ConstantExpression::make(metaPool, defaultValue);
            column.set_default_value(Expression::encode(defaultExpr));
        }
        column.set_nullable(isNull);
        cpp2::ColumnTypeDef typeDef;
        typeDef.set_type(type);
        if (type == PropertyType::FIXED_STRING) {
            typeDef.set_type_length(typeLen);
        }
        column.set_type(std::move(typeDef));
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
        for (auto& h : hosts) {
            auto ret = ActiveHostsMan::updateHostInfo(kv, h, HostInfo(now, role, gitInfoSha));
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
        }
    }

    static int32_t createSomeHosts(kvstore::KVStore* kv,
                                   std::vector<HostAddr> hosts
                                       = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}}) {
        std::vector<HostAddr> thriftHosts(hosts);
        registerHB(kv, hosts);
        {
            cpp2::ListHostsReq req;
            req.set_type(cpp2::ListHostType::STORAGE);

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

    static void assembleGroupAndZone(kvstore::KVStore* kv,
                                     const ZoneInfo& zoneInfo,
                                     const GroupInfo& groupInfo) {
        std::vector<kvstore::KV> data;
        int32_t id = 10;
        for (auto iter = zoneInfo.begin(); iter != zoneInfo.end(); iter++) {
            data.emplace_back(MetaServiceUtils::indexZoneKey(iter->first),
                              std::string(reinterpret_cast<const char*>(&id), sizeof(ZoneID)));
            data.emplace_back(MetaServiceUtils::zoneKey(iter->first),
                              MetaServiceUtils::zoneVal(iter->second));
            id += 1;
        }

        id = 100;
        for (auto iter = groupInfo.begin(); iter != groupInfo.end(); iter++) {
            data.emplace_back(MetaServiceUtils::indexGroupKey(iter->first),
                              std::string(reinterpret_cast<const char*>(&id), sizeof(GroupID)));
            data.emplace_back(MetaServiceUtils::groupKey(iter->first),
                              MetaServiceUtils::groupVal(iter->second));
            id += 1;
        }

        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, 0, std::move(data),
                          [&] (nebula::cpp2::ErrorCode code) {
                              ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
                              baton.post();
                          });
        baton.wait();
    }

    static bool modifyHostAboutZone(kvstore::KVStore* kv, const std::string& zoneName,
                                    HostAddr host, bool isAdd) {
        auto zoneKey = MetaServiceUtils::zoneKey(zoneName);
        std::string zoneValue;
        auto retCode = kv->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneValue);
        if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Get zone " <<  zoneName << " failed";
            return false;
        }

        auto hosts = MetaServiceUtils::parseZoneHosts(std::move(zoneValue));
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
        data.emplace_back(zoneKey, MetaServiceUtils::zoneVal(std::move(hosts)));

        bool ret = false;
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, 0, std::move(data),
                          [&] (nebula::cpp2::ErrorCode code) {
                              ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
                              baton.post();
                          });
        baton.wait();
        return ret;
    }

    static bool modifyZoneAboutGroup(kvstore::KVStore* kv, const std::string& groupName,
                                     const std::string& zoneName, bool isAdd) {
        auto groupKey = MetaServiceUtils::groupKey(groupName);
        std::string groupValue;
        auto retCode = kv->get(kDefaultSpaceId, kDefaultPartId, groupKey, &groupValue);
        if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Get group " <<  groupName << " failed";
            return false;
        }

        auto zoneNames = MetaServiceUtils::parseZoneNames(std::move(groupValue));
        auto iter = std::find(zoneNames.begin(), zoneNames.end(), zoneName);
        if (isAdd) {
            if (iter != zoneNames.end()) {
                LOG(ERROR) << "Zone " << zoneName << " have existed";
                return false;
            }
            zoneNames.emplace_back(std::move(zoneName));
        } else {
            if (iter == zoneNames.end()) {
                LOG(ERROR) << "Zone " << zoneName << " not existed";
                return false;
            }
            zoneNames.erase(iter);
        }

        std::vector<kvstore::KV> data;
        data.emplace_back(std::move(groupKey), MetaServiceUtils::groupVal(zoneNames));

        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, 0, std::move(data),
                          [&] (nebula::cpp2::ErrorCode code) {
                              ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
                              baton.post();
                          });
        baton.wait();
        return true;
    }

    static void assembleSpace(kvstore::KVStore* kv, GraphSpaceID id, int32_t partitionNum,
                              int32_t replica = 1, int32_t totalHost = 1) {
        // mock the part distribution like create space
        cpp2::SpaceDesc properties;
        properties.set_space_name("test_space");
        properties.set_partition_num(partitionNum);
        properties.set_replica_factor(replica);
        auto spaceVal = MetaServiceUtils::spaceVal(properties);
        std::vector<nebula::kvstore::KV> data;
        data.emplace_back(MetaServiceUtils::indexSpaceKey("test_space"),
                          std::string(reinterpret_cast<const char*>(&id), sizeof(GraphSpaceID)));
        data.emplace_back(MetaServiceUtils::spaceKey(id), MetaServiceUtils::spaceVal(properties));

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
            data.emplace_back(MetaServiceUtils::partKey(id, partId),
                              MetaServiceUtils::partVal(hosts));
        }
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, 0, std::move(data),
                          [&] (nebula::cpp2::ErrorCode code) {
                              ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
                              baton.post();
                          });
        baton.wait();
    }

    static void mockTag(kvstore::KVStore* kv, int32_t tagNum,
                        SchemaVer version = 0, bool nullable = false) {
        std::vector<nebula::kvstore::KV> tags;
        SchemaVer ver = version;
        for (auto t = 0; t < tagNum; t++) {
            TagID tagId = t;
            cpp2::Schema srcsch;
            for (auto i = 0; i < 2; i++) {
                cpp2::ColumnDef column;
                column.set_name(folly::stringPrintf("tag_%d_col_%d", tagId, i));
                if (i < 1) {
                    column.type.set_type(PropertyType::INT64);
                } else {
                    column.type.set_type(PropertyType::FIXED_STRING);
                    column.type.set_type_length(MAX_INDEX_TYPE_LENGTH);
                }
                if (nullable) {
                    column.set_nullable(nullable);
                }
                (*srcsch.columns_ref()).emplace_back(std::move(column));
            }
            auto tagName = folly::stringPrintf("tag_%d", tagId);
            auto tagIdVal = std::string(reinterpret_cast<const char*>(&tagId), sizeof(tagId));
            tags.emplace_back(MetaServiceUtils::indexTagKey(1, tagName), tagIdVal);
            tags.emplace_back(MetaServiceUtils::schemaTagKey(1, tagId, ver++),
                              MetaServiceUtils::schemaVal(tagName, srcsch));
        }
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, 0, std::move(tags),
                          [&] (nebula::cpp2::ErrorCode code) {
                              ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
                              baton.post();
                          });
        baton.wait();
    }

    static void mockTagIndex(kvstore::KVStore* kv,
                             TagID tagID, std::string tagName,
                             IndexID indexID, std::string indexName,
                             std::vector<cpp2::ColumnDef> columns) {
        GraphSpaceID space = 1;
        cpp2::IndexItem item;
        item.set_index_id(indexID);
        item.set_index_name(indexName);
        cpp2::SchemaID schemaID;
        schemaID.set_tag_id(tagID);
        item.set_schema_id(std::move(schemaID));
        item.set_schema_name(std::move(tagName));
        item.set_fields(std::move(columns));

        std::vector<nebula::kvstore::KV> data;
        data.emplace_back(MetaServiceUtils::indexIndexKey(space, indexName),
                          std::string(reinterpret_cast<const char*>(&indexID), sizeof(IndexID)));
        data.emplace_back(MetaServiceUtils::indexKey(space, indexID),
                          MetaServiceUtils::indexVal(item));
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, 0, std::move(data),
                          [&] (nebula::cpp2::ErrorCode code) {
                              ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
                              baton.post();
                          });
        baton.wait();
    }

    static void mockEdgeIndex(kvstore::KVStore* kv,
                              EdgeType edgeType, std::string edgeName,
                              IndexID indexID, std::string indexName,
                              std::vector<cpp2::ColumnDef> columns) {
        GraphSpaceID space = 1;
        cpp2::IndexItem item;
        item.set_index_id(indexID);
        item.set_index_name(indexName);
        cpp2::SchemaID schemaID;
        schemaID.set_edge_type(edgeType);
        item.set_schema_id(std::move(schemaID));
        item.set_schema_name(std::move(edgeName));
        item.set_fields(std::move(columns));

        std::vector<nebula::kvstore::KV> data;
        data.emplace_back(MetaServiceUtils::indexIndexKey(space, indexName),
                          std::string(reinterpret_cast<const char*>(&indexID), sizeof(IndexID)));
        data.emplace_back(MetaServiceUtils::indexKey(space, indexID),
                          MetaServiceUtils::indexVal(item));
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, 0, std::move(data),
                          [&] (nebula::cpp2::ErrorCode code) {
                              ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
                              baton.post();
                          });
        baton.wait();
    }

    static void mockEdge(kvstore::KVStore* kv, int32_t edgeNum,
                         SchemaVer version = 0, bool nullable = false) {
        std::vector<nebula::kvstore::KV> edges;
        SchemaVer ver = version;
        for (auto t = 0; t < edgeNum; t++) {
            EdgeType edgeType = t;
            cpp2::Schema srcsch;
            for (auto i = 0; i < 2; i++) {
                cpp2::ColumnDef column;
                column.name = folly::stringPrintf("edge_%d_col_%d", edgeType, i);
                if (i < 1) {
                    column.type.set_type(PropertyType::INT64);
                } else {
                    column.type.set_type(PropertyType::FIXED_STRING);
                    column.type.set_type_length(MAX_INDEX_TYPE_LENGTH);
                }
                if (nullable) {
                    column.set_nullable(nullable);
                }
                (*srcsch.columns_ref()).emplace_back(std::move(column));
            }
            auto edgeName = folly::stringPrintf("edge_%d", edgeType);
            auto edgeTypeVal = std::string(reinterpret_cast<const char*>(&edgeType),
                                           sizeof(edgeType));
            edges.emplace_back(MetaServiceUtils::indexEdgeKey(1, edgeName), edgeTypeVal);
            edges.emplace_back(MetaServiceUtils::schemaEdgeKey(1, edgeType, ver++),
                               MetaServiceUtils::schemaVal(edgeName, srcsch));
        }

        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, 0, std::move(edges), [&] (nebula::cpp2::ErrorCode code) {
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
            baton.post();
        });
        baton.wait();
    }

    static cpp2::Schema mockSchemaWithAllType() {
        cpp2::Schema schema;
        std::vector<nebula::meta::cpp2::ColumnDef> cols;
        cols.emplace_back(TestUtils::columnDef(0,
                    PropertyType::BOOL, Value(true), false));
        cols.emplace_back(TestUtils::columnDef(1,
                    PropertyType::INT8, Value(NullType::__NULL__), true));
        cols.emplace_back(TestUtils::columnDef(2,
                    PropertyType::INT16, Value(20), false));
        cols.emplace_back(TestUtils::columnDef(3,
                    PropertyType::INT32, Value(200), false));
        cols.emplace_back(TestUtils::columnDef(4,
                    PropertyType::INT64, Value(2000), false));
        cols.emplace_back(TestUtils::columnDef(5,
                    PropertyType::FLOAT, Value(10.0), false));
        cols.emplace_back(TestUtils::columnDef(6,
                    PropertyType::DOUBLE, Value(20.0), false));
        cols.emplace_back(TestUtils::columnDef(7,
                    PropertyType::STRING, Value("string"), false));
        cols.emplace_back(TestUtils::columnDef(8,
                    PropertyType::FIXED_STRING, Value("longlongstring"), false, 10));
        cols.emplace_back(TestUtils::columnDef(9,
                    PropertyType::TIMESTAMP, Value(123456), true));
        cols.emplace_back(TestUtils::columnDef(10,
                    PropertyType::DATE, Value(Date()), true));
        cols.emplace_back(TestUtils::columnDef(11,
                    PropertyType::DATETIME, Value(DateTime()), true));
        schema.set_columns(std::move(cols));
        return schema;
    }

    static void checkSchemaWithAllType(const cpp2::Schema &schema) {
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

    static bool verifySchema(cpp2::Schema &result,
                             cpp2::Schema &expected) {
        if (result.get_columns().size() != expected.get_columns().size()) {
            return false;
        }

        std::vector<cpp2::ColumnDef> resultColumns = result.get_columns();
        std::vector<cpp2::ColumnDef> expectedColumns = expected.get_columns();
        std::sort(resultColumns.begin(), resultColumns.end(),
                  [](cpp2::ColumnDef col0, cpp2::ColumnDef col1) {
                      return col0.get_name() < col1.get_name();
                  });
        std::sort(expectedColumns.begin(), expectedColumns.end(),
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

    static bool verifyMap(std::map<std::string, std::vector<std::string>> &result,
                          std::map<std::string, std::vector<std::string>> &expected) {
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

    static bool verifyMap(std::map<std::string, std::vector<cpp2::ColumnDef>> &result,
                          std::map<std::string, std::vector<cpp2::ColumnDef>> &expected) {
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

    static bool verifyResult(std::vector<std::string> &result,
                             std::vector<std::string> &expected) {
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

    static bool verifyResult(std::vector<cpp2::ColumnDef> &result,
                             std::vector<cpp2::ColumnDef> &expected) {
        if (result.size() != expected.size()) {
            return false;
        }

        std::sort(result.begin(), result.end(),
                  [](cpp2::ColumnDef& x, cpp2::ColumnDef& y) {
                      return x.get_name().compare(y.get_name());
                  });
        std::sort(expected.begin(), expected.end(),
                  [](cpp2::ColumnDef& x, cpp2::ColumnDef& y) {
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
