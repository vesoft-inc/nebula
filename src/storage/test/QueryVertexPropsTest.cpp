/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "kvstore/RocksEngineConfig.h"
#include "storage/test/TestUtils.h"
#include "storage/query/QueryVertexPropsProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore* kv,
              meta::SchemaManager* schemaMng,
              TagVersion version) {
    LOG(INFO) << "Prepare data...";
    for (PartitionID partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (int32_t vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            for (int32_t tagId = 3001; tagId < 3010; tagId++) {
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, version);
                auto schema = schemaMng->getTagSchema(0, tagId);
                RowWriter writer(schema);
                for (int64_t numInt = 0; numInt < 3; numInt++) {
                    writer << (numInt + version);
                }
                for (int32_t numString = 3; numString < 6; numString++) {
                    writer << folly::stringPrintf("tag_string_col_%ld", numString + version);
                }
                auto val = writer.encode();
                data.emplace_back(std::move(key), std::move(val));
            }
        }
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(
            0,
            partId,
            std::move(data),
            [&](kvstore::ResultCode code) {
                EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
                baton.post();
            });
        baton.wait();
        kvstore::ResultCode code = kv->flush(0);  // flush per partition
        EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
    }
}

void testWithVersion(kvstore::KVStore* kv,
                     meta::SchemaManager* schemaMng,
                     folly::CPUThreadPoolExecutor* executor,
                     TagVersion version) {
    {
        LOG(INFO) << "Build VertexPropsRequest...";
        cpp2::VertexPropRequest req;
        req.set_space_id(0);
        decltype(req.parts) tmpIds;
        std::unordered_set<VertexID> vids;
        for (auto partId = 0; partId < 3; partId++) {
            for (auto vertexId =  partId * 10;
                vertexId < (partId + 1) * 10;
                vertexId++) {
                tmpIds[partId].emplace_back(vertexId);
                vids.emplace(vertexId);
            }
        }
        req.set_parts(std::move(tmpIds));
        // Return tag props col_0, col_2, col_4
        decltype(req.return_columns) tmpColumns;
        for (int i = 0; i < 3; i++) {
            tmpColumns.emplace_back(TestUtils::vertexPropDef(
                folly::stringPrintf("tag_%d_col_%d", 3001 + i * 2, i * 2), 3001 + i * 2));
        }
        req.set_return_columns(std::move(tmpColumns));

        LOG(INFO) << "Test QueryVertexPropsRequest...";
        auto* processor = QueryVertexPropsProcessor::instance(kv,
                                                              schemaMng,
                                                              nullptr,
                                                              executor);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_codes.size());

        EXPECT_EQ(30, resp.vertices.size());

        auto* vschema = resp.get_vertex_schema();
        DCHECK(vschema != nullptr);

        for (auto& vp : resp.vertices) {
            auto vidFound = vids.find(vp.vertex_id);
            ASSERT_TRUE(vidFound != vids.end()) << "Vid: " << vp.vertex_id;

            auto size = std::accumulate(vp.tag_data.cbegin(), vp.tag_data.cend(), 0,
                                        [vschema](int acc, auto& td) {
                                            auto it = vschema->find(td.tag_id);
                                            DCHECK(it != vschema->end());
                                            return acc + it->second.columns.size();
                                        });

            EXPECT_EQ(3, size);

            checkTagData<int64_t>(vp.tag_data, 3001, "tag_3001_col_0", vschema, 0 + version);
            checkTagData<int64_t>(vp.tag_data, 3003, "tag_3003_col_2", vschema, 2 + version);
            checkTagData<std::string>(vp.tag_data, 3005, "tag_3005_col_4", vschema,
                                    folly::stringPrintf("tag_string_col_%ld", 4 + version));
        }
    }
    {
        LOG(INFO) << "Build VertexPropsRequest...";
        cpp2::VertexPropRequest req;
        req.set_space_id(0);
        decltype(req.parts) tmpIds;
        std::unordered_set<VertexID> vids;
        for (auto partId = 0; partId < 3; partId++) {
            for (auto vertexId =  partId * 10;
                vertexId < (partId + 1) * 10;
                vertexId++) {
                tmpIds[partId].emplace_back(vertexId);
                vids.emplace(vertexId);
            }
        }
        req.set_parts(std::move(tmpIds));

        LOG(INFO) << "Test QueryVertexPropsRequest...";
        auto* processor = QueryVertexPropsProcessor::instance(kv,
                                                              schemaMng,
                                                              nullptr,
                                                              executor);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_codes.size());

        EXPECT_EQ(30, resp.vertices.size());

        for (auto& vp : resp.vertices) {
            auto vidFound = vids.find(vp.vertex_id);
            ASSERT_TRUE(vidFound != vids.end()) << "Vid: " << vp.vertex_id;

            auto schema = schemaMng->getTagSchema(0, 3001, 0);
            ASSERT_NE(nullptr, schema);
            checkTagData<int64_t>(vp.tag_data, 3001, "tag_3001_col_0", schema, 0 + version);

            schema = schemaMng->getTagSchema(0, 3003, 0);
            ASSERT_NE(nullptr, schema);
            checkTagData<int64_t>(vp.tag_data, 3003, "tag_3003_col_2", schema, 2 + version);

            schema = schemaMng->getTagSchema(0, 3005, 0);
            ASSERT_NE(nullptr, schema);
            checkTagData<std::string>(vp.tag_data, 3005, "tag_3005_col_4", schema,
                                      folly::stringPrintf("tag_string_col_%ld", 4 + version));
        }
    }
}

TEST(QueryVertexPropsTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/QueryVertexPropsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMng = TestUtils::mockSchemaMan();

    auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(3);
    TagVersion version = 1;
    mockData(kv.get(), schemaMng.get(), version);
    testWithVersion(kv.get(), schemaMng.get(), executor.get(), version);

    version = 0;
    mockData(kv.get(), schemaMng.get(), version);
    testWithVersion(kv.get(), schemaMng.get(), executor.get(), version);
}

TEST(QueryVertexPropsTest, PrefixBloomFilterTest) {
    FLAGS_enable_rocksdb_statistics = true;
    FLAGS_enable_prefix_filtering = true;
    fs::TempDir rootPath("/tmp/QueryVertexPropsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMng = TestUtils::mockSchemaMan();
    auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(3);
    TagVersion version = 1;
    mockData(kv.get(), schemaMng.get(), version);
    testWithVersion(kv.get(), schemaMng.get(), executor.get(), version);
    std::shared_ptr<rocksdb::Statistics> statistics = kvstore::getDBStatistics();
    ASSERT_TRUE(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_CHECKED) > 0);
    ASSERT_TRUE(statistics->getTickerCount(rocksdb::Tickers::BLOOM_FILTER_PREFIX_USEFUL) > 0);
}

TEST(QueryVertexPropsTest, QueryAfterTagAltered) {
    fs::TempDir rootPath("/tmp/QueryVertexPropsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(3);

    LOG(INFO) << "Prepare meta...";
    auto schemaMng = TestUtils::mockSchemaMan();

    auto spaceId = 0;
    TagVersion version = std::numeric_limits<int64_t>::max() / 2 /* in case of overflow */;

    {
        LOG(INFO) << "Prepare data...";
        mockData(kv.get(), schemaMng.get(), version);

        LOG(INFO) << "Alter the tag schema with adding a prop.";
        auto schemaVer = 1;
        for (auto tagId = 3001; tagId < 3010; tagId++) {
            auto schema = schemaMng->getTagSchema(spaceId, tagId);
            auto iter = schema->begin();
            auto schemaWriter = std::make_shared<SchemaWriter>(schemaVer);
            while (iter) {
                auto* name = iter->getName();
                auto type = iter->getType();
                schemaWriter->appendCol(name, std::move(type));
                ++iter;
            }
            nebula::cpp2::ValueType type;
            type.type = nebula::cpp2::SupportedType::STRING;
            schemaWriter->appendCol("AddedProp", std::move(type));
            schemaMng->addTagSchema(spaceId, tagId, schemaWriter, schemaVer);
        }

        LOG(INFO) << "Build VertexPropsRequest...";
        cpp2::VertexPropRequest req;
        req.set_space_id(spaceId);
        decltype(req.parts) tmpIds;
        for (auto partId = 0; partId < 3; partId++) {
            for (auto vertexId =  partId * 10;
                vertexId < (partId + 1) * 10;
                vertexId++) {
                tmpIds[partId].emplace_back(vertexId);
            }
        }
        req.set_parts(std::move(tmpIds));
        // Return tag props col_0, col_2, col_4
        decltype(req.return_columns) tmpColumns;
        for (int i = 0; i < 3; i++) {
            tmpColumns.emplace_back(TestUtils::vertexPropDef(
                folly::stringPrintf("tag_%d_col_%d", 3001 + i * 2, i * 2), 3001 + i * 2));
            tmpColumns.emplace_back(TestUtils::vertexPropDef("AddedProp", 3001 + i * 2));
        }
        req.set_return_columns(std::move(tmpColumns));

        LOG(INFO) << "Test QueryVertexPropsRequest...";
        auto* processor = QueryVertexPropsProcessor::instance(kv.get(),
                                                              schemaMng.get(),
                                                              nullptr,
                                                              executor.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_codes.size());

        EXPECT_EQ(30, resp.vertices.size());

        auto* vschema = resp.get_vertex_schema();
        DCHECK(vschema != nullptr);

        for (auto& vp : resp.vertices) {
            auto size = std::accumulate(vp.tag_data.cbegin(), vp.tag_data.cend(), 0,
                                        [vschema](int acc, auto& td) {
                                            auto it = vschema->find(td.tag_id);
                                            DCHECK(it != vschema->end());
                                            return acc + it->second.columns.size();
                                        });

            EXPECT_EQ(6, size);

            checkTagData<int64_t>(vp.tag_data, 3001, "tag_3001_col_0", vschema, 0 + version);
            checkTagData<std::string>(vp.tag_data, 3001, "AddedProp", vschema, "");
            checkTagData<int64_t>(vp.tag_data, 3003, "tag_3003_col_2", vschema, 2 + version);
            checkTagData<std::string>(vp.tag_data, 3003, "AddedProp", vschema, "");
            checkTagData<std::string>(vp.tag_data, 3005, "tag_3005_col_4", vschema,
                                      folly::stringPrintf("tag_string_col_%ld", 4 + version));
            checkTagData<std::string>(vp.tag_data, 3005, "AddedProp", vschema, "");
        }
    }

    version = std::numeric_limits<int64_t>::max() - 100;
    {
        LOG(INFO) << "Now update the data with new tag prop.";
        for (auto partId = 0; partId < 3; partId++) {
            std::vector<kvstore::KV> data;
            for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
                for (auto tagId = 3001; tagId < 3010; tagId++) {
                    auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, version);
                    auto schema = schemaMng->getTagSchema(spaceId, tagId);
                    RowWriter writer(schema);
                    for (int64_t numInt = 0; numInt < 3; numInt++) {
                        writer << (numInt + version);
                    }
                    for (auto numString = 3; numString < 6; numString++) {
                        writer << folly::stringPrintf("tag_string_col_%ld", numString + version);
                    }
                    writer << "AddedPropValue";
                    auto val = writer.encode();
                    data.emplace_back(std::move(key), std::move(val));
                }
            }
            folly::Baton<true, std::atomic> baton;
            kv->asyncMultiPut(
                0,
                partId,
                std::move(data),
                [&](kvstore::ResultCode code) {
                    EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
                    baton.post();
                });
            baton.wait();
        }

        LOG(INFO) << "Build VertexPropsRequest...";
        cpp2::VertexPropRequest req;
        req.set_space_id(0);
        decltype(req.parts) tmpIds;
        for (auto partId = 0; partId < 3; partId++) {
            for (auto vertexId =  partId * 10;
                vertexId < (partId + 1) * 10;
                vertexId++) {
                tmpIds[partId].emplace_back(vertexId);
            }
        }
        req.set_parts(std::move(tmpIds));
        // Return tag props col_0, col_2, col_4
        decltype(req.return_columns) tmpColumns;
        for (int i = 0; i < 3; i++) {
            tmpColumns.emplace_back(TestUtils::vertexPropDef(
                folly::stringPrintf("tag_%d_col_%d", 3001 + i * 2, i * 2), 3001 + i * 2));
            tmpColumns.emplace_back(TestUtils::vertexPropDef("AddedProp", 3001 + i * 2));
        }
        req.set_return_columns(std::move(tmpColumns));

        LOG(INFO) << "Test QueryVertexPropsRequest...";
        auto* processor = QueryVertexPropsProcessor::instance(kv.get(),
                                                              schemaMng.get(),
                                                              nullptr,
                                                              executor.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_codes.size());

        EXPECT_EQ(30, resp.vertices.size());

        auto* vschema = resp.get_vertex_schema();
        DCHECK(vschema != nullptr);

        for (auto& vp : resp.vertices) {
            auto size = std::accumulate(vp.tag_data.cbegin(), vp.tag_data.cend(), 0,
                                        [vschema](int acc, auto& td) {
                                            auto it = vschema->find(td.tag_id);
                                            DCHECK(it != vschema->end());
                                            return acc + it->second.columns.size();
                                        });

            EXPECT_EQ(6, size);

            checkTagData<int64_t>(vp.tag_data, 3001, "tag_3001_col_0", vschema, 0 + version);
            checkTagData<std::string>(vp.tag_data, 3001, "AddedProp", vschema, "AddedPropValue");
            checkTagData<int64_t>(vp.tag_data, 3003, "tag_3003_col_2", vschema, 2 + version);
            checkTagData<std::string>(vp.tag_data, 3003, "AddedProp", vschema, "AddedPropValue");
            checkTagData<std::string>(vp.tag_data, 3005, "tag_3005_col_4", vschema,
                                      folly::stringPrintf("tag_string_col_%ld", 4 + version));
            checkTagData<std::string>(vp.tag_data, 3005, "AddedProp", vschema, "AddedPropValue");
        }
    }
    {
        LOG(INFO) << "Alter the tag schema with altering the prop type.";
        auto schemaVer = 2;
        for (auto tagId = 3001; tagId < 3010; tagId++) {
            auto schema = schemaMng->getTagSchema(spaceId, tagId, 0);
            auto iter = schema->begin();
            auto schemaWriter = std::make_shared<SchemaWriter>(schemaVer);
            while (iter) {
                auto* name = iter->getName();
                auto type = iter->getType();
                schemaWriter->appendCol(name, std::move(type));
                ++iter;
            }
            nebula::cpp2::ValueType type;
            type.type = nebula::cpp2::SupportedType::INT;
            schemaWriter->appendCol("AddedProp", std::move(type));
            schemaMng->addTagSchema(spaceId, tagId, schemaWriter, schemaVer);
        }

        LOG(INFO) << "Build VertexPropsRequest...";
        cpp2::VertexPropRequest req;
        req.set_space_id(spaceId);
        decltype(req.parts) tmpIds;
        for (auto partId = 0; partId < 3; partId++) {
            for (auto vertexId =  partId * 10;
                vertexId < (partId + 1) * 10;
                vertexId++) {
                tmpIds[partId].emplace_back(vertexId);
            }
        }
        req.set_parts(std::move(tmpIds));
        // Return tag props col_0, col_2, col_4
        decltype(req.return_columns) tmpColumns;
        for (int i = 0; i < 3; i++) {
            tmpColumns.emplace_back(TestUtils::vertexPropDef(
                folly::stringPrintf("tag_%d_col_%d", 3001 + i * 2, i * 2), 3001 + i * 2));
            tmpColumns.emplace_back(TestUtils::vertexPropDef("AddedProp", 3001 + i * 2));
        }
        req.set_return_columns(std::move(tmpColumns));

        LOG(INFO) << "Test QueryVertexPropsRequest...";
        auto* processor = QueryVertexPropsProcessor::instance(kv.get(),
                                                            schemaMng.get(),
                                                            nullptr,
                                                            executor.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_codes.size());

        EXPECT_EQ(30, resp.vertices.size());

        auto* vschema = resp.get_vertex_schema();
        DCHECK(vschema != nullptr);

        for (auto& vp : resp.vertices) {
            auto size = std::accumulate(vp.tag_data.cbegin(), vp.tag_data.cend(), 0,
                                        [vschema](int acc, auto& td) {
                                            auto it = vschema->find(td.tag_id);
                                            DCHECK(it != vschema->end());
                                            return acc + it->second.columns.size();
                                        });

            EXPECT_EQ(6, size);

            checkTagData<int64_t>(vp.tag_data, 3001, "tag_3001_col_0", vschema, 0 + version);
            checkTagData<int64_t>(vp.tag_data, 3001, "AddedProp", vschema, 0);
            checkTagData<int64_t>(vp.tag_data, 3003, "tag_3003_col_2", vschema, 2 + version);
            checkTagData<int64_t>(vp.tag_data, 3003, "AddedProp", vschema, 0);
            checkTagData<std::string>(vp.tag_data, 3005, "tag_3005_col_4", vschema,
                                    folly::stringPrintf("tag_string_col_%ld", 4 + version));
            checkTagData<int64_t>(vp.tag_data, 3005, "AddedProp", vschema, 0);
        }
    }
}

TEST(QueryVertexPropsTest, TTLTest) {
    fs::TempDir rootPath("/tmp/QueryVertexPropsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaWithTTLMan();

    LOG(INFO) << "Prepare data...";
    for (auto partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            auto tagId = 3001;
            auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, 0);
            RowWriter writer;
            for (int64_t numInt = 0; numInt < 3; numInt++) {
                // all data expired, 1546272000 timestamp representation "2019-1-1 0:0:0"
                writer << numInt + 1546272000;
            }
            for (auto numString = 3; numString < 6; numString++) {
                writer << folly::stringPrintf("tag_string_col_%d", numString);
            }
            auto val = writer.encode();
            data.emplace_back(std::move(key), std::move(val));
        }

        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(
            0,
            partId,
            std::move(data),
            [&](kvstore::ResultCode code) {
                EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
                baton.post();
            });
        baton.wait();
    }

    LOG(INFO) << "Build VertexPropsRequest...";
    cpp2::VertexPropRequest req;
    req.set_space_id(0);
    decltype(req.parts) tmpIds;
    for (auto partId = 0; partId < 3; partId++) {
        for (auto vertexId =  partId * 10;
             vertexId < (partId + 1) * 10;
             vertexId++) {
            tmpIds[partId].push_back(vertexId);
        }
    }
    req.set_parts(std::move(tmpIds));
    // Return tag props col_0
    decltype(req.return_columns) tmpColumns;
    tmpColumns.emplace_back(TestUtils::vertexPropDef(
        folly::stringPrintf("tag_%d_col_%d", 3001, 0), 3001));
    req.set_return_columns(std::move(tmpColumns));

    LOG(INFO) << "Test QueryVertexPropsRequest...";
    auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(3);
    auto* processor = QueryVertexPropsProcessor::instance(kv.get(),
                                                          schemaMan.get(),
                                                          nullptr,
                                                          executor.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, resp.result.failed_codes.size());

    EXPECT_EQ(30, resp.vertices.size());

    auto* vschema = resp.get_vertex_schema();
    DCHECK(vschema != nullptr);

    for (auto& vp : resp.vertices) {
        auto it =  std::find_if(vp.tag_data.cbegin(), vp.tag_data.cend(), [](auto& td) {
            if (td.tag_id == 3001) {
                return true;
            }
            return false;
        });

        DCHECK(it == vp.tag_data.cend());
        auto it2  = vschema->find(3001);
        DCHECK(it2 != vschema->end());
    }
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
