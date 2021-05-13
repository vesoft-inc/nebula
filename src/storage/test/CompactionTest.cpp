/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/IndexKeyUtils.h"
#include <gtest/gtest.h>
#include "storage/CommonUtils.h"
#include "storage/test/QueryTestUtils.h"
#include "storage/test/TestUtils.h"
#include "codec/RowWriterV2.h"
#include "mock/AdHocSchemaManager.h"
#include "mock/AdHocIndexManager.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"


namespace nebula {
namespace storage {

// statis tag record count, can distinguish multiple versions
void checkTagVertexData(int32_t spaceVidLen,
                        GraphSpaceID spaceId,
                        TagID tagId,
                        int parts,
                        StorageEnv* env,
                        int expectNum,
                        bool distinguishMultiVer = false) {
    int totalCount = 0;
    // partId start 1
    std::unique_ptr<kvstore::KVIterator> iter;
    VertexID lastVertexId = "";

    for (int part = 1; part <= parts; part++) {
        auto prefix = NebulaKeyUtils::vertexPrefix(part);
        auto ret = env->kvstore_->prefix(spaceId, part, prefix, &iter);
        ASSERT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);

        while (iter && iter->valid()) {
            auto key = iter->key();
            auto tId = NebulaKeyUtils::getTagId(spaceVidLen, key);
            if (tId == tagId) {
                if (distinguishMultiVer) {
                    auto vId = NebulaKeyUtils::getVertexId(spaceVidLen, key).str();
                    if (lastVertexId == vId) {
                        // mutil version
                    } else {
                        lastVertexId  = vId;
                        totalCount++;
                    }
                 } else {
                    totalCount++;
                 }
            }
            iter->next();
        }
    }

    ASSERT_EQ(expectNum, totalCount);
}

// statis edge record count, can distinguish multiple versions
void checkEdgeData(int32_t spaceVidLen,
                   GraphSpaceID spaceId,
                   EdgeType type,
                   int parts,
                   StorageEnv* env,
                   int expectNum,
                   bool distinguishMultiVer = false) {
    int totalCount = 0;
    // partId start 1
    std::unique_ptr<kvstore::KVIterator> iter;
    VertexID    lastSrcVertexId = "";
    VertexID    lastDstVertexId = "";
    EdgeRanking lastRank = 0;

    for (int part = 1; part <= parts; part++) {
        auto prefix = NebulaKeyUtils::edgePrefix(part);
        auto ret = env->kvstore_->prefix(spaceId, part, prefix, &iter);
        ASSERT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);

        while (iter && iter->valid()) {
            auto key = iter->key();
            if (NebulaKeyUtils::isEdge(spaceVidLen, key)) {
                auto eType = NebulaKeyUtils::getEdgeType(spaceVidLen, key);
                if (eType == type) {
                    if (distinguishMultiVer) {
                        auto source = NebulaKeyUtils::getSrcId(spaceVidLen, key).str();
                        auto ranking = NebulaKeyUtils::getRank(spaceVidLen, key);
                        auto destination = NebulaKeyUtils::getDstId(spaceVidLen, key).str();
                        if (source == lastSrcVertexId &&
                            ranking == lastRank &&
                            destination == lastDstVertexId) {
                            // Multi version
                        } else {
                            lastSrcVertexId = source;
                            lastRank = ranking;
                            lastDstVertexId = destination;
                            totalCount++;
                        }
                    } else {
                        totalCount++;
                    }
                }
            }
            iter->next();
        }
    }

    ASSERT_EQ(expectNum, totalCount);
}

// statis index record count
void checkIndexData(GraphSpaceID spaceId,
                    IndexID indexId,
                    int parts,
                    StorageEnv* env,
                    int expectNum) {
    int totalCount = 0;
    // partId start 1
    std::unique_ptr<kvstore::KVIterator> iter;

    for (int part = 1; part <= parts; part++) {
        auto prefix = IndexKeyUtils::indexPrefix(part, indexId);
        auto ret = env->kvstore_->prefix(spaceId, part, prefix, &iter);
        ASSERT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);

        while (iter && iter->valid()) {
            totalCount++;
            iter->next();
        }
    }

    ASSERT_EQ(expectNum, totalCount);
}

TEST(CompactionFilterTest, InvalidSchemaFilterTest) {
    fs::TempDir rootPath("/tmp/CompactionFilterTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), HostAddr("", 0),
                          1, true, false, {}, true);

    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    // remove players data, tagId is 1
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, parts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, parts));

    LOG(INFO) << "Before compaction, check data...";
    // check players data, data count is 0
    checkTagVertexData(spaceVidLen, spaceId, tagId, parts, env, 51);
    // check teams data, data count is 30
    checkTagVertexData(spaceVidLen, spaceId, 2, parts, env, 30);

    // check serve positive data, data count is 167
    checkEdgeData(spaceVidLen, spaceId, 101, parts, env, 167);
    // check teammates positive data, data count is 18
    checkEdgeData(spaceVidLen, spaceId, 102, parts, env, 18);

    LOG(INFO) << "Let's delete one tag";
    auto* adhoc = dynamic_cast<mock::AdHocSchemaManager*>(env->schemaMan_);
    adhoc->removeTagSchema(spaceId, tagId);

    LOG(INFO) << "Do compaction";
    auto* ns = dynamic_cast<kvstore::NebulaStore*>(env->kvstore_);
    ns->compact(spaceId);

    LOG(INFO) << "Finish compaction, check data...";
    // check players data, data count is 0
    checkTagVertexData(spaceVidLen, spaceId, tagId, parts, env, 0);
    // check teams data, data count is 30
    checkTagVertexData(spaceVidLen, spaceId, 2, parts, env, 30);

    // check serve positive data, data count is 167
    checkEdgeData(spaceVidLen, spaceId, 101, parts, env, 167);
    // check teammates positive data, data count is 18
    checkEdgeData(spaceVidLen, spaceId, 102, parts, env, 18);
}

TEST(CompactionFilterTest, TTLFilterDataExpiredTest) {
    FLAGS_mock_ttl_col = true;
    FLAGS_mock_ttl_duration = 1;

    fs::TempDir rootPath("/tmp/CompactionFilterTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), HostAddr("", 0),
                          1, true, false, {}, true);
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, parts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, parts));

    LOG(INFO) << "Before compaction, check data...";
    // check players data, data count is 51
    checkTagVertexData(spaceVidLen, spaceId, 1, parts, env, 51);
    // check teams data, data count is 30
    checkTagVertexData(spaceVidLen, spaceId, 2, parts, env, 30);

    // check serve positive data, data count is 167
    checkEdgeData(spaceVidLen, spaceId, 101, parts, env, 167);
    // check teammates positive data, data count is 18
    checkEdgeData(spaceVidLen, spaceId, 102, parts, env, 18);

    // wait ttl data Expire
    sleep(FLAGS_mock_ttl_duration + 1);

    LOG(INFO) << "Do compaction";
    auto* ns = dynamic_cast<kvstore::NebulaStore*>(env->kvstore_);
    ns->compact(spaceId);

    LOG(INFO) << "Finish compaction, check data...";
    // check players data, data count is 0
    checkTagVertexData(spaceVidLen, spaceId, 1, parts, env, 0);
    // check teams data, data count is 30
    checkTagVertexData(spaceVidLen, spaceId, 2, parts, env, 30);

    // check serve positive data, data count is 0
    checkEdgeData(spaceVidLen, spaceId, 101, parts, env, 0);
    // check teammates positive data, data count is 18
    checkEdgeData(spaceVidLen, spaceId, 102, parts, env, 18);

    FLAGS_mock_ttl_col = false;
}


TEST(CompactionFilterTest, TTLFilterDataNotExpiredTest) {
    FLAGS_mock_ttl_col = true;
    FLAGS_mock_ttl_duration = 1800;

    fs::TempDir rootPath("/tmp/CompactionFilterTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), HostAddr("", 0),
                          1, true, false, {}, true);
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, parts));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, parts));

    LOG(INFO) << "Before compaction, check data...";
    // check players data, data count is 51
    checkTagVertexData(spaceVidLen, spaceId, 1, parts, env, 51);
    // check teams data, data count is 30
    checkTagVertexData(spaceVidLen, spaceId, 2, parts, env, 30);

    // check serve positive data, data count is 167
    checkEdgeData(spaceVidLen, spaceId, 101, parts, env, 167);
    // check teammates positive data, data count is 18
    checkEdgeData(spaceVidLen, spaceId, 102, parts, env, 18);

    LOG(INFO) << "Do compaction";
    auto* ns = dynamic_cast<kvstore::NebulaStore*>(env->kvstore_);
    ns->compact(spaceId);

    LOG(INFO) << "Finish compaction, check data...";
    // check players data, data count is 51
    checkTagVertexData(spaceVidLen, spaceId, 1, parts, env, 51);
    // check teams data, data count is 30
    checkTagVertexData(spaceVidLen, spaceId, 2, parts, env, 30);

    // check serve positive data, data count is 167
    checkEdgeData(spaceVidLen, spaceId, 101, parts, env, 167);
    // check teammates positive data, data count is 18
    checkEdgeData(spaceVidLen, spaceId, 102, parts, env, 18);

    FLAGS_mock_ttl_col = false;
}

TEST(CompactionFilterTest, DropIndexTest) {
    fs::TempDir rootPath("/tmp/CompactionFilterTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), HostAddr("", 0),
                          1, true, false, {}, true);
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    // Add tag/edge data and index data
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, parts, true));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, parts, true));

    LOG(INFO) << "Before compaction, check data...";
    // check players data, data count is 51
    checkTagVertexData(spaceVidLen, spaceId, 1, parts, env, 51);
    // check teams data, data count is 30
    checkTagVertexData(spaceVidLen, spaceId, 2, parts, env, 30);

    // check serve positive data, data count is 167
    checkEdgeData(spaceVidLen, spaceId, 101, parts, env, 167);
    // check teammates positive data, data count is 18
    checkEdgeData(spaceVidLen, spaceId, 102, parts, env, 18);

    // check player indexId 1 data
    checkIndexData(spaceId, 1, 6, env, 51);
    // check teams indexId 2 data
    checkIndexData(spaceId, 2, 6, env, 30);

    // check serve indexId 101 data
    checkIndexData(spaceId, 101, 6, env, 167);
    // check teammates indexId 102 data
    checkIndexData(spaceId, 102, 6, env, 18);

    // drop player indexId 1
    LOG(INFO) << "Let's delete one tag index";
    IndexID indexId = 1;
    auto* adIndex = dynamic_cast<mock::AdHocIndexManager*>(env->indexMan_);
    adIndex->removeTagIndex(spaceId, indexId);

    LOG(INFO) << "Do compaction";
    auto* ns = dynamic_cast<kvstore::NebulaStore*>(env->kvstore_);
    ns->compact(spaceId);

    LOG(INFO) << "Finish compaction, check index data...";
    // check players data, data count is 51
    checkTagVertexData(spaceVidLen, spaceId, 1, parts, env, 51);
    // check teams data, data count is 30
    checkTagVertexData(spaceVidLen, spaceId, 2, parts, env, 30);

    // check serve positive data, data count is 167
    checkEdgeData(spaceVidLen, spaceId, 101, parts, env, 167);
    // check teammates positive data, data count is 18
    checkEdgeData(spaceVidLen, spaceId, 102, parts, env, 18);

    // check player indexId 1 data
    checkIndexData(spaceId, 1, 6, env, 0);
    // check teams indexId 2 data
    checkIndexData(spaceId, 2, 6, env, 30);

    // check serve indexId 101 data
    checkIndexData(spaceId, 101, 6, env, 167);
    // check teammates indexId 102 data
    checkIndexData(spaceId, 102, 6, env, 18);
}

TEST(CompactionFilterTest, TTLFilterDataIndexExpiredTest) {
    FLAGS_mock_ttl_col = true;
    FLAGS_mock_ttl_duration = 1;

    fs::TempDir rootPath("/tmp/TTLFilterDataIndexExpiredTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), HostAddr("", 0),
                          1, true, false, {}, true);
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    // Add tag/edge data and index data
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, parts, true));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, parts, true));

    LOG(INFO) << "Before compaction, check data...";
    // check players data, data count is 51
    checkTagVertexData(spaceVidLen, spaceId, 1, parts, env, 51);
    // check teams data, data count is 30
    checkTagVertexData(spaceVidLen, spaceId, 2, parts, env, 30);

    // check serve positive data, data count is 167
    checkEdgeData(spaceVidLen, spaceId, 101, parts, env, 167);
    // check teammates positive data, data count is 18
    checkEdgeData(spaceVidLen, spaceId, 102, parts, env, 18);

    // check player indexId 1 data
    checkIndexData(spaceId, 1, 6, env, 51);
    // check teams indexId 2 data
    checkIndexData(spaceId, 2, 6, env, 30);

    // check serve indexId 101 data
    checkIndexData(spaceId, 101, 6, env, 167);
    // check teammates indexId 102 data
    checkIndexData(spaceId, 102, 6, env, 18);

    // wait ttl data Expire
    sleep(FLAGS_mock_ttl_duration + 1);

    LOG(INFO) << "Do compaction";
    auto* ns = dynamic_cast<kvstore::NebulaStore*>(env->kvstore_);
    ns->compact(spaceId);

    LOG(INFO) << "Finish compaction, check index data...";
    // check players data, data count is 0
    checkTagVertexData(spaceVidLen, spaceId, 1, parts, env, 0);
    // check teams data, data count is 30
    checkTagVertexData(spaceVidLen, spaceId, 2, parts, env, 30);

    // check serve positive data, data count is 0
    checkEdgeData(spaceVidLen, spaceId, 101, parts, env, 0);
    // check teammates positive data, data count is 18
    checkEdgeData(spaceVidLen, spaceId, 102, parts, env, 18);

    // check player indexId 1 data
    checkIndexData(spaceId, 1, 6, env, 0);
    // check teams indexId 2 data
    checkIndexData(spaceId, 2, 6, env, 30);

    // check serve indexId 101 data
    checkIndexData(spaceId, 101, 6, env, 0);
    // check teammates indexId 102 data
    checkIndexData(spaceId, 102, 6, env, 18);

    FLAGS_mock_ttl_col = false;
}

TEST(CompactionFilterTest, TTLFilterDataIndexNotExpiredTest) {
    FLAGS_mock_ttl_col = true;
    FLAGS_mock_ttl_duration = 1800;

    fs::TempDir rootPath("/tmp/TTLFilterDataIndexNotExpiredTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path(), HostAddr("", 0),
                          1, true, false, {}, true);
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    // Add tag/edge data and index data
    ASSERT_TRUE(QueryTestUtils::mockVertexData(env, parts, true));
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, parts, true));

    LOG(INFO) << "Before compaction, check data...";
    // check players data, data count is 51
    checkTagVertexData(spaceVidLen, spaceId, 1, parts, env, 51);
    // check teams data, data count is 30
    checkTagVertexData(spaceVidLen, spaceId, 2, parts, env, 30);

    // check serve positive data, data count is 167
    checkEdgeData(spaceVidLen, spaceId, 101, parts, env, 167);
    // check teammates positive data, data count is 18
    checkEdgeData(spaceVidLen, spaceId, 102, parts, env, 18);

    // check player indexId 1 data
    checkIndexData(spaceId, 1, 6, env, 51);
    // check teams indexId 2 data
    checkIndexData(spaceId, 2, 6, env, 30);

    // check serve indexId 101 data
    checkIndexData(spaceId, 101, 6, env, 167);
    // check teammates indexId 102 data
    checkIndexData(spaceId, 102, 6, env, 18);

    LOG(INFO) << "Do compaction";
    auto* ns = dynamic_cast<kvstore::NebulaStore*>(env->kvstore_);
    ns->compact(spaceId);

    LOG(INFO) << "Finish compaction, check index data...";
    // check players data, data count is 51
    checkTagVertexData(spaceVidLen, spaceId, 1, parts, env, 51);
    // check teams data, data count is 30
    checkTagVertexData(spaceVidLen, spaceId, 2, parts, env, 30);

    // check serve positive data, data count is 167
    checkEdgeData(spaceVidLen, spaceId, 101, parts, env, 167);
    // check teammates positive data, data count is 18
    checkEdgeData(spaceVidLen, spaceId, 102, parts, env, 18);

    // check player indexId 1 data
    checkIndexData(spaceId, 1, 6, env, 51);
    // check teams indexId 2 data
    checkIndexData(spaceId, 2, 6, env, 30);

    // check serve indexId 101 data
    checkIndexData(spaceId, 101, 6, env, 167);
    // check teammates indexId 102 data
    checkIndexData(spaceId, 102, 6, env, 18);

    FLAGS_mock_ttl_col = false;
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

