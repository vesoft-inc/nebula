/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TEST_TESTUTILS_H_
#define STORAGE_TEST_TESTUTILS_H_

#include "common/base/Base.h"
#include "codec/RowReaderWrapper.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/NebulaStore.h"
#include "utils/NebulaKeyUtils.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

void checkAddVerticesData(cpp2::AddVerticesRequest req,
                          StorageEnv* env,
                          int expectNum,
                          /* 0 not specify prop_names, 1 specify prop_names, 2 mix */
                          int mode = 0 ) {
    EXPECT_TRUE(mode == 0 || mode == 1 || mode == 2);
    auto spaceId = req.get_space_id();
    auto ret = env->schemaMan_->getSpaceVidLen(spaceId);
    EXPECT_TRUE(ret.ok());
    auto spaceVidLen = ret.value();

    int totalCount = 0;
    for (auto& part : *req.parts_ref()) {
        auto partId = part.first;
        auto newVertexVec = part.second;

        auto count = 0;
        for (auto& newVertex : newVertexVec) {
            auto vid = newVertex.get_id();
            auto newTagVec = *newVertex.tags_ref();

            for (auto& newTag : newTagVec) {
                auto tagId = newTag.get_tag_id();
                auto prefix =
                    NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vid.getStr(), tagId);
                std::unique_ptr<kvstore::KVIterator> iter;
                EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
                          env->kvstore_->prefix(spaceId, partId, prefix, &iter));

                auto schema = env->schemaMan_->getTagSchema(spaceId, tagId);
                EXPECT_TRUE(schema != NULL);

                int num = 0;
                while (iter && iter->valid()) {
                    auto reader = RowReaderWrapper::getRowReader(schema.get(), iter->val());
                    // For players tagId is 1
                    Value val;
                    if (mode == 0) {
                        if (tagId == 1) {
                            for (auto i = 0; i < 9; i++) {
                                val = reader->getValueByIndex(i);
                                EXPECT_EQ((*newTag.props_ref())[i], val);
                            }
                            if ((*newTag.props_ref()).size() >= 10) {
                                val = reader->getValueByIndex(9);
                                EXPECT_EQ((*newTag.props_ref())[9], val);
                                if ((*newTag.props_ref()).size() == 11) {
                                    val = reader->getValueByIndex(10);
                                    EXPECT_EQ((*newTag.props_ref())[10], val);
                                }
                            }
                        } else if (tagId == 2) {
                            // For teams tagId is 2
                            val = reader->getValueByIndex(0);
                            EXPECT_EQ((*newTag.props_ref())[0], val);
                        } else {
                            // Impossible to get here
                            ASSERT_TRUE(false);
                        }
                    } else if (mode == 1) {
                        if (tagId == 1) {
                            // For the specified attribute order, the default value and
                            // nullable columns always use the default value or null value
                            for (auto i = 0; i < 9; i++) {
                                val = reader->getValueByIndex(i);
                                EXPECT_EQ((*newTag.props_ref())[8 - i], val);
                            }
                        } else if (tagId == 2) {
                            // For teams tagId is 2
                            val = reader->getValueByIndex(0);
                            EXPECT_EQ((*newTag.props_ref())[0], val);
                        } else {
                            // Impossible to get here
                            ASSERT_TRUE(false);
                        }
                    } else {   // mode == 2
                        if (tagId == 1) {
                            if (num == 0) {
                                // For the specified attribute order, the default value and
                                // nullable columns always use the default value or null value
                                for (auto i = 0; i < 9; i++) {
                                    val = reader->getValueByIndex(i);
                                    EXPECT_EQ((*newTag.props_ref())[i], val);
                                }
                                val = reader->getValueByIndex(9);
                                EXPECT_EQ("America", val.getStr());
                            } else {
                                for (auto i = 0; i < 9; i++) {
                                    val = reader->getValueByIndex(i);
                                    EXPECT_EQ((*newTag.props_ref())[i], val);
                                }
                                if ((*newTag.props_ref()).size() >= 10) {
                                    val = reader->getValueByIndex(9);
                                    EXPECT_EQ((*newTag.props_ref())[9], val);
                                    if ((*newTag.props_ref()).size() == 11) {
                                        val = reader->getValueByIndex(10);
                                        EXPECT_EQ((*newTag.props_ref())[10], val);
                                    }
                                }
                            }
                        }

                        if (tagId == 2) {
                            // For teams tagId is 2
                            val = reader->getValueByIndex(0);
                            EXPECT_EQ((*newTag.props_ref())[0], val);
                        }
                    }
                    num++;
                    count++;
                    iter->next();
                }
            }
        }
        if (mode == 2) {
            EXPECT_EQ(newVertexVec.size(), count);
        } else {
            // There is only one tag per vertex, either tagId is 1 or tagId is 2
            EXPECT_EQ(newVertexVec.size(), count);
        }
        totalCount += count;
    }
    EXPECT_EQ(expectNum, totalCount);
}

void checkVerticesData(int32_t spaceVidLen,
                       GraphSpaceID spaceId,
                       std::unordered_map<PartitionID, std::vector<Value>> parts,
                       StorageEnv* env,
                       int expectNum) {
    int totalCount = 0;
    for (auto& part : parts) {
        auto partId = part.first;
        auto deleteVidVec = part.second;
        for (auto& vid : deleteVidVec) {
            auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vid.getStr());
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
                      env->kvstore_->prefix(spaceId, partId, prefix, &iter));

            while (iter && iter->valid()) {
                totalCount++;
                iter->next();
            }
        }
    }
    EXPECT_EQ(expectNum, totalCount);
}

void checkAddEdgesData(cpp2::AddEdgesRequest req,
                       StorageEnv* env,
                       int expectNum,
                       /* 0 not specify prop_names, 1 specify prop_names, 2 mix */
                       int mode = 0 ) {
    EXPECT_TRUE(mode == 0 || mode == 1 || mode == 2);
    auto spaceId = req.get_space_id();
    auto ret = env->schemaMan_->getSpaceVidLen(spaceId);
    EXPECT_TRUE(ret.ok());
    auto spaceVidLen = ret.value();

    int totalCount = 0;
    for (auto& part : *req.parts_ref()) {
        auto partId = part.first;
        auto newEdgeVec = part.second;
        for (auto& newEdge : newEdgeVec) {
            auto edgekey = newEdge.get_key();
            auto newEdgeProp = newEdge.get_props();

            auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen,
                                                     partId,
                                                     edgekey.get_src().getStr(),
                                                     edgekey.get_edge_type(),
                                                     edgekey.get_ranking(),
                                                     edgekey.get_dst().getStr());
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
                      env->kvstore_->prefix(spaceId, partId, prefix, &iter));

            auto schema = env->schemaMan_->getEdgeSchema(spaceId,
                    std::abs(edgekey.get_edge_type()));
            EXPECT_TRUE(schema != NULL);

            Value val;
            int num = 0;
            while (iter && iter->valid()) {
                auto reader = RowReaderWrapper::getRowReader(schema.get(), iter->val());
                if (mode == 0) {
                    for (auto i = 0; i < 7; i++) {
                        val = reader->getValueByIndex(i);
                        EXPECT_EQ(newEdgeProp[i], val);
                    }
                    if (newEdgeProp.size() >= 8) {
                        val = reader->getValueByIndex(7);
                        EXPECT_EQ(newEdgeProp[7], val);
                        if (newEdgeProp.size() == 9) {
                            val = reader->getValueByIndex(8);
                            EXPECT_EQ(newEdgeProp[8], val);
                        }
                    }
                } else if (mode == 1) {
                    // For the specified attribute order, the default value and nullable columns
                    // always use the default value or null value
                    for (auto i = 0; i < 7; i++) {
                        val = reader->getValueByIndex(i);
                        EXPECT_EQ(newEdgeProp[6 - i], val);
                    }
                } else {  // mode is 2
                    for (auto i = 0; i < 7; i++) {
                        val = reader->getValueByIndex(i);
                        EXPECT_EQ(newEdgeProp[i], val);
                    }
                    // When adding edge in specified Order, the last two columns
                    // use the default value and null
                    if (num == 0) {
                        val = reader->getValueByIndex(7);
                        EXPECT_EQ("trade", val.getStr());
                    } else {
                        if (newEdgeProp.size() >= 8) {
                            val = reader->getValueByIndex(7);
                            EXPECT_EQ(newEdgeProp[7], val);
                            if (newEdgeProp.size() == 9) {
                                val = reader->getValueByIndex(8);
                                EXPECT_EQ(newEdgeProp[8], val);
                            }
                        }
                    }
                }
                num++;
                totalCount++;
                iter->next();
            }
        }
    }
    EXPECT_EQ(expectNum, totalCount);
}

void checkEdgesData(int32_t spaceVidLen,
                     GraphSpaceID spaceId,
                     std::unordered_map<PartitionID,
                                        std::vector<nebula::storage::cpp2::EdgeKey>> parts,
                     StorageEnv* env,
                     int expectNum) {
    int totalCount = 0;
    for (auto& part : parts) {
        auto partId = part.first;
        auto deleteEdgeKeyVec = part.second;
        for (auto& edgeKey : deleteEdgeKeyVec) {
            auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen,
                                                     partId,
                                                     edgeKey.get_src().getStr(),
                                                     edgeKey.get_edge_type(),
                                                     edgeKey.get_ranking(),
                                                     edgeKey.get_dst().getStr());
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED,
                      env->kvstore_->prefix(spaceId, partId, prefix, &iter));

            while (iter && iter->valid()) {
                totalCount++;
                iter->next();
            }
        }
    }
    EXPECT_EQ(expectNum, totalCount);
}

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TEST_TESTUTILS_H_
