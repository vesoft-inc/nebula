/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/index/VectorIndexProcessor.h"
#include "meta/test/TestUtils.h"
#include "mock/MockCluster.h"

namespace nebula {
namespace meta {

class VectorIndexTest : public ::testing::Test {
 protected:
  void SetUp() override {
    rootPath_ = std::make_unique<fs::TempDir>("/tmp/vector_index_test.XXXXXX");
    kv_ = MockCluster::initMetaKV(rootPath_->path());
    TestUtils::setupHB(kv_.get(), {}, cpp2::HostRole::STORAGE, "");
  }

  void TearDown() override {
    kv_.reset();
    rootPath_.reset();
  }

 protected:
  std::unique_ptr<fs::TempDir> rootPath_;
  std::unique_ptr<kvstore::KVStore> kv_;
};

TEST_F(VectorIndexTest, CreateVectorIndexTest) {
  GraphSpaceID spaceId = 1;
  // Create space first
  { TestUtils::assembleSpace(kv_.get(), spaceId, 1); }

  // Create tag with vector field
  TagID tagId = 2;
  {
    cpp2::Schema schema;
    cpp2::ColumnDef column;
    column.name_ref() = "col_text";
    column.type.type_ref() =
        nebula::cpp2::PropertyType::STRING;  // For now using STRING, may need VECTOR type
    schema.columns_ref().value().emplace_back(std::move(column));

    std::vector<kvstore::KV> data;
    data.emplace_back(MetaKeyUtils::schemaTagKey(spaceId, tagId, 0),
                      MetaKeyUtils::schemaVal("tag_1", schema));
    folly::Baton<true, std::atomic> baton;
    kv_->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  // Create vector index
  {
    cpp2::CreateVectorIndexReq req;
    req.vector_index_name_ref() = "test_vector_index";
    cpp2::VectorIndex index;
    index.space_id_ref() = spaceId;
    index.dimension_ref() = 128;
    index.model_endpoint_ref() = "http://embedding-model:8000";
    index.field_ref() = "col_text";
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = tagId;
    index.depend_schema_ref() = schemaId;
    req.index_ref() = index;

    auto* processor = CreateVectorIndexProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
}

TEST_F(VectorIndexTest, CreateVectorIndexWithInvalidFieldTest) {
  GraphSpaceID spaceId = 1;
  // Create space first
  { TestUtils::assembleSpace(kv_.get(), spaceId, 1); }

  // Create tag
  TagID tagId = 2;
  { TestUtils::mockTag(kv_.get(), spaceId); }

  // Create vector index with invalid field
  {
    cpp2::CreateVectorIndexReq req;
    req.vector_index_name_ref() = "test_vector_index";
    cpp2::VectorIndex index;
    index.space_id_ref() = spaceId;
    index.dimension_ref() = 128;
    index.model_endpoint_ref() = "http://embedding-model:8000";
    index.field_ref() = "non_existent_field";  // Invalid field
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = tagId;
    index.depend_schema_ref() = schemaId;
    req.index_ref() = index;

    auto* processor = CreateVectorIndexProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
}

TEST_F(VectorIndexTest, ListAndDropVectorIndexTest) {
  GraphSpaceID spaceId = 1;
  // Create space first
  { TestUtils::assembleSpace(kv_.get(), spaceId, 1); }

  // Create tag with vector field
  TagID tagId = 2;
  {
    cpp2::Schema schema;
    cpp2::ColumnDef column;
    column.name_ref() = "col_text";
    column.type.type_ref() = nebula::cpp2::PropertyType::STRING;
    schema.columns_ref().value().emplace_back(std::move(column));

    std::vector<kvstore::KV> data;
    data.emplace_back(MetaKeyUtils::schemaTagKey(spaceId, tagId, 0),
                      MetaKeyUtils::schemaVal("tag_1", schema));
    folly::Baton<true, std::atomic> baton;
    kv_->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
          ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  // Create vector index first
  std::string indexName = "test_vector_index";
  {
    cpp2::CreateVectorIndexReq req;
    req.vector_index_name_ref() = indexName;
    cpp2::VectorIndex index;
    index.space_id_ref() = spaceId;
    index.dimension_ref() = 128;
    index.model_endpoint_ref() = "http://embedding-model:8000";
    index.field_ref() = "col_text";
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = tagId;
    index.depend_schema_ref() = schemaId;
    req.index_ref() = index;

    auto* processor = CreateVectorIndexProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }

  // List vector indexes
  {
    cpp2::ListVectorIndexesReq req;
    auto* processor = ListVectorIndexesProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, resp.get_indexes().size());
    ASSERT_TRUE(resp.get_indexes().count(indexName));
  }

  // Drop vector index
  {
    cpp2::DropVectorIndexReq req;
    req.space_id_ref() = spaceId;
    req.vector_index_name_ref() = indexName;
    auto* processor = DropVectorIndexProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }

  // List again to verify it's gone
  {
    cpp2::ListVectorIndexesReq req;
    auto* processor = ListVectorIndexesProcessor::instance(kv_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(0, resp.get_indexes().size());
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
