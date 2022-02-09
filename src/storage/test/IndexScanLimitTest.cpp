/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <rocksdb/db.h>

#include "codec/RowReader.h"
#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/NebulaKeyUtils.h"
#include "mock/AdHocIndexManager.h"
#include "mock/AdHocSchemaManager.h"
#include "mock/MockCluster.h"
#include "storage/index/LookupProcessor.h"
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/test/TestUtils.h"

namespace nebula {
namespace storage {
ObjectPool objPool;
auto pool = &objPool;

class IndexScanLimitTest : public ::testing::Test {
 protected:
  GraphSpaceID spaceId = 1;
  TagID tagId = 100;
  EdgeType edgeType = 200;
  IndexID tagIndex = 101;
  IndexID edgeIndex = 201;
  size_t vertexLen = 32;
  const std::vector<PartitionID> parts{1, 2, 3, 4, 5, 6};

 private:
  std::unique_ptr<kvstore::NebulaStore> initKV(kvstore::KVOptions options) {
    HostAddr localHost;
    auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
    auto workers = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(1);
    workers->setNamePrefix("executor");
    workers->start();
    localHost.host = "0.0.0.0";
    localHost.port = network::NetworkUtils::getAvailablePort();
    auto store =
        std::make_unique<kvstore::NebulaStore>(std::move(options), ioPool, localHost, workers);
    store->init();
    return store;
  }

  std::unique_ptr<kvstore::MemPartManager> memPartMan() {
    auto memPartMan = std::make_unique<kvstore::MemPartManager>();
    // GraphSpaceID =>  {PartitionIDs}
    auto& partsMap = memPartMan->partsMap();
    for (auto partId : parts) {
      partsMap[spaceId][partId] = meta::PartHosts();
    }
    return memPartMan;
  }

  std::shared_ptr<meta::NebulaSchemaProvider> mockSchema() {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
    schema->addField("col1", nebula::cpp2::PropertyType::INT64);
    schema->addField("col2", nebula::cpp2::PropertyType::STRING);
    return schema;
  }

  std::unique_ptr<meta::SchemaManager> memSchemaMan() {
    auto schemaMan = std::make_unique<nebula::mock::AdHocSchemaManager>(6);
    schemaMan->addTagSchema(spaceId, tagId, mockSchema());
    schemaMan->addEdgeSchema(spaceId, edgeType, mockSchema());
    return schemaMan;
  }

  std::vector<nebula::meta::cpp2::ColumnDef> genCols() {
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    meta::cpp2::ColumnDef col;
    col.name = "col1";
    col.type.type_ref() = nebula::cpp2::PropertyType::INT64;
    cols.emplace_back(std::move(col));
    return cols;
  }

  std::unique_ptr<meta::IndexManager> memIndexMan(bool isEmpty = false) {
    auto indexMan = std::make_unique<nebula::mock::AdHocIndexManager>();
    if (isEmpty) {
      return indexMan;
    }
    indexMan->addTagIndex(spaceId, tagId, tagIndex, genCols());
    indexMan->addEdgeIndex(spaceId, edgeType, edgeIndex, genCols());
    return indexMan;
  }

 protected:
  bool mockData() {
    auto tag = schemaMan_->getTagSchema(spaceId, tagId);
    if (!tag) {
      LOG(INFO) << "Space " << spaceId << ", Tag " << tagId << " invalid";
      return false;
    }

    // Edge and vertex have the same schema of structure, so it's good to only generate it once.
    RowWriterV2 writer1(tag.get());
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.setValue(0, 111));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.setValue(1, "row_111"));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer1.finish());
    auto val1 = std::move(writer1).moveEncodedStr();
    RowWriterV2 writer2(tag.get());
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.setValue(0, 222));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.setValue(1, "row_222"));
    EXPECT_EQ(WriteResult::SUCCEEDED, writer2.finish());
    auto val2 = std::move(writer2).moveEncodedStr();

    for (auto pId : parts) {
      std::vector<kvstore::KV> data;
      for (int64_t vid = pId * 1000; vid < (pId + 1) * 1000; vid++) {
        int64_t col1Val = vid % 2 == 0 ? 111 : 222;
        std::string val = vid % 2 == 0 ? val1 : val2;
        auto vertex = folly::to<std::string>(vid);
        auto edgeKey = NebulaKeyUtils::edgeKey(vertexLen, pId, vertex, edgeType, 0, vertex);
        auto tagKey = NebulaKeyUtils::tagKey(vertexLen, pId, vertex, tagId);
        data.emplace_back(std::move(edgeKey), val);
        data.emplace_back(std::move(tagKey), std::move(val));
        if (indexMan_ != nullptr) {
          auto indexItem = std::make_unique<meta::cpp2::IndexItem>();
          indexItem->fields_ref() = genCols();
          if (indexMan_->getTagIndex(spaceId, tagIndex).ok()) {
            auto vertexIndexKeys = IndexKeyUtils::vertexIndexKeys(
                vertexLen,
                pId,
                tagIndex,
                vertex,
                IndexKeyUtils::encodeValues({col1Val}, indexItem.get()));
            for (auto& vertexIndexKey : vertexIndexKeys) {
              data.emplace_back(std::move(vertexIndexKey), "");
            }
          }
          if (indexMan_->getEdgeIndex(spaceId, edgeIndex).ok()) {
            auto edgeIndexKeys = IndexKeyUtils::edgeIndexKeys(
                vertexLen,
                pId,
                edgeIndex,
                vertex,
                0,
                vertex,
                IndexKeyUtils::encodeValues({col1Val}, indexItem.get()));
            for (auto& edgeIndexKey : edgeIndexKeys) {
              data.emplace_back(std::move(edgeIndexKey), "");
            }
          }
        }
      }
      folly::Baton<true, std::atomic> baton;
      storageKV_->asyncMultiPut(spaceId, pId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        EXPECT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
        baton.post();
      });
      baton.wait();
    }
    return true;
  }

  void SetUp() override {
    rootPath_ = new fs::TempDir("/tmp/IndexScanLimitTest.XXXXXX");
    kvstore::KVOptions options;
    schemaMan_ = memSchemaMan();
    indexMan_ = memIndexMan();
    options.partMan_ = memPartMan();
    options.schemaMan_ = schemaMan_.get();

    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk1", rootPath_->path()));

    options.dataPaths_ = std::move(paths);
    storageKV_ = initKV(std::move(options));
    mock::MockCluster::waitUntilAllElected(storageKV_.get(), spaceId, parts);

    storageEnv_ = std::make_unique<storage::StorageEnv>();
    storageEnv_->schemaMan_ = schemaMan_.get();
    storageEnv_->indexMan_ = indexMan_.get();
    storageEnv_->kvstore_ = storageKV_.get();
    storageEnv_->rebuildIndexGuard_ = std::make_unique<storage::IndexGuard>();
    storageEnv_->verticesML_ = std::make_unique<storage::VerticesMemLock>();
    storageEnv_->edgesML_ = std::make_unique<storage::EdgesMemLock>();
    EXPECT_TRUE(mockData());
  }

  void TearDown() override {
    delete rootPath_;
  }

 protected:
  fs::TempDir* rootPath_;
  std::unique_ptr<meta::MetaClient> metaClient_;
  std::unique_ptr<meta::SchemaManager> schemaMan_{nullptr};
  std::unique_ptr<meta::IndexManager> indexMan_{nullptr};
  std::unique_ptr<kvstore::NebulaStore> storageKV_{nullptr};
  std::unique_ptr<storage::StorageEnv> storageEnv_{nullptr};
};

TEST_F(IndexScanLimitTest, LookupTagIndexLimit) {
  cpp2::LookupIndexRequest req;
  nebula::storage::cpp2::IndexSpec indices;
  req.space_id_ref() = spaceId;
  nebula::cpp2::SchemaID schemaId;
  schemaId.tag_id_ref() = tagId;
  indices.schema_id_ref() = schemaId;
  req.parts_ref() = parts;
  std::vector<std::string> returnCols;
  returnCols.emplace_back(kVid);
  req.return_columns_ref() = std::move(returnCols);
  cpp2::IndexQueryContext context1;
  context1.index_id_ref() = tagIndex;
  decltype(indices.contexts) contexts;
  contexts.emplace_back(std::move(context1));
  indices.contexts_ref() = std::move(contexts);
  req.indices_ref() = std::move(indices);

  // verify all data
  {
    auto* processor = LookupProcessor::instance(storageEnv_.get(), nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(6000, resp.get_data()->rows.size());
  }

  // limit == 0
  {
    req.limit_ref() = 0;
    auto* processor = LookupProcessor::instance(storageEnv_.get(), nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(0, resp.get_data()->rows.size());
  }

  // limit == 1
  {
    req.limit_ref() = 1;
    auto* processor = LookupProcessor::instance(storageEnv_.get(), nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(1 * parts.size(), resp.get_data()->rows.size());
  }

  // limit 5 by each part
  {
    req.limit_ref() = 5;
    auto* processor = LookupProcessor::instance(storageEnv_.get(), nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(5 * parts.size(), resp.get_data()->rows.size());
  }

  // limit 5 by each part through IndexScanNode->DataNode
  {
    req.limit_ref() = 5;
    cpp2::IndexColumnHint columnHint;
    columnHint.begin_value_ref() = Value(111);
    columnHint.column_name_ref() = "col1";
    columnHint.scan_type_ref() = cpp2::ScanType::PREFIX;
    std::vector<cpp2::IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    req.return_columns_ref().value().emplace_back("col2");
    req.indices_ref().value().contexts_ref().value().begin()->column_hints_ref() =
        std::move(columnHints);
    auto* processor = LookupProcessor::instance(storageEnv_.get(), nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(5 * parts.size(), resp.get_data()->rows.size());
  }

  // limit 5 by each part through IndexScanNode->DataNode->FilterNode
  {
    req.limit_ref() = 5;
    cpp2::IndexColumnHint columnHint;
    columnHint.begin_value_ref() = Value(111);
    columnHint.column_name_ref() = "col1";
    columnHint.scan_type_ref() = cpp2::ScanType::PREFIX;
    std::vector<cpp2::IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    auto expr = RelationalExpression::makeNE(pool,
                                             TagPropertyExpression::make(pool, "100", "col1"),
                                             ConstantExpression::make(pool, Value(300L)));
    req.indices_ref().value().contexts_ref().value().begin()->filter_ref() = expr->encode();
    req.indices_ref().value().contexts_ref().value().begin()->column_hints_ref() =
        std::move(columnHints);
    auto* processor = LookupProcessor::instance(storageEnv_.get(), nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(5 * parts.size(), resp.get_data()->rows.size());
  }
}

TEST_F(IndexScanLimitTest, LookupEdgeIndexLimit) {
  cpp2::LookupIndexRequest req;
  nebula::storage::cpp2::IndexSpec indices;
  req.space_id_ref() = spaceId;
  nebula::cpp2::SchemaID schemaId;
  schemaId.edge_type_ref() = edgeType;
  indices.schema_id_ref() = schemaId;
  req.parts_ref() = parts;
  std::vector<std::string> returnCols;
  returnCols.emplace_back(kSrc);
  req.return_columns_ref() = std::move(returnCols);
  cpp2::IndexQueryContext context1;
  context1.index_id_ref() = edgeIndex;
  decltype(indices.contexts) contexts;
  contexts.emplace_back(std::move(context1));
  indices.contexts_ref() = std::move(contexts);
  req.indices_ref() = std::move(indices);

  // verify all data
  {
    auto* processor = LookupProcessor::instance(storageEnv_.get(), nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(6000, resp.get_data()->rows.size());
  }

  // limit == 0
  {
    req.limit_ref() = 0;
    auto* processor = LookupProcessor::instance(storageEnv_.get(), nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(0, resp.get_data()->rows.size());
  }

  // limit == 1
  {
    req.limit_ref() = 1;
    auto* processor = LookupProcessor::instance(storageEnv_.get(), nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(1 * parts.size(), resp.get_data()->rows.size());
  }

  // limit 5 by each part
  {
    req.limit_ref() = 5;
    auto* processor = LookupProcessor::instance(storageEnv_.get(), nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(5 * parts.size(), resp.get_data()->rows.size());
  }

  // limit 5 by each part through IndexScanNode->DataNode
  {
    req.limit_ref() = 5;
    cpp2::IndexColumnHint columnHint;
    columnHint.begin_value_ref() = Value(111);
    columnHint.column_name_ref() = "col1";
    columnHint.scan_type_ref() = cpp2::ScanType::PREFIX;
    std::vector<cpp2::IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    req.return_columns_ref().value().emplace_back("col2");
    req.indices_ref().value().contexts_ref().value().begin()->column_hints_ref() =
        std::move(columnHints);

    auto* processor = LookupProcessor::instance(storageEnv_.get(), nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(5 * parts.size(), resp.get_data()->rows.size());
  }

  // limit 5 by each part through IndexScanNode->DataNode->FilterNode
  {
    req.limit_ref() = 5;
    cpp2::IndexColumnHint columnHint;
    columnHint.begin_value_ref() = Value(111);
    columnHint.column_name_ref() = "col1";
    columnHint.scan_type_ref() = cpp2::ScanType::PREFIX;
    std::vector<cpp2::IndexColumnHint> columnHints;
    columnHints.emplace_back(std::move(columnHint));
    auto expr = RelationalExpression::makeNE(pool,
                                             EdgePropertyExpression::make(pool, "200", "col1"),
                                             ConstantExpression::make(pool, Value(300L)));
    req.indices_ref().value().contexts_ref().value().begin()->filter_ref() = expr->encode();
    req.indices_ref().value().contexts_ref().value().begin()->column_hints_ref() =
        std::move(columnHints);
    auto* processor = LookupProcessor::instance(storageEnv_.get(), nullptr, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
    EXPECT_EQ(5 * parts.size(), resp.get_data()->rows.size());
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
