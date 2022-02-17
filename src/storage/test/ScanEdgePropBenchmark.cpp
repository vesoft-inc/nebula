/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <bits/std_abs.h>                          // for abs
#include <folly/Range.h>                           // for Range
#include <folly/init/Init.h>                       // for init
#include <folly/io/async/ScopedEventBaseThread.h>  // for StringPiece
#include <folly/stop_watch.h>                      // for stop_watch
#include <glog/logging.h>                          // for WARNING
#include <gtest/gtest-param-test.h>                // for Values, ParamItera...
#include <gtest/gtest.h>                           // for Message
#include <gtest/gtest.h>                           // for TestPartResult
#include <stdlib.h>                                // for abs, size_t

#include <chrono>              // for duration, microsec...
#include <cstdint>             // for int32_t
#include <ext/alloc_traits.h>  // for __alloc_traits<>::...
#include <memory>              // for unique_ptr, allocator
#include <string>              // for string, basic_string
#include <type_traits>         // for remove_reference<>...
#include <unordered_map>       // for operator!=, _Node_...
#include <utility>             // for move, make_pair, pair
#include <vector>              // for vector

#include "codec/RowReader.h"                   // for RowReader
#include "codec/RowReaderV2.h"                 // for RowReaderV2
#include "codec/RowReaderWrapper.h"            // for RowReaderWrapper
#include "common/base/Logging.h"               // for LOG, LogMessage
#include "common/base/Status.h"                // for Status
#include "common/base/StatusOr.h"              // for StatusOr
#include "common/datatypes/List.h"             // for List
#include "common/datatypes/Value.h"            // for Value
#include "common/fs/TempDir.h"                 // for TempDir
#include "common/meta/NebulaSchemaProvider.h"  // for NebulaSchemaProvider
#include "common/meta/SchemaManager.h"         // for SchemaManager
#include "common/thrift/ThriftTypes.h"         // for SchemaVer, VertexID
#include "common/utils/NebulaKeyUtils.h"       // for NebulaKeyUtils
#include "interface/gen-cpp2/common_types.h"   // for ErrorCode, ErrorCo...
#include "kvstore/KVIterator.h"                // for KVIterator
#include "kvstore/KVStore.h"                   // for KVStore
#include "kvstore/RocksEngineConfig.h"         // for FLAGS_rocksdb_bloc...
#include "mock/AdHocSchemaManager.h"           // for AdHocSchemaManager
#include "mock/MockCluster.h"                  // for MockCluster
#include "storage/CommonUtils.h"               // for StorageEnv
#include "storage/exec/QueryUtils.h"           // for QueryUtils
#include "storage/exec/StorageIterator.h"      // for StorageIterator
#include "storage/query/QueryBaseProcessor.h"  // for PropContext
#include "storage/test/QueryTestUtils.h"       // for QueryTestUtils

namespace nebula {

class ScanEdgePropBench : public ::testing::TestWithParam<std::pair<int, int>> {};

class TestSingleEdgeIterator : public storage::StorageIterator {
 public:
  explicit TestSingleEdgeIterator(std::unique_ptr<kvstore::KVIterator> iter)
      : iter_(std::move(iter)) {}

  bool valid() const override {
    return iter_->valid();
  }

  void next() override {
    do {
      iter_->next();
    } while (iter_->valid() && !check());
  }

  folly::StringPiece key() const override {
    return iter_->key();
  }

  folly::StringPiece val() const override {
    return iter_->val();
  }

  RowReader* reader() const override {
    return reader_.get();
  }

 private:
  // return true when the value iter to a valid edge value
  bool check() {
    return true;
  }

  std::unique_ptr<kvstore::KVIterator> iter_;
  std::unique_ptr<RowReader> reader_;
};

TEST_P(ScanEdgePropBench, ProcessEdgeProps) {
  // forbid rocksdb block cache
  FLAGS_rocksdb_block_cache = 0;
  auto param = GetParam();
  SchemaVer schemaVerCount = param.first;
  EdgeRanking rankCount = param.second;
  fs::TempDir rootPath("/tmp/ScanEdgePropBench.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path(), {"0", 0}, schemaVerCount);
  auto* env = cluster.storageEnv_.get();
  auto totalParts = cluster.getTotalParts();
  ASSERT_TRUE(
      storage::QueryTestUtils::mockBenchEdgeData(env, totalParts, schemaVerCount, rankCount));

  std::hash<std::string> hash;
  VertexID vId = "Tim Duncan";
  GraphSpaceID spaceId = 1;
  PartitionID partId = (hash(vId) % totalParts) + 1;
  EdgeType edgeType = 101;
  auto vIdLen = env->schemaMan_->getSpaceVidLen(spaceId).value();
  bool isIntId = false;

  std::vector<storage::PropContext> props;
  {
    std::vector<std::string> names = {"playerName", "teamName", "startYear", "endYear"};
    for (const auto& name : names) {
      storage::PropContext ctx(name.c_str());
      ctx.returned_ = true;
      props.emplace_back(std::move(ctx));
    }
  }

  auto prefix = NebulaKeyUtils::edgePrefix(vIdLen, partId, vId, edgeType);
  {
    // mock the process in 1.0, each time we get the schema from SchemaMan
    // (cache in MetaClient), and then collect values
    nebula::Value result = nebula::List();
    nebula::List list;
    auto* schemaMan = dynamic_cast<mock::AdHocSchemaManager*>(env->schemaMan_);
    std::unique_ptr<kvstore::KVIterator> kvIter;
    std::unique_ptr<storage::StorageIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &kvIter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && kvIter && kvIter->valid()) {
      iter.reset(new TestSingleEdgeIterator(std::move(kvIter)));
    }
    size_t edgeRowCount = 0;
    folly::stop_watch<std::chrono::microseconds> watch;
    for (; iter->valid(); iter->next(), edgeRowCount++) {
      auto key = iter->key();
      auto val = iter->val();

      SchemaVer schemaVer;
      int32_t readerVer;
      RowReaderWrapper::getVersions(val, schemaVer, readerVer);
      auto schema = schemaMan->getEdgeSchemaFromMap(spaceId, edgeType, schemaVer);
      ASSERT_TRUE(schema != nullptr);
      auto wrapper = std::make_unique<RowReaderWrapper>();
      ASSERT_TRUE(wrapper->reset(schema.get(), val, readerVer));
      auto code =
          storage::QueryUtils::collectEdgeProps(key, vIdLen, isIntId, wrapper.get(), &props, list);
      ASSERT_TRUE(code.ok());
      result.mutableList().values.emplace_back(std::move(list));
    }
    LOG(WARNING) << "ProcessEdgeProps with schema from map: process " << edgeRowCount
                 << " edges takes " << watch.elapsed().count() << " us.";
  }
  {
    // new edge reader each time
    nebula::Value result = nebula::List();
    nebula::List list;
    std::unique_ptr<kvstore::KVIterator> kvIter;
    std::unique_ptr<storage::StorageIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &kvIter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && kvIter && kvIter->valid()) {
      iter.reset(new TestSingleEdgeIterator(std::move(kvIter)));
    }
    size_t edgeRowCount = 0;
    folly::stop_watch<std::chrono::microseconds> watch;
    for (; iter->valid(); iter->next(), edgeRowCount++) {
      auto key = iter->key();
      auto val = iter->val();
      auto reader =
          RowReaderWrapper::getEdgePropReader(env->schemaMan_, spaceId, std::abs(edgeType), val);
      ASSERT_TRUE(reader.get() != nullptr);
      auto code =
          storage::QueryUtils::collectEdgeProps(key, vIdLen, isIntId, reader.get(), &props, list);
      ASSERT_TRUE(code.ok());
      result.mutableList().values.emplace_back(std::move(list));
    }
    LOG(WARNING) << "ProcessEdgeProps new reader each time: process " << edgeRowCount
                 << " edges takes " << watch.elapsed().count() << " us.";
  }
  {
    // reset edge reader each time instead of new one
    nebula::Value result = nebula::List();
    nebula::List list;
    std::unique_ptr<kvstore::KVIterator> kvIter;
    std::unique_ptr<storage::StorageIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &kvIter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && kvIter && kvIter->valid()) {
      iter.reset(new TestSingleEdgeIterator(std::move(kvIter)));
    }
    size_t edgeRowCount = 0;
    RowReaderWrapper reader;
    folly::stop_watch<std::chrono::microseconds> watch;
    for (; iter->valid(); iter->next(), edgeRowCount++) {
      auto key = iter->key();
      auto val = iter->val();
      reader =
          RowReaderWrapper::getEdgePropReader(env->schemaMan_, spaceId, std::abs(edgeType), val);
      ASSERT_TRUE(reader.get() != nullptr);
      auto code =
          storage::QueryUtils::collectEdgeProps(key, vIdLen, isIntId, reader.get(), &props, list);
      ASSERT_TRUE(code.ok());
      result.mutableList().values.emplace_back(std::move(list));
    }
    LOG(WARNING) << "ProcessEdgeProps reader reset: process " << edgeRowCount << " edges takes "
                 << watch.elapsed().count() << " us.";
  }
  {
    // use the schema saved in processor
    nebula::Value result = nebula::List();
    nebula::List list;
    std::unique_ptr<kvstore::KVIterator> kvIter;
    std::unique_ptr<storage::StorageIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &kvIter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && kvIter && kvIter->valid()) {
      iter.reset(new TestSingleEdgeIterator(std::move(kvIter)));
    }
    size_t edgeRowCount = 0;
    RowReaderWrapper reader;

    // find all version of edge schema
    auto edges = env->schemaMan_->getAllVerEdgeSchema(spaceId);
    ASSERT_TRUE(edges.ok());
    auto edgeSchemas = std::move(edges).value();
    auto edgeIter = edgeSchemas.find(std::abs(edgeType));
    ASSERT_TRUE(edgeIter != edgeSchemas.end());
    const auto& schemas = edgeIter->second;

    folly::stop_watch<std::chrono::microseconds> watch;
    for (; iter->valid(); iter->next(), edgeRowCount++) {
      auto key = iter->key();
      auto val = iter->val();
      ASSERT_TRUE(reader->reset(schemas, val));
      auto code =
          storage::QueryUtils::collectEdgeProps(key, vIdLen, isIntId, reader.get(), &props, list);
      ASSERT_TRUE(code.ok());
      result.mutableList().values.emplace_back(std::move(list));
    }
    LOG(WARNING) << "ProcessEdgeProps reader reset with vector schemas: process " << edgeRowCount
                 << " edges takes " << watch.elapsed().count() << " us.";
  }
  {
    // use the schema saved in processor
    // only use RowReaderV2 instead of RowReaderWrapper
    nebula::Value result = nebula::List();
    nebula::List list;
    std::unique_ptr<kvstore::KVIterator> kvIter;
    std::unique_ptr<storage::StorageIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &kvIter);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED && kvIter && kvIter->valid()) {
      iter.reset(new TestSingleEdgeIterator(std::move(kvIter)));
    }
    size_t edgeRowCount = 0;
    RowReaderV2 reader;

    // find all version of edge schema
    auto edges = env->schemaMan_->getAllVerEdgeSchema(spaceId);
    ASSERT_TRUE(edges.ok());
    auto edgeSchemas = std::move(edges).value();
    auto edgeIter = edgeSchemas.find(std::abs(edgeType));
    ASSERT_TRUE(edgeIter != edgeSchemas.end());
    const auto& schemas = edgeIter->second;

    auto resetReaderV2 = [&schemas, &reader](folly::StringPiece row) -> bool {
      SchemaVer schemaVer;
      int32_t readerVer;
      RowReaderWrapper::getVersions(row, schemaVer, readerVer);
      if (static_cast<size_t>(schemaVer) >= schemas.size()) {
        return false;
      }
      if (schemaVer != schemas[schemaVer]->getVersion()) {
        return false;
      }
      EXPECT_EQ(readerVer, 2);
      return reader.resetImpl(schemas[schemaVer].get(), row);
    };

    folly::stop_watch<std::chrono::microseconds> watch;
    for (; iter->valid(); iter->next(), edgeRowCount++) {
      auto key = iter->key();
      auto val = iter->val();
      ASSERT_TRUE(resetReaderV2(val));
      auto code =
          storage::QueryUtils::collectEdgeProps(key, vIdLen, isIntId, &reader, &props, list);
      ASSERT_TRUE(code.ok());
      result.mutableList().values.emplace_back(std::move(list));
    }
    LOG(WARNING) << "ProcessEdgeProps only RowReaderV2 reset with vector "
                    "schemas: process "
                 << edgeRowCount << " edges takes " << watch.elapsed().count() << " us.";
  }
}

// the parameter pair<int, int> is
// 1. count of edge schema version,
// 2. how many edges will be scanned
INSTANTIATE_TEST_SUITE_P(ScanEdgePropBench,
                         ScanEdgePropBench,
                         ::testing::Values(std::make_pair(1, 10000),
                                           std::make_pair(10, 10000),
                                           std::make_pair(1, 100),
                                           std::make_pair(10, 100)));

}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::WARNING);
  return RUN_ALL_TESTS();
}
