/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include <filesystem>

#include "common/base/Base.h"
#include "common/datatypes/Vector.h"
#include "common/vectorIndex/HNSWIndex.h"
#include "common/vectorIndex/IVFIndex.h"
#include "common/vectorIndex/VectorIndexUtils.h"

namespace nebula {

class AnnIndexTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Test parameters
    graphID_ = 1;
    partitionID_ = 1;
    indexID_ = 100;
    indexName_ = "test_index";
    propFromNode_ = true;
    dim_ = 128;
    rootPath_ = "/tmp/vector_index_test";
    metricType_ = MetricType::L2;
    minTrainDataSize_ = 3;

    // Create test directory
    std::filesystem::create_directories(rootPath_);

    // Generate test data
    generateTestData();
  }

  void TearDown() override {
    // Clean up test directory
    std::filesystem::remove_all(rootPath_);
  }

  void generateTestData() {
    std::random_device rd;
    std::mt19937 gen(42);  // Fixed seed for reproducible tests
    std::uniform_real_distribution<float> dis(-1.0f, 1.0f);

    // Generate training vectors
    size_t numVectors = 100;
    trainData_.reserve(numVectors * dim_);
    trainIds_.reserve(numVectors);

    for (size_t i = 0; i < numVectors; ++i) {
      trainIds_.push_back(static_cast<VectorID>(i + 1000));  // Start from 1000
      for (size_t j = 0; j < dim_; ++j) {
        trainData_.push_back(dis(gen));
      }
    }

    for (size_t i = 0; i < numVectors / 10; ++i) {
      learnData_.push_back(trainData_[i]);
    }
    trainData_.insert(trainData_.begin(), learnData_.begin(), learnData_.end());

    // Generate query vector
    queryData_.reserve(dim_);
    for (size_t j = 0; j < dim_; ++j) {
      queryData_.push_back(dis(gen));
    }

    // Create VecData structure
    vecData_.cnt = numVectors;
    vecData_.dim = dim_;
    vecData_.fdata = trainData_.data();
    vecData_.ids = trainIds_.data();
  }

  std::unique_ptr<IDSelector> createIDSelector(const std::vector<VectorID>& ids) {
    auto selector = std::make_unique<IDSelector>();
    selector->cnt = ids.size();
    selector->ids = const_cast<VectorID*>(ids.data());
    return selector;
  }

 protected:
  GraphSpaceID graphID_;
  PartitionID partitionID_;
  IndexID indexID_;
  std::string indexName_;
  bool propFromNode_;
  size_t dim_;
  std::string rootPath_;
  MetricType metricType_;
  size_t minTrainDataSize_;

  std::vector<float> trainData_;
  std::vector<float> learnData_;
  std::vector<VectorID> trainIds_;
  std::vector<float> queryData_;
  VecData vecData_;
};

// HNSW Index Tests
class HNSWIndexTest : public AnnIndexTest {
 protected:
  void SetUp() override {
    AnnIndexTest::SetUp();

    // HNSW specific parameters
    M_ = 16;
    efConstruction_ = 200;
    maxElements_ = 1000;
    efSearch_ = 50;
  }

  std::unique_ptr<HNSWIndex> createHNSWIndex() {
    return std::make_unique<HNSWIndex>(graphID_,
                                       partitionID_,
                                       indexID_,
                                       indexName_,
                                       propFromNode_,
                                       dim_,
                                       rootPath_,
                                       metricType_,
                                       minTrainDataSize_);
  }

  std::unique_ptr<BuildParamsHNSW> createBuildParams() {
    return std::make_unique<BuildParamsHNSW>(metricType_, M_, efConstruction_, maxElements_);
  }

  std::unique_ptr<SearchParamsHNSW> createSearchParams(size_t k) {
    return std::make_unique<SearchParamsHNSW>(k, queryData_.data(), dim_, efSearch_);
  }

 protected:
  size_t M_;
  size_t efConstruction_;
  size_t maxElements_;
  size_t efSearch_;
};

// HNSW Single-threaded Functional Tests
TEST_F(HNSWIndexTest, Constructor) {
  auto index = createHNSWIndex();
  EXPECT_EQ(index->indexType(), AnnIndexType::HNSW);
  EXPECT_FALSE(index->toString().empty());
}

TEST_F(HNSWIndexTest, InitializeIndex) {
  auto index = createHNSWIndex();
  auto params = createBuildParams();

  auto status = index->init(params.get());
  EXPECT_TRUE(status.ok()) << status.toString();
}

TEST_F(HNSWIndexTest, AddVectors) {
  auto index = createHNSWIndex();
  auto params = createBuildParams();

  ASSERT_TRUE(index->init(params.get()).ok());

  auto status = index->add(&vecData_, true);
  EXPECT_TRUE(status.ok()) << status.toString();
}

TEST_F(HNSWIndexTest, SearchVectors) {
  auto index = createHNSWIndex();
  auto buildParams = createBuildParams();
  auto searchParams = createSearchParams(5);

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());
  SearchResult result;
  auto status = index->search(searchParams.get(), &result);
  EXPECT_TRUE(status.ok()) << status.toString();

  // Check results
  EXPECT_LE(result.IDs.size(), 5);
  EXPECT_EQ(result.IDs.size(), result.distances.size());
}

TEST_F(HNSWIndexTest, UpsertVectors) {
  auto index = createHNSWIndex();
  auto buildParams = createBuildParams();

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  // Modify some data and upsert
  std::vector<float> upsertData = {trainData_[0], trainData_[1], trainData_[2]};  // First vector
  std::vector<VectorID> upsertIds = {trainIds_[0]};

  // Modify the data slightly
  for (auto& val : upsertData) {
    val += 0.1f;
  }

  VecData upsertVecData;
  upsertVecData.cnt = 1;
  upsertVecData.dim = dim_;
  upsertVecData.fdata = upsertData.data();
  upsertVecData.ids = upsertIds.data();

  auto status = index->upsert(&upsertVecData);
  EXPECT_TRUE(status.ok()) << status.toString();
}

TEST_F(HNSWIndexTest, RemoveVectors) {
  auto index = createHNSWIndex();
  auto buildParams = createBuildParams();

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  // Remove some vectors
  std::vector<VectorID> removeIds = {trainIds_[0], trainIds_[1], trainIds_[2]};
  auto selector = createIDSelector(removeIds);

  auto result = index->remove(*selector);
  EXPECT_TRUE(result.ok()) << result.status().toString();
  EXPECT_EQ(result.value(), removeIds.size());
}

TEST_F(HNSWIndexTest, ReconstructVector) {
  auto index = createHNSWIndex();
  auto buildParams = createBuildParams();

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  auto result = index->reconstruct(trainIds_[0]);
  if (result.ok()) {
    EXPECT_EQ(result.value().values.size(), dim_);
  }
  // Note: Some index types may not support reconstruction
}

TEST_F(HNSWIndexTest, WriteAndReadIndex) {
  auto index = createHNSWIndex();
  auto buildParams = createBuildParams();

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  std::string indexFile = "test_hnsw_index.bin";

  // Write index
  auto writeStatus = index->write(rootPath_, indexFile);
  EXPECT_TRUE(writeStatus.ok()) << writeStatus.toString();

  // Create new index and read
  auto newIndex = createHNSWIndex();
  ASSERT_TRUE(newIndex->init(buildParams.get()).ok());

  auto readStatus = newIndex->read(rootPath_ + "/" + indexFile);
  EXPECT_TRUE(readStatus.ok()) << readStatus.toString();

  // Test search on loaded index
  auto searchParams = createSearchParams(3);
  SearchResult result;
  auto searchStatus = newIndex->search(searchParams.get(), &result);
  EXPECT_TRUE(searchStatus.ok()) << searchStatus.toString();
}

TEST_F(HNSWIndexTest, WALOperations) {
  auto index = createHNSWIndex();
  auto buildParams = createBuildParams();

  ASSERT_TRUE(index->init(buildParams.get()).ok());

  std::string walName = "test_hnsw.wal";

  // Open WAL
  auto openStatus = index->openWal(walName);
  EXPECT_TRUE(openStatus.ok()) << openStatus.toString();

  // Close WAL
  auto closeStatus = index->closeWal();
  EXPECT_TRUE(closeStatus.ok()) << closeStatus.toString();

  // Remove WAL
  auto removeStatus = index->removeWal();
  EXPECT_TRUE(removeStatus.ok()) << removeStatus.toString();
}

// IVF Index Tests
class IVFIndexTest : public AnnIndexTest {
 protected:
  void SetUp() override {
    AnnIndexTest::SetUp();

    // IVF specific parameters
    nlist_ = 4;
    nprobe_ = 2;
    trainSize_ = 10;
  }

  std::unique_ptr<IVFIndex> createIVFIndex() {
    return std::make_unique<IVFIndex>(graphID_,
                                      partitionID_,
                                      indexID_,
                                      indexName_,
                                      propFromNode_,
                                      dim_,
                                      rootPath_,
                                      metricType_,
                                      minTrainDataSize_);
  }

  std::unique_ptr<BuildParamsIVF> createBuildParams() {
    return std::make_unique<BuildParamsIVF>(metricType_, AnnIndexType::IVF, nlist_, trainSize_);
  }

  std::unique_ptr<SearchParamsIVF> createSearchParams(size_t k) {
    return std::make_unique<SearchParamsIVF>(k, queryData_.data(), dim_, nprobe_);
  }

 protected:
  size_t nlist_;
  size_t nprobe_;
  size_t trainSize_;
};

// IVF Single-threaded Functional Tests
TEST_F(IVFIndexTest, Constructor) {
  auto index = createIVFIndex();
  EXPECT_EQ(index->indexType(), AnnIndexType::IVF);
  EXPECT_FALSE(index->toString().empty());
}

TEST_F(IVFIndexTest, InitializeIndex) {
  auto index = createIVFIndex();
  auto params = createBuildParams();

  auto status = index->init(params.get());
  EXPECT_TRUE(status.ok()) << status.toString();
}

TEST_F(IVFIndexTest, AddVectors) {
  auto index = createIVFIndex();
  auto params = createBuildParams();

  ASSERT_TRUE(index->init(params.get()).ok());

  auto status = index->add(&vecData_, true);
  EXPECT_TRUE(status.ok()) << status.toString();
}

TEST_F(IVFIndexTest, SearchVectors) {
  auto index = createIVFIndex();
  auto buildParams = createBuildParams();
  auto searchParams = createSearchParams(5);

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  SearchResult result;
  auto status = index->search(searchParams.get(), &result);
  EXPECT_TRUE(status.ok()) << status.toString();

  // Check results
  EXPECT_LE(result.IDs.size(), 5);
  EXPECT_EQ(result.IDs.size(), result.distances.size());
}

TEST_F(IVFIndexTest, UpsertVectors) {
  auto index = createIVFIndex();
  auto buildParams = createBuildParams();

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  // Modify some data and upsert
  std::vector<float> upsertData(dim_);
  std::copy(trainData_.begin(), trainData_.begin() + dim_, upsertData.begin());
  std::vector<VectorID> upsertIds = {trainIds_[0]};

  // Modify the data slightly
  for (auto& val : upsertData) {
    val += 0.1f;
  }

  VecData upsertVecData;
  upsertVecData.cnt = 1;
  upsertVecData.dim = dim_;
  upsertVecData.fdata = upsertData.data();
  upsertVecData.ids = upsertIds.data();

  auto status = index->upsert(&upsertVecData);
  EXPECT_TRUE(status.ok()) << status.toString();
}

TEST_F(IVFIndexTest, RemoveVectors) {
  auto index = createIVFIndex();
  auto buildParams = createBuildParams();

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  // Remove some vectors
  std::vector<VectorID> removeIds = {trainIds_[0], trainIds_[1], trainIds_[2]};
  auto selector = createIDSelector(removeIds);

  auto result = index->remove(*selector);
  EXPECT_TRUE(result.ok()) << result.status().toString();
  EXPECT_EQ(result.value(), removeIds.size());
}

TEST_F(IVFIndexTest, ReconstructVector) {
  auto index = createIVFIndex();
  auto buildParams = createBuildParams();

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  auto result = index->reconstruct(trainIds_[0]);
  if (result.ok()) {
    EXPECT_EQ(result.value().values.size(), dim_);
  }
  // Note: Some index types may not support reconstruction
}

TEST_F(IVFIndexTest, WriteAndReadIndex) {
  auto index = createIVFIndex();
  auto buildParams = createBuildParams();

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  std::string indexFile = "test_ivf_index.bin";

  // Write index
  auto writeStatus = index->write(rootPath_, indexFile);
  EXPECT_TRUE(writeStatus.ok()) << writeStatus.toString();

  // Create new index and read
  auto newIndex = createIVFIndex();
  ASSERT_TRUE(newIndex->init(buildParams.get()).ok());

  auto readStatus = newIndex->read(rootPath_ + "/" + indexFile);
  EXPECT_TRUE(readStatus.ok()) << readStatus.toString();

  // Test search on loaded index
  auto searchParams = createSearchParams(3);
  SearchResult result;
  auto searchStatus = newIndex->search(searchParams.get(), &result);
  EXPECT_TRUE(searchStatus.ok()) << searchStatus.toString();
}

TEST_F(IVFIndexTest, InnerProductMetric) {
  auto index = std::make_unique<IVFIndex>(graphID_,
                                          partitionID_,
                                          indexID_,
                                          indexName_,
                                          propFromNode_,
                                          dim_,
                                          rootPath_,
                                          MetricType::INNER_PRODUCT,
                                          minTrainDataSize_);

  auto buildParams = std::make_unique<BuildParamsIVF>(
      MetricType::INNER_PRODUCT, AnnIndexType::IVF, nlist_, trainSize_);

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  auto searchParams = std::make_unique<SearchParamsIVF>(5, queryData_.data(), dim_, nprobe_);

  SearchResult result;
  auto status = index->search(searchParams.get(), &result);
  EXPECT_TRUE(status.ok()) << status.toString();
}

// Multi-threaded Tests
class AnnIndexConcurrencyTest : public AnnIndexTest {
 protected:
  void SetUp() override {
    AnnIndexTest::SetUp();

    // Generate more data for concurrency testing
    generateLargeTestData();
  }

  void generateLargeTestData() {
    std::random_device rd;
    std::mt19937 gen(42);
    std::uniform_real_distribution<float> dis(-1.0f, 1.0f);

    size_t numVectors = 1000;
    largeTrainData_.reserve(numVectors * dim_);
    largeTrainIds_.reserve(numVectors);

    for (size_t i = 0; i < numVectors; ++i) {
      largeTrainIds_.push_back(static_cast<VectorID>(i + 2000));  // Start from 2000
      for (size_t j = 0; j < dim_; ++j) {
        largeTrainData_.push_back(dis(gen));
      }
    }

    largeVecData_.cnt = numVectors;
    largeVecData_.dim = dim_;
    largeVecData_.fdata = largeTrainData_.data();
    largeVecData_.ids = largeTrainIds_.data();
  }

 protected:
  std::vector<float> largeTrainData_;
  std::vector<VectorID> largeTrainIds_;
  VecData largeVecData_;
};

TEST_F(AnnIndexConcurrencyTest, HNSWConcurrentSearch) {
  auto index = std::make_unique<HNSWIndex>(graphID_,
                                           partitionID_,
                                           indexID_,
                                           indexName_,
                                           propFromNode_,
                                           dim_,
                                           rootPath_,
                                           metricType_,
                                           minTrainDataSize_);

  auto buildParams = std::make_unique<BuildParamsHNSW>(metricType_, 16, 200, 2000);

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  constexpr int numThreads = 10;
  constexpr int searchesPerThread = 50;
  std::vector<std::thread> threads;
  std::atomic<int> successCount{0};
  std::atomic<int> errorCount{0};

  for (int i = 0; i < numThreads; ++i) {
    threads.emplace_back([&index, &successCount, &errorCount, this]() {
      auto searchParams = std::make_unique<SearchParamsHNSW>(5, queryData_.data(), dim_, 50);

      for (int j = 0; j < searchesPerThread; ++j) {
        SearchResult result;
        auto status = index->search(searchParams.get(), &result);
        if (status.ok()) {
          successCount++;
        } else {
          errorCount++;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(successCount.load(), numThreads * searchesPerThread);
  EXPECT_EQ(errorCount.load(), 0);
}

TEST_F(AnnIndexConcurrencyTest, IVFConcurrentSearch) {
  auto index = std::make_unique<IVFIndex>(graphID_,
                                          partitionID_,
                                          indexID_,
                                          indexName_,
                                          propFromNode_,
                                          dim_,
                                          rootPath_,
                                          metricType_,
                                          minTrainDataSize_);

  auto buildParams = std::make_unique<BuildParamsIVF>(metricType_, AnnIndexType::IVF, 4, 10);

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  constexpr int numThreads = 10;
  constexpr int searchesPerThread = 50;
  std::vector<std::thread> threads;
  std::atomic<int> successCount{0};
  std::atomic<int> errorCount{0};

  for (int i = 0; i < numThreads; ++i) {
    threads.emplace_back([&index, &successCount, &errorCount, this]() {
      auto searchParams = std::make_unique<SearchParamsIVF>(5, queryData_.data(), dim_, 4);

      for (int j = 0; j < searchesPerThread; ++j) {
        SearchResult result;
        auto status = index->search(searchParams.get(), &result);
        if (status.ok()) {
          successCount++;
        } else {
          errorCount++;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(successCount.load(), numThreads * searchesPerThread);
  EXPECT_EQ(errorCount.load(), 0);
}

TEST_F(AnnIndexConcurrencyTest, HNSWConcurrentAddAndSearch) {
  auto index = std::make_unique<HNSWIndex>(graphID_,
                                           partitionID_,
                                           indexID_,
                                           indexName_,
                                           propFromNode_,
                                           dim_,
                                           rootPath_,
                                           metricType_,
                                           minTrainDataSize_);

  auto buildParams = std::make_unique<BuildParamsHNSW>(metricType_, 16, 200, 2000);

  ASSERT_TRUE(index->init(buildParams.get()).ok());

  constexpr int numThreads = 5;
  std::vector<std::thread> threads;
  std::atomic<int> addSuccessCount{0};
  std::atomic<int> searchSuccessCount{0};

  // Add threads
  for (int i = 0; i < numThreads; ++i) {
    threads.emplace_back([&index, &addSuccessCount, this, i]() {
      size_t vectorsPerThread = vecData_.cnt / numThreads;
      size_t start = i * vectorsPerThread;
      size_t end = (i == numThreads - 1) ? vecData_.cnt : (i + 1) * vectorsPerThread;

      VecData threadData;
      threadData.cnt = end - start;
      threadData.dim = dim_;
      threadData.fdata = vecData_.fdata + start * dim_;
      threadData.ids = vecData_.ids + start;

      auto status = index->add(&threadData);
      if (status.ok()) {
        addSuccessCount += threadData.cnt;
      }
    });
  }

  // Wait for all add operations to complete
  for (auto& t : threads) {
    t.join();
  }
  threads.clear();

  // Search threads
  for (int i = 0; i < numThreads; ++i) {
    threads.emplace_back([&index, &searchSuccessCount, this]() {
      auto searchParams = std::make_unique<SearchParamsHNSW>(5, queryData_.data(), dim_, 50);

      for (int j = 0; j < 20; ++j) {
        SearchResult result;
        auto status = index->search(searchParams.get(), &result);
        if (status.ok()) {
          searchSuccessCount++;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(addSuccessCount.load(), vecData_.cnt);
  EXPECT_EQ(searchSuccessCount.load(), numThreads * 20);
}

TEST_F(AnnIndexConcurrencyTest, IVFConcurrentUpsertAndRemove) {
  auto index = std::make_unique<IVFIndex>(graphID_,
                                          partitionID_,
                                          indexID_,
                                          indexName_,
                                          propFromNode_,
                                          dim_,
                                          rootPath_,
                                          metricType_,
                                          minTrainDataSize_);

  auto buildParams = std::make_unique<BuildParamsIVF>(metricType_, AnnIndexType::IVF, 8, 100);

  ASSERT_TRUE(index->init(buildParams.get()).ok());
  ASSERT_TRUE(index->add(&vecData_, true).ok());

  constexpr int numThreads = 4;
  std::vector<std::thread> threads;
  std::atomic<int> upsertSuccessCount{0};
  std::atomic<int> removeSuccessCount{0};

  // Upsert threads
  for (int i = 0; i < numThreads / 2; ++i) {
    threads.emplace_back([&index, &upsertSuccessCount, this, i]() {
      // Create modified data for upsert
      std::vector<float> modifiedData(dim_);
      std::copy(
          trainData_.begin() + i * dim_, trainData_.begin() + (i + 1) * dim_, modifiedData.begin());

      // Modify the data slightly
      for (auto& val : modifiedData) {
        val += 0.2f;
      }

      VectorID upsertId = trainIds_[i];

      VecData upsertVecData;
      upsertVecData.cnt = 1;
      upsertVecData.dim = dim_;
      upsertVecData.fdata = modifiedData.data();
      upsertVecData.ids = &upsertId;

      auto status = index->upsert(&upsertVecData);
      if (status.ok()) {
        upsertSuccessCount++;
      }
    });
  }

  // Remove threads
  for (int i = numThreads / 2; i < numThreads; ++i) {
    threads.emplace_back([&index, &removeSuccessCount, this, i]() {
      size_t removeIndex = i + 10;  // Remove different vectors
      if (removeIndex < trainIds_.size()) {
        std::vector<VectorID> removeIds = {trainIds_[removeIndex]};
        auto selector = createIDSelector(removeIds);

        auto result = index->remove(*selector);
        if (result.ok()) {
          removeSuccessCount++;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(upsertSuccessCount.load(), numThreads / 2);
  EXPECT_EQ(removeSuccessCount.load(), numThreads / 2);
}

// Performance Tests
class AnnIndexPerformanceTest : public AnnIndexConcurrencyTest {
 protected:
  void SetUp() override {
    AnnIndexConcurrencyTest::SetUp();
    // Performance tests use the large dataset from parent class
  }
};

TEST_F(AnnIndexPerformanceTest, HNSWPerformanceTest) {
  auto index = std::make_unique<HNSWIndex>(graphID_,
                                           partitionID_,
                                           indexID_,
                                           indexName_,
                                           propFromNode_,
                                           dim_,
                                           rootPath_,
                                           metricType_,
                                           minTrainDataSize_);

  auto buildParams = std::make_unique<BuildParamsHNSW>(metricType_, 16, 200, 5000);

  ASSERT_TRUE(index->init(buildParams.get()).ok());

  // Measure add performance
  auto start = std::chrono::high_resolution_clock::now();

  auto status = index->add(&largeVecData_);
  EXPECT_TRUE(status.ok()) << status.toString();

  auto end = std::chrono::high_resolution_clock::now();
  auto addDuration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  std::cout << "HNSW: Added " << largeVecData_.cnt << " vectors in " << addDuration.count() / 1000
            << "s\n";

  // Measure search performance
  auto searchParams = std::make_unique<SearchParamsHNSW>(10, queryData_.data(), dim_, 50);

  start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < 1000; ++i) {
    SearchResult result;
    auto searchStatus = index->search(searchParams.get(), &result);
    EXPECT_TRUE(searchStatus.ok());
  }

  end = std::chrono::high_resolution_clock::now();
  auto searchDuration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  std::cout << "HNSW: Performed 1000 searches in " << searchDuration.count() << " ms" << std::endl;
}

TEST_F(AnnIndexPerformanceTest, IVFPerformanceTest) {
  auto index = std::make_unique<IVFIndex>(graphID_,
                                          partitionID_,
                                          indexID_,
                                          indexName_,
                                          propFromNode_,
                                          dim_,
                                          rootPath_,
                                          metricType_,
                                          minTrainDataSize_);

  auto buildParams = std::make_unique<BuildParamsIVF>(metricType_, AnnIndexType::IVF, 16, 200);

  ASSERT_TRUE(index->init(buildParams.get()).ok());

  // Measure add performance
  auto start = std::chrono::high_resolution_clock::now();

  auto status = index->add(&largeVecData_);
  EXPECT_TRUE(status.ok()) << status.toString();

  auto end = std::chrono::high_resolution_clock::now();
  auto addDuration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  std::cout << "IVF: Added " << largeVecData_.cnt << " vectors in " << addDuration.count() / 1000
            << "s\n";

  // Measure search performance
  auto searchParams = std::make_unique<SearchParamsIVF>(10, queryData_.data(), dim_, 8);

  start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < 1000; ++i) {
    SearchResult result;
    auto searchStatus = index->search(searchParams.get(), &result);
    EXPECT_TRUE(searchStatus.ok());
  }

  end = std::chrono::high_resolution_clock::now();
  auto searchDuration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  std::cout << "IVF: Performed 1000 searches in " << searchDuration.count() << " ms" << std::endl;
}

// Edge Cases and Error Handling Tests
class AnnIndexEdgeCaseTest : public AnnIndexTest {};

TEST_F(AnnIndexEdgeCaseTest, EmptyDataOperations) {
  auto index = std::make_unique<HNSWIndex>(graphID_,
                                           partitionID_,
                                           indexID_,
                                           indexName_,
                                           propFromNode_,
                                           dim_,
                                           rootPath_,
                                           metricType_,
                                           minTrainDataSize_);

  auto buildParams = std::make_unique<BuildParamsHNSW>(metricType_, 16, 200, 1000);
  ASSERT_TRUE(index->init(buildParams.get()).ok());

  // Search on empty index
  auto searchParams = std::make_unique<SearchParamsHNSW>(5, queryData_.data(), dim_, 50);
  SearchResult result;
  auto status = index->search(searchParams.get(), &result);

  // Should handle empty index gracefully
  if (status.ok()) {
    EXPECT_EQ(result.IDs.size(), 0);
  }
}

TEST_F(AnnIndexEdgeCaseTest, LargeKValue) {
  auto index = std::make_unique<HNSWIndex>(graphID_,
                                           partitionID_,
                                           indexID_,
                                           indexName_,
                                           propFromNode_,
                                           dim_,
                                           rootPath_,
                                           metricType_,
                                           minTrainDataSize_);

  auto buildParams = std::make_unique<BuildParamsHNSW>(metricType_, 16, 200, 1000);
  ASSERT_TRUE(index->init(buildParams.get()).ok());

  // Add only a few vectors
  VecData smallData;
  smallData.cnt = 5;
  smallData.dim = dim_;
  smallData.fdata = trainData_.data();
  smallData.ids = trainIds_.data();

  ASSERT_TRUE(index->add(&smallData).ok());

  // Search with k larger than dataset size
  auto searchParams = std::make_unique<SearchParamsHNSW>(10, queryData_.data(), dim_, 50);
  SearchResult result;
  auto status = index->search(searchParams.get(), &result);

  EXPECT_TRUE(status.ok());
  EXPECT_LE(result.IDs.size(), 10);  // Should return at most 10 results
}

TEST_F(AnnIndexEdgeCaseTest, InvalidDimensionData) {
  auto index = std::make_unique<HNSWIndex>(graphID_,
                                           partitionID_,
                                           indexID_,
                                           indexName_,
                                           propFromNode_,
                                           dim_,
                                           rootPath_,
                                           metricType_,
                                           minTrainDataSize_);

  auto buildParams = std::make_unique<BuildParamsHNSW>(metricType_, 16, 200, 1000);
  ASSERT_TRUE(index->init(buildParams.get()).ok());

  // Create data with wrong dimension
  VecData wrongDimData;
  wrongDimData.cnt = 1;
  wrongDimData.dim = dim_ + 10;  // Wrong dimension
  wrongDimData.fdata = trainData_.data();
  wrongDimData.ids = trainIds_.data();

  auto status = index->add(&wrongDimData);
  // Should fail gracefully
  EXPECT_FALSE(status.ok());
}

}  // namespace nebula
