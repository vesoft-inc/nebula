/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <sys/resource.h>

#include "common/base/Base.h"
#include "common/datatypes/Vector.h"
#ifdef ENABLE_MEMORY_TRACKER
#include "common/memory/MemoryTracker.h"
#endif
#include "common/vectorIndex/HNSWIndex.h"
#include "common/vectorIndex/IVFIndex.h"
#include "common/vectorIndex/VectorIndexUtils.h"

namespace nebula {

// Dataset utility functions - all datasets use fvecs format
class DatasetLoader {
 public:
  static std::vector<Vector> loadVectors(const std::string& filepath, size_t maxCount = 0) {
    return loadFloatVectors(filepath, maxCount);
  }

  static std::vector<Vector> loadFloatVectors(const std::string& filepath, size_t maxCount = 0) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
      LOG(ERROR) << "Cannot open file: " << filepath;
      return {};
    }

    uint32_t dimension, numVectors;
    file.read(reinterpret_cast<char*>(&dimension), sizeof(uint32_t));
    file.seekg(0, std::ios::end);
    auto fsize = file.tellg();
    numVectors = static_cast<size_t>(fsize) / ((dimension + 1) * sizeof(float));
    if (maxCount > 0 && maxCount < numVectors) {
      numVectors = maxCount;
    }
    LOG(INFO) << "Loading " << numVectors << " float vectors with dimension " << dimension
              << " from " << filepath;

    std::vector<Vector> vectors;
    vectors.reserve(numVectors);
    file.seekg(0, std::ios::beg);
    while (file.peek() != std::ifstream::traits_type::eof() && vectors.size() < numVectors) {
      std::vector<float> floatData(dimension);
      file.seekg(sizeof(uint32_t), std::ios::cur);
      file.read(reinterpret_cast<char*>(floatData.data()), dimension * sizeof(float));
      vectors.emplace_back(std::move(floatData));
    }

    file.close();
    LOG(INFO) << "Successfully loaded " << vectors.size() << " float vectors";
    return vectors;
  }

  static std::vector<std::vector<int32_t>> loadGroundTruth(const std::string& filepath,
                                                           size_t maxQueries = 0) {
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
      LOG(ERROR) << "Cannot open ground truth file: " << filepath;
      return {};
    }

    // 读取第一个向量的维度信息
    uint32_t dimension;
    file.read(reinterpret_cast<char*>(&dimension), sizeof(uint32_t));
    file.seekg(0, std::ios::end);
    auto fsize = file.tellg();

    // 正确计算向量数量：每个向量 = dimension信息(4字节) + dimension个int32数据
    size_t numQueries = static_cast<size_t>(fsize) / ((1 + dimension) * sizeof(uint32_t));
    if (maxQueries > 0 && maxQueries < numQueries) {
      numQueries = maxQueries;
    }
    LOG(INFO) << "Loading " << numQueries << " ground truth vectors with dimension " << dimension
              << " from " << filepath;

    file.seekg(0, std::ios::beg);
    std::vector<std::vector<int32_t>> groundTruth;
    groundTruth.reserve(numQueries);

    for (size_t i = 0; i < numQueries; ++i) {
      uint32_t vecDim;
      file.read(reinterpret_cast<char*>(&vecDim), sizeof(uint32_t));
      if (vecDim != dimension) {
        LOG(WARNING) << "Dimension mismatch at vector " << i << ": expected " << dimension
                     << ", got " << vecDim;
      }
      std::vector<int32_t> neighbors(vecDim);
      file.read(reinterpret_cast<char*>(neighbors.data()), vecDim * sizeof(int32_t));
      groundTruth.emplace_back(std::move(neighbors));
    }

    file.close();
    LOG(INFO) << "Successfully loaded ground truth for " << groundTruth.size() << " queries";
    return groundTruth;
  }
};

// 内存监控辅助类
class MemoryMonitor {
 public:
  // 记录内存使用的快照
  static int64_t getCurrentMemoryUsage() {
    int64_t used = memory::MemoryStats::instance().used();
    int64_t limit = memory::MemoryStats::instance().getLimit();
    bool isOn = memory::MemoryTracker::isOn();
    LOG(INFO) << "MemoryTracker Debug - Used: " << used << ", Limit: " << limit
              << ", IsOn: " << isOn;
    return used;
  }

  static int64_t getMemoryUsage() {
    int64_t trackerMemory = memory::MemoryStats::instance().used();
    if (trackerMemory > 0) {
      return trackerMemory;
    }
    LOG(WARNING) << "All memory monitoring methods returned 0";
    return 0;
  }

  static int64_t calculateMemoryIncrease(int64_t start, int64_t end) {
    return end - start;
  }

  // 格式化内存大小为可读字符串
  static std::string formatMemorySize(int64_t bytes) {
    constexpr int64_t KiB = 1024;
    constexpr int64_t MiB = 1024 * KiB;
    constexpr int64_t GiB = 1024 * MiB;

    if (bytes < KiB) {
      return std::to_string(bytes) + "B";
    } else if (bytes < MiB) {
      return std::to_string(bytes / KiB) + "." +
             std::to_string((bytes % KiB) * 1000 / KiB).substr(0, 3) + "KiB";
    } else if (bytes < GiB) {
      return std::to_string(bytes / MiB) + "." +
             std::to_string((bytes % MiB) * 1000 / MiB).substr(0, 3) + "MiB";
    } else {
      return std::to_string(bytes / GiB) + "." +
             std::to_string((bytes % GiB) * 1000 / GiB).substr(0, 3) + "GiB";
    }
  }

  static int64_t recordBaseline() {
    return getMemoryUsage();
  }

  static int64_t getPeakMemoryUsage(int64_t baseline) {
    return getMemoryUsage() - baseline;
  }
};

class BenchmarkMetrics {
 public:
  static double calculateRecall(const std::vector<int32_t>& predicted,
                                const std::vector<int32_t>& groundTruth,
                                size_t k) {
    std::unordered_set<int32_t> truthSet(groundTruth.begin(),
                                         groundTruth.begin() + std::min(k, groundTruth.size()));
    size_t hits = 0;
    size_t checkCount = std::min(k, predicted.size());

    for (size_t i = 0; i < checkCount; ++i) {
      if (truthSet.count(predicted[i]) > 0) {
        hits++;
      }
    }

    double recall = static_cast<double>(hits) / std::min(k, groundTruth.size());
    return recall;
  }

  static double calculateAverageRecall(const std::vector<std::vector<int32_t>>& predicted,
                                       const std::vector<std::vector<int32_t>>& groundTruth,
                                       size_t k) {
    if (predicted.size() != groundTruth.size()) {
      return 0.0;
    }

    double totalRecall = 0.0;
    for (size_t i = 0; i < predicted.size(); ++i) {
      totalRecall += calculateRecall(predicted[i], groundTruth[i], k);
    }

    return totalRecall / predicted.size();
  }

  // Queries Per Second
  static double calculateQPS(size_t numQueries, const std::chrono::milliseconds& totalTime) {
    if (totalTime.count() == 0) return 0.0;
    return static_cast<double>(numQueries) * 1000.0 / totalTime.count();
  }
};

class ANNBenchmarkTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // New API related identifiers
    graphID_ = 1;
    partitionID_ = 1;
    indexId_ = 100;
    indexName_ = "ann_bench";
    propFromNode_ = true;
    rootPath_ = "/tmp/nebula_ann_bench";
    metricType_ = MetricType::L2;
    minTrainDataSize_ = 3;

    // Legacy kept for convenience
    spaceId_ = graphID_;
    setupDatasetConfig();

    // HNSW Params
    maxElements_ = 1000010;
    M_ = 16;
    efConstruction_ = 200;
    ef_search_ = 500;

    // IVF Params
    nlist_ = 2048;
    nprob_ = 64;

    LOG(INFO) << "MemoryTracker status: " << (memory::MemoryTracker::isOn() ? "ON" : "OFF");
    LOG(INFO) << "Current memory stats: " << memory::MemoryStats::instance().toString();

    // 尝试启用内存跟踪
    memory::MemoryStats::turnOnThrow();

    // 设置一个合理的内存限制（如果当前限制过大）
    int64_t currentLimit = memory::MemoryStats::instance().getLimit();
    if (currentLimit == std::numeric_limits<int64_t>::max()) {
      // 设置一个8GB的限制用于测试
      memory::MemoryStats::instance().setLimit(8LL * 1024 * 1024 * 1024);
      LOG(INFO) << "Set memory limit to 8GB for testing";
    }
    LOG(INFO) << "Updated memory stats: " << memory::MemoryStats::instance().toString();

    LOG(INFO) << "Setting up ANN benchmark test for " << datasetName_ << " dataset...";
    LOG(INFO) << "Vector dimension: " << dim_;
  }

  void setupDatasetConfig() {
    setupSIFTConfig();
  }

  // SIFT
  void setupSIFTConfig() {
    datasetName_ = "SIFT1M";
    dim_ = 128;
    baseDataPath_ = "/home/lzy/data/sift/sift_base.fvecs";
    learnDataPath_ = "/home/lzy/data/sift/sift_learn.fvecs";
    queryDataPath_ = "/home/lzy/data/sift/sift_query.fvecs";
    groundTruthPath_ = "/home/lzy/data/sift/sift_groundtruth.ivecs";
    maxTrainVectors_ = 1000000;
    maxlearnVectors_ = 100000;
    maxQueryVectors_ = 10000;
  }

  bool checkDataFiles() {
    std::ifstream base(baseDataPath_);
    std::ifstream query(queryDataPath_);
    std::ifstream truth(groundTruthPath_);

    bool filesExist = base.good() && query.good() && truth.good();
    if (!filesExist) {
      LOG(WARNING) << datasetName_ << " dataset files not found. Please download:";
      LOG(WARNING) << "- " << baseDataPath_;
      LOG(WARNING) << "- " << queryDataPath_;
      LOG(WARNING) << "- " << groundTruthPath_;
      LOG(WARNING) << "From: http://corpus-texmex.irisa.fr/";
    }

    return filesExist;
  }

  void loadTrainData() {
    if (trainVectors_.empty()) {
      trainVectors_ = DatasetLoader::loadVectors(baseDataPath_, maxTrainVectors_);
    }
  }

  void loadLearnData() {
    if (learnVectors_.empty()) {
      learnVectors_ = DatasetLoader::loadVectors(learnDataPath_, maxlearnVectors_);
    }
  }

  void loadQueryData() {
    if (queryVectors_.empty()) {
      queryVectors_ = DatasetLoader::loadVectors(queryDataPath_, maxQueryVectors_);
    }
  }

  void loadGroundTruth() {
    if (groundTruth_.empty()) {
      groundTruth_ = DatasetLoader::loadGroundTruth(groundTruthPath_, maxQueryVectors_);
    }
  }

  // 持有式 VecData 封装，保证底层指针有效
  OwnedVecData makeOwnedVecData(const std::vector<Vector>& vecs, size_t dim) {
    OwnedVecData owned;
    owned.flat.reserve(vecs.size() * dim);
    owned.ids.reserve(vecs.size());
    for (size_t i = 0; i < vecs.size(); ++i) {
      const auto& vals = vecs[i].data();
      owned.flat.insert(owned.flat.end(), vals.begin(), vals.end());
      owned.ids.emplace_back(static_cast<VectorID>(i));
    }
    owned.view.cnt = vecs.size();
    owned.view.dim = dim;
    owned.view.fdata = owned.flat.data();
    owned.view.ids = owned.ids.data();
    return owned;
  }

  OwnedVecData makeOwnedVecData(const std::vector<Vector>& trainVecs,
                                const std::vector<Vector>& learnVecs,
                                size_t dim) {
    OwnedVecData owned;
    owned.flat.reserve(trainVecs.size() * dim + learnVecs.size() * dim);
    owned.ids.reserve(trainVecs.size());
    for (size_t i = 0; i < learnVecs.size(); ++i) {
      const auto& vals = trainVecs[i].data();
      owned.flat.insert(owned.flat.end(), vals.begin(), vals.end());
    }
    for (size_t i = 0; i < trainVecs.size(); ++i) {
      const auto& vals = trainVecs[i].data();
      owned.flat.insert(owned.flat.end(), vals.begin(), vals.end());
      owned.ids.emplace_back(static_cast<VectorID>(i));
    }
    owned.view.cnt = trainVecs.size() + learnVecs.size();
    owned.view.dim = dim;
    owned.view.fdata = owned.flat.data();
    owned.view.ids = owned.ids.data();
    return owned;
  }

  // Build HNSW Index
  std::unique_ptr<HNSWIndex> buildHNSWIndex() {
    auto index = std::make_unique<HNSWIndex>(graphID_,
                                             partitionID_,
                                             indexId_,
                                             indexName_,
                                             propFromNode_,
                                             dim_,
                                             rootPath_,
                                             metricType_,
                                             minTrainDataSize_);
    BuildParamsHNSW params(
        metricType_, M_, efConstruction_, std::max(maxElements_, trainVectors_.size() + 10));
    auto st = index->init(&params);
    if (!st.ok()) {
      LOG(ERROR) << "HNSW init failed: " << st.toString();
      return nullptr;
    }
    auto owned = makeOwnedVecData(trainVectors_, dim_);
    st = index->add(&owned.view, true);
    if (!st.ok()) {
      LOG(ERROR) << "HNSW add failed: " << st.toString();
      return nullptr;
    }
    return index;
  }

  // build IVF index
  std::unique_ptr<IVFIndex> buildIVFIndex() {
    auto index = std::make_unique<IVFIndex>(graphID_,
                                            partitionID_,
                                            indexId_,
                                            indexName_,
                                            propFromNode_,
                                            dim_,
                                            rootPath_,
                                            metricType_,
                                            minTrainDataSize_);
    size_t trainSize = learnVectors_.empty() ? std::min<size_t>(10000, trainVectors_.size())
                                             : learnVectors_.size();
    LOG(ERROR) << "Train size: " << trainSize;
    BuildParamsIVF params(metricType_, AnnIndexType::IVF, nlist_, trainSize);
    auto st = index->init(&params);
    if (!st.ok()) {
      LOG(ERROR) << "IVF init failed: " << st.toString();
      return nullptr;
    }
    auto owned = makeOwnedVecData(trainVectors_, learnVectors_, dim_);
    st = index->add(&owned.view, true);
    if (!st.ok()) {
      LOG(ERROR) << "IVF add failed: " << st.toString();
      return nullptr;
    }
    return index;
  }

  // Evaluate HNSW：Return {recall, qps}
  std::pair<double, double> evaluateHNSW(HNSWIndex& index, size_t k, size_t efSearchOverride = 0) {
    size_t ef = efSearchOverride ? efSearchOverride : ef_search_;
    std::vector<std::vector<int32_t>> predictions;
    predictions.reserve(queryVectors_.size());

    auto t0 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < queryVectors_.size(); ++i) {
      const auto& vals = queryVectors_[i].data();
      SearchParamsHNSW params(k, const_cast<float*>(vals.data()), dim_, ef);
      SearchResult result;
      auto st = index.search(&params, &result);
      if (!st.ok()) {
        LOG(ERROR) << "HNSW query failed at " << i << ": " << st.toString();
        return {0.0, 0.0};
      }
      std::vector<int32_t> ids;
      ids.reserve(result.IDs.size());
      for (auto vid : result.IDs) {
        ids.emplace_back(static_cast<int32_t>(vid));
      }
      predictions.emplace_back(std::move(ids));
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    auto durMs = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);

    double recall = BenchmarkMetrics::calculateAverageRecall(predictions, groundTruth_, k);
    double qps = BenchmarkMetrics::calculateQPS(queryVectors_.size(), durMs);
    return {recall, qps};
  }

  // Evaluate IVF：Return {recall, qps}
  std::pair<double, double> evaluateIVF(IVFIndex& index,
                                        size_t k,
                                        size_t nprobOverride = 0,
                                        size_t searchTopKOverride = 0) {
    size_t nprobe = nprobOverride ? nprobOverride : nprob_;
    size_t topK = searchTopKOverride ? searchTopKOverride : k;

    std::vector<std::vector<int32_t>> predictions;
    predictions.reserve(queryVectors_.size());

    auto t0 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < queryVectors_.size(); ++i) {
      const auto& vals = queryVectors_[i].data();
      SearchParamsIVF params(topK, const_cast<float*>(vals.data()), dim_, nprobe);
      SearchResult result;
      auto st = index.search(&params, &result);
      if (!st.ok()) {
        LOG(ERROR) << "IVF query failed at " << i << ": " << st.toString();
        return {0.0, 0.0};
      }
      std::vector<int32_t> ids;
      ids.reserve(result.IDs.size());
      for (auto vid : result.IDs) {
        ids.emplace_back(static_cast<int32_t>(vid));
      }
      predictions.emplace_back(std::move(ids));
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    auto durMs = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);

    double recall = BenchmarkMetrics::calculateAverageRecall(predictions, groundTruth_, k);
    double qps = BenchmarkMetrics::calculateQPS(queryVectors_.size(), durMs);
    return {recall, qps};
  }

 public:
  void ensureDataLoadedForHNSW() {
    loadTrainData();
    loadQueryData();
    loadGroundTruth();
  }

  void ensureDataLoadedForIVF() {
    loadTrainData();
    loadLearnData();
    loadQueryData();
    loadGroundTruth();
  }

 protected:
  GraphSpaceID spaceId_;
  IndexID indexId_;

  GraphSpaceID graphID_;
  PartitionID partitionID_;
  std::string indexName_;
  bool propFromNode_;
  std::string rootPath_;
  MetricType metricType_;
  size_t minTrainDataSize_;
  std::string datasetName_;
  size_t dim_;
  size_t maxElements_;
  size_t M_;
  size_t efConstruction_;
  size_t nlist_;
  size_t nprob_;
  size_t ef_search_;
  size_t searchTopK_ = 100;

  std::string baseDataPath_;
  std::string learnDataPath_;
  std::string queryDataPath_;
  std::string groundTruthPath_;

  size_t maxTrainVectors_;
  size_t maxlearnVectors_;
  size_t maxQueryVectors_;

  std::vector<Vector> trainVectors_;
  std::vector<Vector> learnVectors_;
  std::vector<Vector> queryVectors_;
  std::vector<std::vector<int32_t>> groundTruth_;
};

TEST_F(ANNBenchmarkTest, HNSWBenchmarkWithSIFT1M) {
  if (!checkDataFiles()) {
    GTEST_SKIP() << datasetName_ << " dataset files not found";
  }

  LOG(INFO) << "Starting HNSW benchmark with SIFT1M dataset";
  ensureDataLoadedForHNSW();

  ASSERT_FALSE(trainVectors_.empty()) << "Failed to load training vectors";
  ASSERT_FALSE(queryVectors_.empty()) << "Failed to load query vectors";
  ASSERT_FALSE(groundTruth_.empty()) << "Failed to load ground truth";

  // baseline memory
  int64_t memoryBaseline = MemoryMonitor::recordBaseline();
  LOG(INFO) << "Memory Train Data: " << MemoryMonitor::formatMemorySize(memoryBaseline);
  auto buildTime = std::chrono::high_resolution_clock::now();
  auto index = buildHNSWIndex();
  ASSERT_NE(index, nullptr) << "Failed to build HNSW index";

  int64_t memoryAfterInit = MemoryMonitor::getMemoryUsage();
  int64_t initMemoryUsage = memoryAfterInit - memoryBaseline;
  LOG(INFO) << "Memory for extra data structure : "
            << MemoryMonitor::formatMemorySize(initMemoryUsage);

  LOG(INFO) << "HNSW index built in " << buildTime.time_since_epoch().count() / 1000 << " s";
  LOG(INFO) << "Index built for vectors: " << trainVectors_.size();
  LOG(INFO) << "Memory Usage Statistics:";
  LOG(INFO) << "  Final memory usage: " << MemoryMonitor::formatMemorySize(memoryAfterInit);
  std::vector<std::vector<int32_t>> predictions;
  predictions.reserve(queryVectors_.size());

  const std::vector<size_t> kValues = {1, 10, 100};

  for (size_t k : kValues) {
    LOG(INFO) << "Testing with k=" << k;

    auto [recall, qps] = evaluateHNSW(*index, k, ef_search_);
    LOG(INFO) << "HNSW Results (ef_search =" << ef_search_ << "):";
    LOG(INFO) << "  Recall@" << k << ": " << recall;
    LOG(INFO) << "  QPS: " << qps;

    EXPECT_GT(recall, 0.5) << "Recall should be > 50% for k=" << k;
    EXPECT_GT(qps, 100) << "QPS should be > 100 for k=" << k;
  }

  int64_t memoryAfterQuery = MemoryMonitor::getMemoryUsage();
  int64_t queryMemoryUsage = memoryAfterQuery - memoryBaseline;
  LOG(INFO) << "Memory after queries: " << MemoryMonitor::formatMemorySize(queryMemoryUsage);
}

TEST_F(ANNBenchmarkTest, IVFBenchmarkWithSIFT1M) {
  if (!checkDataFiles()) {
    GTEST_SKIP() << datasetName_ << " dataset files not found";
  }

  LOG(INFO) << "Starting IVF benchmark with SIFT1M dataset";

  int64_t memoryBaseline = MemoryMonitor::recordBaseline();
  LOG(INFO) << "Memory baseline: " << MemoryMonitor::formatMemorySize(memoryBaseline);

  loadTrainData();
  loadLearnData();
  loadQueryData();
  loadGroundTruth();

  ASSERT_FALSE(trainVectors_.empty()) << "Failed to load training vectors";
  ASSERT_FALSE(learnVectors_.empty()) << "Failed to load learn vectors";
  ASSERT_FALSE(queryVectors_.empty()) << "Failed to load query vectors";
  ASSERT_FALSE(groundTruth_.empty()) << "Failed to load ground truth";

  int64_t memoryAfterDataLoad = MemoryMonitor::getMemoryUsage();
  int64_t dataLoadMemoryUsage = memoryAfterDataLoad - memoryBaseline;
  LOG(INFO) << "Memory after data loading: "
            << MemoryMonitor::formatMemorySize(dataLoadMemoryUsage);

  auto index = buildIVFIndex();
  ASSERT_NE(index, nullptr) << "Failed to build IVF index";

  int64_t memoryAfterInit = MemoryMonitor::getMemoryUsage();
  int64_t initMemoryUsage = memoryAfterInit - memoryBaseline;
  LOG(INFO) << "Memory after index initialization: "
            << MemoryMonitor::formatMemorySize(initMemoryUsage);

  int64_t memoryAfterDataPrep = MemoryMonitor::getMemoryUsage();
  int64_t dataPrepMemoryUsage = memoryAfterDataPrep - memoryBaseline;
  LOG(INFO) << "Memory after data preparation: "
            << MemoryMonitor::formatMemorySize(dataPrepMemoryUsage);

  LOG(INFO) << "IVF index built for " << trainVectors_.size() << " vectors";
  LOG(INFO) << "IVF Memory Usage Statistics:";
  LOG(INFO) << "  Data loading: " << MemoryMonitor::formatMemorySize(dataLoadMemoryUsage);
  LOG(INFO) << "  After initialization: " << MemoryMonitor::formatMemorySize(initMemoryUsage);

  std::vector<std::vector<int32_t>> predictions;
  predictions.reserve(queryVectors_.size());

  const std::vector<size_t> kValues = {1, 10, 100};
  for (size_t k : kValues) {
    LOG(INFO) << "Testing with k=" << k;
    auto [recall, qps] = evaluateIVF(*index, k, nprob_, searchTopK_);
    LOG(INFO) << "IVF Results (k=" << k << "):";
    LOG(INFO) << "  Recall@" << k << ": " << recall;
    LOG(INFO) << "  QPS: " << qps;

    EXPECT_GT(recall, 0.3) << "Recall should be > 30% for k=" << k;
    EXPECT_GT(qps, 50) << "QPS should be > 50 for k=" << k;
  }

  int64_t memoryAfterQuery = MemoryMonitor::getMemoryUsage();
  int64_t queryMemoryUsage = memoryAfterQuery - memoryBaseline;
  LOG(INFO) << "Memory after queries: " << MemoryMonitor::formatMemorySize(queryMemoryUsage);
}

TEST_F(ANNBenchmarkTest, SIFT1MMemoryUsageTest) {
  if (!checkDataFiles()) {
    GTEST_SKIP() << datasetName_ << " dataset files not found";
  }

  LOG(INFO) << "Starting SIFT1M Memory Usage Test";

  const std::vector<size_t> testSizes = {1000, 10000, 100000, 1000000};

  for (size_t testSize : testSizes) {
    if (testSize > maxTrainVectors_) {
      LOG(WARNING) << "Skipping test size " << testSize << " (exceeds max train vectors)";
      continue;
    }

    LOG(INFO) << "Testing memory usage with " << testSize << " vectors";

    int64_t memoryBaseline = MemoryMonitor::recordBaseline();

    std::vector<Vector> testVectors = DatasetLoader::loadVectors(baseDataPath_, testSize);
    ASSERT_FALSE(testVectors.empty()) << "Failed to load test vectors";

    int64_t memoryAfterLoad = MemoryMonitor::getMemoryUsage();
    int64_t loadMemoryUsage = memoryAfterLoad - memoryBaseline;

    {
      LOG(INFO) << "  HNSW Index Memory Test for " << testSize << " vectors:";

      auto hnswIndex = std::make_unique<HNSWIndex>(graphID_,
                                                   partitionID_,
                                                   indexId_,
                                                   indexName_,
                                                   propFromNode_,
                                                   dim_,
                                                   rootPath_,
                                                   metricType_,
                                                   minTrainDataSize_);
      BuildParamsHNSW hparams(metricType_, M_, efConstruction_, testSize + 10);
      ASSERT_TRUE(hnswIndex->init(&hparams).ok());

      int64_t memoryAfterHNSWInit = MemoryMonitor::getMemoryUsage();
      int64_t hnswInitMemory = memoryAfterHNSWInit - memoryAfterLoad;

      auto owned = makeOwnedVecData(testVectors, dim_);
      ASSERT_TRUE(hnswIndex->add(&owned.view, true).ok());

      int64_t memoryAfterHNSWBuild = MemoryMonitor::getMemoryUsage();
      int64_t hnswTotalMemory = memoryAfterHNSWBuild - memoryAfterLoad;

      LOG(INFO) << "    Data loading: " << MemoryMonitor::formatMemorySize(loadMemoryUsage);
      LOG(INFO) << "    HNSW init: " << MemoryMonitor::formatMemorySize(hnswInitMemory);
      LOG(INFO) << "    HNSW total: " << MemoryMonitor::formatMemorySize(hnswTotalMemory);

      hnswIndex.reset();
    }

    {
      LOG(INFO) << "  IVF Index Memory Test for " << testSize << " vectors:";
      auto ivfIndex = std::make_unique<IVFIndex>(graphID_,
                                                 partitionID_,
                                                 indexId_,
                                                 indexName_,
                                                 propFromNode_,
                                                 dim_,
                                                 rootPath_,
                                                 metricType_,
                                                 minTrainDataSize_);
      BuildParamsIVF ivfParams(metricType_,
                               AnnIndexType::IVF,
                               std::min(static_cast<size_t>(2048), testSize / 10),
                               std::min(testSize / 10, static_cast<size_t>(10000)));
      ASSERT_TRUE(ivfIndex->init(&ivfParams).ok());

      size_t learnSize = std::min(testSize / 10, static_cast<size_t>(10000));
      std::vector<Vector> learnFlatData = DatasetLoader::loadVectors(learnDataPath_, learnSize);

      auto owned2 = makeOwnedVecData(testVectors, learnFlatData, dim_);
      ASSERT_TRUE(ivfIndex->add(&owned2.view, true).ok());

      int64_t memoryAfterIVFBuild = MemoryMonitor::getMemoryUsage();
      int64_t ivfTotalMemory = memoryAfterIVFBuild - memoryAfterLoad;

      LOG(INFO) << "    IVF total: " << MemoryMonitor::formatMemorySize(ivfTotalMemory);
      ivfIndex.reset();
    }
    testVectors.clear();
    LOG(INFO) << "Completed memory test for " << testSize << " vectors\n";
  }
  LOG(INFO) << "SIFT1M Memory Usage Test completed";
}

}  // namespace nebula
