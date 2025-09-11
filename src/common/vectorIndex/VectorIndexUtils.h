/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef COMMON_VECTORINDEX_VECTOR_INDEX_H_
#define COMMON_VECTORINDEX_VECTOR_INDEX_H_
#include <thrift/lib/cpp/Thrift.h>

#include <cstring>

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
namespace nebula {

enum MetricType : int8_t { L2, INNER_PRODUCT, METRIC_INVALID };
enum AnnIndexType : int8_t { IVF, HNSW, ANN_INVALID };

struct IDSelector {
  size_t cnt;
  VectorID* ids;  // vector of IDs to select
  std::string toDebugString() const {
    std::string str;
    str += "cnt: " + std::to_string(cnt) + "\n";
    str += "ids: [";
    for (size_t i = 0; i < cnt; i++) {
      str += std::to_string(ids[i]) + ", ";
    }
    str += "]\n";
    return str;
  }
};

struct VecData {
  size_t cnt;     // number of vectors
  size_t dim;     // dimension of each vector
  float* fdata;   // float type vector data source
  VectorID* ids;  // int64 identifier of each vector
  // Only for debug
  std::string toDebugString() const {
    std::string str;
    str += "cnt: " + std::to_string(cnt) + ", dim: " + std::to_string(dim) + "\n";
    for (size_t i = 0; i < cnt; i++) {
      str += "id: " + std::to_string(ids[i]) + ", ";
      str += "data: [";
      for (size_t j = 0; j < dim; j++) {
        str += std::to_string(fdata[i * dim + j]) + ", ";
      }
      str += "]\n";
    }
    return str;
  }
};

struct OwnedVecData {
  std::vector<float> flat;
  std::vector<VectorID> ids;
  VecData view;
};

// ANN index build parameters
struct BuildParams {
  MetricType metricType{MetricType::L2};
  AnnIndexType indexType{AnnIndexType::IVF};
  BuildParams() = default;
  BuildParams(MetricType metric, AnnIndexType type) : metricType(metric), indexType(type) {}
  virtual ~BuildParams() = default;
};

struct BuildParamsIVF final : public BuildParams {
  size_t nl{3};  // number of lists
  size_t ts{3};  // train size
  BuildParamsIVF() = default;
  explicit BuildParamsIVF(MetricType metric, AnnIndexType, size_t numLists, size_t trainSize)
      : BuildParams(metric, AnnIndexType::IVF), nl(numLists), ts(trainSize) {}
};

struct BuildParamsHNSW final : public BuildParams {
  size_t maxDegree{16};      // the maximum degrees
  size_t efConstruction{8};  // expansion in construction time
  size_t capacity{10000};    // capacity of the index
  BuildParamsHNSW() = default;
  explicit BuildParamsHNSW(MetricType metric, size_t degree, size_t construction, size_t cap)
      : BuildParams(metric, AnnIndexType::HNSW),
        maxDegree(degree),
        efConstruction(construction),
        capacity(cap) {}
};

struct SearchParams {
  size_t topK{10};        // number of nearest neighbors to search
  float* query{nullptr};  // query vector data
  size_t queryDim{0};     // dimension of query vector
  SearchParams() = default;
  explicit SearchParams(size_t k, float* queryVector, size_t queryVectorDim)
      : topK(k), query(queryVector), queryDim(queryVectorDim) {}
  virtual ~SearchParams() = default;
};

struct SearchParamsIVF final : public SearchParams {
  size_t nprobe{10};  // number of lists to probe
  SearchParamsIVF() = default;
  explicit SearchParamsIVF(size_t k, float* queryVector, size_t queryVectorDim, size_t numProbe)
      : SearchParams(k, queryVector, queryVectorDim), nprobe(numProbe) {}
};

struct SearchParamsHNSW final : public SearchParams {
  size_t efSearch{16};  // expansion factor at search time
  SearchParamsHNSW() = default;
  explicit SearchParamsHNSW(size_t k, float* queryVector, size_t queryVectorDim, size_t searchParam)
      : SearchParams(k, queryVector, queryVectorDim), efSearch(searchParam) {}
};

// ANN search result
struct SearchResult {
  std::vector<VectorID> IDs;
  // distances of the result vectors
  std::vector<float> distances;
  // result vectors
  std::vector<float> vectors;

  void clear() {
    IDs.clear();
    distances.clear();
    vectors.clear();
  }

  void shrinkToFit() {
    size_t sz = IDs.size();
    for (auto it = IDs.rbegin(); it != IDs.rend(); it++) {
      if ((*it) != -1) {
        sz = IDs.size() - std::distance(IDs.rbegin(), it);
        break;
      } else {
        sz = 0;
      }
    }
    IDs.resize(sz);
    distances.resize(sz);
  }

  std::string debugString() const {
    std::string str;
    auto n = IDs.size();
    if (n == 0) {
      return str;
    }
    auto dim = vectors.size() / n;
    if (dim == 0) {
      return str;
    }
    for (auto i = 0U; i < n; i++) {
      str += "vector(" + std::to_string(IDs[i]) + "): [";
      for (auto j = 0U; j < dim; j++) {
        str += std::to_string(vectors[i * dim + j]) + ", ";
      }
      str += "]\n";
    }
    return str;
  }
};

struct AnnRaftLog {
  VectorID vecId;
  std::string vecVal;      // the insert vector value or the vector ID for remove
  bool putOrRemove{true};  // true: PUT or UPSERT, PUT by default
};
}  // namespace nebula

// Thrift enum traits specializations
namespace apache {
namespace thrift {

template <>
struct TEnumTraits<nebula::MetricType> {
  static constexpr size_t kMetricTypeSize = 3;
  static const nebula::MetricType values[kMetricTypeSize];
  static const char* names[kMetricTypeSize];

  static const char* findName(nebula::MetricType value) {
    switch (value) {
      case nebula::MetricType::L2:
        return "L2";
      case nebula::MetricType::INNER_PRODUCT:
        return "INNER_PRODUCT";
      case nebula::MetricType::METRIC_INVALID:
        return "METRIC_INVALID";
      default:
        return nullptr;
    }
  }

  static bool findValue(const char* name, nebula::MetricType* out) {
    if (name == nullptr) return false;
    if (std::strcmp(name, "L2") == 0) {
      *out = nebula::MetricType::L2;
      return true;
    }
    if (std::strcmp(name, "INNER_PRODUCT") == 0) {
      *out = nebula::MetricType::INNER_PRODUCT;
      return true;
    }
    if (std::strcmp(name, "METRIC_INVALID") == 0) {
      *out = nebula::MetricType::METRIC_INVALID;
      return true;
    }
    return false;
  }
};

template <>
struct TEnumTraits<nebula::AnnIndexType> {
  static constexpr size_t kAnnIndexTypeSize = 3;
  static const nebula::AnnIndexType values[kAnnIndexTypeSize];
  static const char* names[kAnnIndexTypeSize];

  static const char* findName(nebula::AnnIndexType value) {
    switch (value) {
      case nebula::AnnIndexType::IVF:
        return "IVF";
      case nebula::AnnIndexType::HNSW:
        return "HNSW";
      case nebula::AnnIndexType::ANN_INVALID:
        return "ANN_INVALID";
      default:
        return nullptr;
    }
  }

  static bool findValue(const char* name, nebula::AnnIndexType* out) {
    if (name == nullptr) return false;
    if (std::strcmp(name, "IVF") == 0) {
      *out = nebula::AnnIndexType::IVF;
      return true;
    }
    if (std::strcmp(name, "HNSW") == 0) {
      *out = nebula::AnnIndexType::HNSW;
      return true;
    }
    if (std::strcmp(name, "ANN_INVALID") == 0) {
      *out = nebula::AnnIndexType::ANN_INVALID;
      return true;
    }
    return false;
  }
};

}  // namespace thrift
}  // namespace apache

#endif
