/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>
#include <gtest/gtest.h>

#include <string>
#include <unordered_set>
#include <vector>

#include "common/base/Base.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Value.h"
#include "common/datatypes/Vertex.h"

namespace nebula {

static constexpr size_t kNumRecords = 3000000;
static constexpr size_t kNumColumns = 8;

static const int seed = folly::randomNumberSeed();
using RandomT = std::mt19937;
static RandomT rng(seed);

template <class Integral1, class Integral2>
Integral2 random(Integral1 low, Integral2 up) {
  std::uniform_int_distribution<> range(low, up);
  return range(rng);
}

std::string randomString(size_t size = 15) {
  std::string str(size, ' ');
  for (size_t p = 0; p < size; p++) {
    str[p] = random('a', 'z');
  }
  return str;
}

Map randomMap(size_t size = 8) {
  std::unordered_map<std::string, Value> values;
  for (size_t i = 0; i < size; ++i) {
    values.emplace(randomString(), Value(random(0, 100)));
  }

  return Map(values);
}

void randomIntDataSet(DataSet* ds, size_t numRecords, size_t numColumns) {
  for (size_t i = 0; i < numRecords; ++i) {
    Row row;
    row.reserve(numColumns);
    for (size_t j = 0; j < numColumns; ++j) {
      row.values.emplace_back(Value(random(0, 100)));
    }
    ds->rows.emplace_back(std::move(row));
  }
}

void randomStrDataSet(DataSet* ds, size_t numRecords, size_t numColumns, size_t strSize) {
  for (size_t i = 0; i < numRecords; ++i) {
    Row row;
    row.reserve(numColumns);
    for (size_t j = 0; j < numColumns; ++j) {
      Value tmp(randomString(strSize));
      row.values.emplace_back(tmp);
    }
    ds->rows.emplace_back(std::move(row));
  }
}

void randomMapDataSet(DataSet* ds, size_t numRecords, size_t numColumns) {
  for (size_t i = 0; i < numRecords; ++i) {
    Row row;
    row.reserve(numColumns);
    for (size_t j = 0; j < numColumns; ++j) {
      Value tmp(randomMap());
      row.values.emplace_back(tmp);
    }
    ds->rows.emplace_back(std::move(row));
  }
}

// BENCHMARK(ConstructIntDataSet) {
//   auto* ds = new DataSet();
//   ds->rows.reserve(kNumRecords);
//   randomIntDataSet(ds, kNumRecords, kNumColumns);
//   BENCHMARK_SUSPEND {
//     delete ds;
//   }
// }

// BENCHMARK(ConstructIntDataSet2) {
//   auto* ds = new DataSet();
//   // ds->rows.reserve(kNumRecords);
//   randomIntDataSet(ds, kNumRecords, kNumColumns);
//   BENCHMARK_SUSPEND {
//     delete ds;
//   }
// }

// BENCHMARK(ConstructStrDataSet) {
//   auto* ds = new DataSet();
//   ds->rows.reserve(kNumRecords);
//   randomStrDataSet(ds, kNumRecords, kNumColumns);
//   BENCHMARK_SUSPEND {
//     delete ds;
//   }
// }

// BENCHMARK_DRAW_LINE();

void destroyIntDSTest(size_t numRecords, size_t numColumns) {
  std::cout << "destroyIntDSTest" << std::endl;
  auto* ds = new DataSet();
  BENCHMARK_SUSPEND {
    ds->rows.reserve(kNumRecords);
    randomIntDataSet(ds, numRecords, numColumns);
  }

  delete ds;
}

BENCHMARK(DestructIntDataSet) {
  auto* ds = new DataSet();
  BENCHMARK_SUSPEND {
    ds->rows.reserve(kNumRecords);
    randomIntDataSet(ds, kNumRecords, kNumColumns);
  }

  delete ds;
  // std::cout << "\n-----------------Start delete------------------" << std::endl;
  // delete ds;
  // std::cout << "\n-----------------End  delete-------------------" << std::endl;
}

BENCHMARK_DRAW_LINE();

BENCHMARK(DestructSmallStrDataSet) {
  auto* ds = new DataSet();
  BENCHMARK_SUSPEND {
    ds->rows.reserve(kNumRecords);
    randomStrDataSet(ds, kNumRecords, kNumColumns, 10);
  }

  delete ds;
  // std::cout << "\n-----------------Start delete------------------" << std::endl;
  // delete ds;
  // std::cout << "\n-----------------End  delete-------------------" << std::endl;
}

BENCHMARK(DestructMediumStrDataSet) {
  auto* ds = new DataSet();
  BENCHMARK_SUSPEND {
    ds->rows.reserve(kNumRecords);
    randomStrDataSet(ds, kNumRecords, kNumColumns, 100);
  }

  delete ds;
  // std::cout << "\n-----------------Start delete------------------" << std::endl;
  // delete ds;
  // std::cout << "\n-----------------End  delete-------------------" << std::endl;
}

BENCHMARK(DestructLargeStrDataSet) {
  auto* ds = new DataSet();
  BENCHMARK_SUSPEND {
    ds->rows.reserve(kNumRecords);
    randomStrDataSet(ds, kNumRecords, kNumColumns, 1000);
  }

  delete ds;
  // std::cout << "\n-----------------Start delete------------------" << std::endl;
  // delete ds;
  // std::cout << "\n-----------------End  delete-------------------" << std::endl;
}

BENCHMARK_DRAW_LINE();

BENCHMARK(DestructMapDataSet) {
  auto* ds = new DataSet();
  BENCHMARK_SUSPEND {
    ds->rows.reserve(kNumRecords);
    randomMapDataSet(ds, kNumRecords, kNumColumns);
  }

  delete ds;
  // std::cout << "\n-----------------Start delete------------------" << std::endl;
  // delete ds;
  // std::cout << "\n-----------------End  delete-------------------" << std::endl;
}

// BENCHMARK(ConAndDeIntDataSet) {
//   auto* ds = new DataSet();
//   ds->rows.reserve(kNumRecords);
//   randomIntDataSet(ds, kNumRecords, kNumColumns);
//   delete ds;
// }

// BENCHMARK(ConAndDeStrDataSet) {
//   auto* ds = new DataSet();
//   ds->rows.reserve(kNumRecords);
//   randomStrDataSet(ds, kNumRecords, kNumColumns);
//   delete ds;
// }

}  // namespace nebula

int main() {
  folly::runBenchmarks();
  return 0;
}

// 300,000 records * 8 columns
// ============================================================================
// nebula/src/common/datatypes/test/DataSetBenchmark.cpprelative  time/iter  iters/s
// ============================================================================
// DestructIntDataSet                                          12.74ms    78.51
// ----------------------------------------------------------------------------
// DestructSmallStrDataSet                                     54.00ms    18.52
// DestructMediumStrDataSet                                   100.95ms     9.91
// DestructLargeStrDataSet                                    249.07ms     4.01
// ----------------------------------------------------------------------------
// DestructMapDataSet                                         679.31ms     1.47
// ============================================================================

// 3000,000 records * 8 columns
// ============================================================================
// nebula/src/common/datatypes/test/DataSetBenchmark.cpprelative  time/iter  iters/s
// ============================================================================
// DestructIntDataSet                                         134.35ms     7.44
// ----------------------------------------------------------------------------
// DestructSmallStrDataSet                                    620.12ms     1.61
// DestructMediumStrDataSet                                      1.10s  911.59m
// DestructLargeStrDataSet                                       3.44s  290.30m
// ----------------------------------------------------------------------------
// DestructMapDataSet                                            7.33s  136.37m
// ============================================================================
