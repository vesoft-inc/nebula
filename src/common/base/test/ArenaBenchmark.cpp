// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <folly/memory/Arena.h>

#include <string>
#include <type_traits>

#include "common/base/Arena.h"
#include "common/expression/LabelExpression.h"

namespace nebula {

class TestExpr : public LabelExpression {
 public:
  explicit TestExpr(const std::string &name = "")
      : LabelExpression(reinterpret_cast<ObjectPool *>(1), name) {}
};

BENCHMARK(DefaultAllocator, iters) {
  std::size_t round = iters * 1000;
  for (std::size_t _ = 0; _ < round; ++_) {
    auto *expr = new TestExpr("Label");
    delete expr;
  }
}

BENCHMARK_RELATIVE(ArenaAllocator, iters) {
  std::size_t round = iters * 1000;
  Arena a;
  for (std::size_t _ = 0; _ < round; ++_) {
    auto *ptr = a.allocateAligned(sizeof(TestExpr));
    auto *expr = new (ptr) TestExpr("Label");
    expr->~TestExpr();
  }
}

BENCHMARK_RELATIVE(FollyArenaAllocator, iters) {
  std::size_t round = iters * 1000;
  folly::SysArena a;
  for (std::size_t _ = 0; _ < round; ++_) {
    auto *ptr = a.allocate(sizeof(TestExpr));
    auto *expr = new (ptr) TestExpr("Label");
    expr->~TestExpr();
  }
}

BENCHMARK_DRAW_LINE();

}  // namespace nebula

int main(int argc, char **argv) {
  folly::init(&argc, &argv, true);

  folly::runBenchmarks();
  return 0;
}

// CPU info
// Brand Raw: Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz
// Hz Advertised Friendly: 3.0000 GHz
// Hz Actual Friendly: 3.2942 GHz
// Hz Advertised: (3000000000, 0)
// Hz Actual: (3294220000, 0)
// Arch: X86_64
// Bits: 64
// Count: 40
// Arch String Raw: x86_64
// L1 Data Cache Size: 32768
// L1 Instruction Cache Size: 32768
// L2 Cache Size: 262144
// L2 Cache Line Size: 256
// L2 Cache Associativity: 6
// L3 Cache Size: 26214400
//
// Build in Release mode
//
// ============================================================================
// /home/shylock.huang/nebula/src/common/base/test/ArenaBenchmark.cpprelative  time/iter  iters/s
// ============================================================================
// DefaultAllocator                                            36.59us   27.33K
// ArenaAllocator                                   145.89%    25.08us   39.87K
// FollyArenaAllocator                              138.96%    26.33us   37.98K
// ----------------------------------------------------------------------------
// ============================================================================
