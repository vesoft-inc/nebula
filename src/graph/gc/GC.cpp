// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/gc/GC.h"

namespace nebula {
namespace graph {
folly::UMPSCQueue<std::vector<Result>, false> GC::queue_;
}  // namespace graph
}  // namespace nebula
