// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/gc/GC.h"

namespace nebula {
namespace graph {
GC& GC::instance() {
  static GC gc;
  return gc;
}

GC::GC() {
  worker_.addDelayTask(50, &GC::periodicTask, this);
}

void GC::clear(std::vector<Result>&& garbage) {
  queue_.enqueue(garbage);
}

void GC::periodicTask() {
  // TODO: maybe could release by batch
  queue_.dequeue();
  worker_.addDelayTask(50, &GC::periodicTask, this);
}
}  // namespace graph
}  // namespace nebula
