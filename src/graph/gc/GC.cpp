// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/gc/GC.h"

DEFINE_uint32(gc_worker_size, 4, "Background garbage clean workers.");

namespace nebula {
namespace graph {
GC& GC::instance() {
  static GC gc;
  return gc;
}

GC::GC() {
  workers_.start(4, "GC");
  workers_.addRepeatTask(50, &GC::periodicTask, this);
}

void GC::clear(std::vector<Result>&& garbage) {
  queue_.enqueue(std::move(garbage));
}

void GC::periodicTask() {
  // TODO: maybe could release by batch
  queue_.dequeue();
}
}  // namespace graph
}  // namespace nebula
