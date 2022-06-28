// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/gc/GC.h"

#include "graph/service/GraphFlags.h"

namespace nebula {
namespace graph {
GC& GC::instance() {
  static GC gc;
  return gc;
}

GC::GC() {
  if (FLAGS_gc_worker_size == 0) {
    workers_.start(std::thread::hardware_concurrency(), "GC");
  } else {
    workers_.start(FLAGS_gc_worker_size, "GC");
  }
  workers_.addRepeatTaskForAll(50, &GC::periodicTask, this);
}

void GC::clear(std::vector<Result>&& garbage) {
  queue_.enqueue(std::move(garbage));
}

void GC::periodicTask() {
  // TODO: maybe could release by batch
  queue_.try_dequeue();
}
}  // namespace graph
}  // namespace nebula
