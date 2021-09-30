/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/memory/MemoryUtils.h"

#include <folly/String.h>
#include <gflags/gflags.h>

#include <cstdio>
#include <fstream>
#include <regex>

#include "common/fs/FileUtils.h"

DEFINE_bool(containerized, false, "Whether run this process inside the docker container");
DEFINE_double(system_memory_high_watermark_ratio, 0.8, "high watermark ratio of system memory");

using nebula::fs::FileUtils;

namespace nebula {

static const std::regex reMemAvailable(R"(^Mem(Available|Total):\s+(\d+)\skB$)");
static const std::regex reTotalCache(R"(^total_(cache|inactive_file)\s+(\d+)$)");

std::atomic_bool MemoryUtils::kHitMemoryHighWatermark{false};

StatusOr<bool> MemoryUtils::hitsHighWatermark() {
  double available = 0.0, total = 0.0;
  if (FLAGS_containerized) {
    FileUtils::FileLineIterator iter("/sys/fs/cgroup/memory/memory.stat", &reTotalCache);
    uint64_t cacheSize = 0;
    for (; iter.valid(); ++iter) {
      auto& sm = iter.matched();
      cacheSize += std::stoul(sm[2].str(), NULL);
    }

    auto limitStatus = MemoryUtils::readSysContents("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    NG_RETURN_IF_ERROR(limitStatus);
    uint64_t limitInBytes = std::move(limitStatus).value();

    auto usageStatus = MemoryUtils::readSysContents("/sys/fs/cgroup/memory/memory.usage_in_bytes");
    NG_RETURN_IF_ERROR(usageStatus);
    uint64_t usageInBytes = std::move(usageStatus).value();

    total = static_cast<double>(limitInBytes);
    available = static_cast<double>(limitInBytes - usageInBytes + cacheSize);
  } else {
    FileUtils::FileLineIterator iter("/proc/meminfo", &reMemAvailable);
    std::vector<uint64_t> memorySize;
    for (; iter.valid(); ++iter) {
      auto& sm = iter.matched();
      memorySize.emplace_back(std::stoul(sm[2].str(), NULL) << 10);
    }
    CHECK_EQ(memorySize.size(), 2U);
    size_t i = 0, j = 1;
    if (memorySize[0] < memorySize[1]) {
      std::swap(i, j);
    }
    total = memorySize[i];
    available = memorySize[j];
  }

  auto hits = (1 - available / total) > FLAGS_system_memory_high_watermark_ratio;
  LOG_IF_EVERY_N(WARNING, hits, 100)
      << "Memory usage has hit the high watermark of system, available: " << available
      << " vs. total: " << total << " in bytes.";
  return hits;
}

StatusOr<uint64_t> MemoryUtils::readSysContents(const std::string& path) {
  std::ifstream ifs(path);
  if (!ifs) {
    return Status::Error("Could not open the file: %s", path.c_str());
  }
  uint64_t value = 0;
  ifs >> value;
  return value;
}

}  // namespace nebula
