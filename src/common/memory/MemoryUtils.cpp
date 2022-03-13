/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/memory/MemoryUtils.h"

#include <gflags/gflags.h>

#include <algorithm>
#include <fstream>

#include "common/fs/FileUtils.h"

DEFINE_bool(containerized, false, "Whether run this process inside the docker container");
DEFINE_double(system_memory_high_watermark_ratio, 0.8, "high watermark ratio of system memory");

using nebula::fs::FileUtils;

namespace nebula {

static const std::regex reMemAvailable(
    R"(^Mem(Available|Free|Total):\s+(\d+)\skB$)");  // when can't use MemAvailable, use MemFree
                                                     // instead.
static const std::regex reTotalCache(R"(^total_(cache|inactive_file)\s+(\d+)$)");

std::atomic_bool MemoryUtils::kHitMemoryHighWatermark{false};

StatusOr<bool> MemoryUtils::hitsHighWatermark() {
  if (FLAGS_system_memory_high_watermark_ratio >= 1.0) {
    return false;
  }
  double available = 0.0, total = 0.0;
  if (FLAGS_containerized) {
    bool cgroupsv2 = FileUtils::exist("/sys/fs/cgroup/cgroup.controllers");
    std::string statPath =
        cgroupsv2 ? "/sys/fs/cgroup/memory.stat" : "/sys/fs/cgroup/memory/memory.stat";
    FileUtils::FileLineIterator iter(statPath, &reTotalCache);
    uint64_t cacheSize = 0;
    for (; iter.valid(); ++iter) {
      auto& sm = iter.matched();
      cacheSize += std::stoul(sm[2].str(), nullptr);
    }

    std::string limitPath =
        cgroupsv2 ? "/sys/fs/cgroup/memory.max" : "/sys/fs/cgroup/memory/memory.limit_in_bytes";
    auto limitStatus = MemoryUtils::readSysContents(limitPath);
    NG_RETURN_IF_ERROR(limitStatus);
    uint64_t limitInBytes = std::move(limitStatus).value();

    std::string usagePath =
        cgroupsv2 ? "/sys/fs/cgroup/memory.current" : "/sys/fs/cgroup/memory/memory.usage_in_bytes";
    auto usageStatus = MemoryUtils::readSysContents(usagePath);
    NG_RETURN_IF_ERROR(usageStatus);
    uint64_t usageInBytes = std::move(usageStatus).value();

    total = static_cast<double>(limitInBytes);
    available = static_cast<double>(limitInBytes - usageInBytes + cacheSize);
  } else {
    FileUtils::FileLineIterator iter("/proc/meminfo", &reMemAvailable);
    std::vector<uint64_t> memorySize;
    for (; iter.valid(); ++iter) {
      auto& sm = iter.matched();
      memorySize.emplace_back(std::stoul(sm[2].str(), nullptr) << 10);
    }
    std::sort(memorySize.begin(), memorySize.end());
    if (memorySize.size() >= 2u) {
      total = memorySize.back();
      available = memorySize[memorySize.size() - 2];
    } else {
      return false;
    }
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
