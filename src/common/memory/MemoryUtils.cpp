/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/memory/MemoryUtils.h"

#include <folly/String.h>
#include <gflags/gflags.h>

#include <cstdio>
#include <regex>

#include "common/fs/FileUtils.h"

DEFINE_bool(containerized, false, "Whether run this process inside the docker container");
DEFINE_double(system_memory_high_watermark_ratio, 0.8, "high watermark ratio of system memory");

using nebula::fs::FileUtils;

namespace nebula {

static const std::regex reMemAvailable("^Mem(Available|Total):\\s+(\\d+)\\skB$");
static const std::regex reTotalCache("^total_(cache|inactive_file)\\s+(\\d+)$");

StatusOr<bool> MemoryUtils::hitsHighWatermark() {
  double available = 0.0, total = 0.0;
  if (FLAGS_containerized) {
    FileUtils::FileLineIterator iter("/sys/fs/cgroup/memory/memory.stat", &reTotalCache);
    uint64_t cacheSize = 0;
    while (iter.valid()) {
      auto& sm = iter.matched();
      cacheSize += std::stoul(sm[2].str(), NULL);
    }
    auto limitStatus = MemoryUtils::readSysContents("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    NG_RETURN_IF_ERROR(limitStatus);
    uint64_t limitInBytes = std::move(limitStatus).value();

    auto usageStatus = MemoryUtils::readSysContents("/sys/fs/cgroup/memory/memory.usage_in_bytes");
    NG_RETURN_IF_ERROR(usageStatus);
    uint64_t usageInBytes = std::move(usageStatus).value();
    available = static_cast<double>(limitInBytes - usageInBytes + cacheSize);
    total = limitInBytes;
  } else {
    FileUtils::FileLineIterator iter("/proc/meminfo", &reMemAvailable);
    std::vector<uint64_t> memorySize;
    while (iter.valid()) {
      auto& sm = iter.matched();
      memorySize.emplace_back(std::stoul(sm[1].str(), NULL) << 10);
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
  LOG_IF(WARNING, hits) << "Memory usage has hit the high watermark of system, available: "
                        << available << " vs. total: " << total << " in bytes.";
  return hits;
}

StatusOr<uint64_t> MemoryUtils::readSysContents(const std::string& path) {
  auto cmd = folly::sformat("cat {}", path);
  FILE* pipe = popen(cmd.c_str(), "r");
  if (!pipe) {
    return Status::Error("Failed to open %s: %s", path.c_str(), strerror(errno));
  }

  uint64_t value = 0;
  if (fscanf(pipe, "%lu", &value) != 2) {
    return Status::Error("Failed to read from %s", path.c_str());
  }

  if (pclose(pipe) < 0) {
    return Status::Error("Failed to close the pipe: %s", strerror(errno));
  }

  return value;
}

}  // namespace nebula
