/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/memory/MemoryUtils.h"

#include <gflags/gflags.h>
#include <jemalloc/jemalloc.h>

#include <algorithm>
#include <fstream>

#include "common/fs/FileUtils.h"
#include "common/memory/MemoryTracker.h"
#include "common/time/WallClock.h"

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)

DEFINE_bool(containerized, false, "Whether run this process inside the docker container");
DEFINE_double(system_memory_high_watermark_ratio, 0.8, "high watermark ratio of system memory");

DEFINE_string(cgroup_v2_controllers, "/sys/fs/cgroup/cgroup.controllers", "cgroup v2 controllers");

DEFINE_string(cgroup_v1_memory_stat_path,
              "/sys/fs/cgroup/memory/memory.stat",
              "cgroup v1 memory stat");
DEFINE_string(cgroup_v2_memory_stat_path, "/sys/fs/cgroup/memory.stat", "cgroup v2 memory stat");

DEFINE_string(cgroup_v1_memory_max_path,
              "/sys/fs/cgroup/memory/memory.limit_in_bytes",
              "cgroup v1 memory max");
DEFINE_string(cgroup_v2_memory_max_path, "/sys/fs/cgroup/memory.max", "cgroup v2 memory max path");

DEFINE_string(cgroup_v1_memory_current_path,
              "/sys/fs/cgroup/memory/memory.usage_in_bytes",
              "cgroup v1 memory current");
DEFINE_string(cgroup_v2_memory_current_path,
              "/sys/fs/cgroup/memory.current",
              "cgroup v2 memory current");

DEFINE_bool(memory_purge_enabled, true, "memory purge enabled, default true");
DEFINE_int32(memory_purge_interval_seconds, 10, "memory purge interval in seconds, default 10");

using nebula::fs::FileUtils;

namespace nebula {

static const std::regex reMemAvailable(
    R"(^Mem(Available|Free|Total):\s+(\d+)\skB$)");  // when can't use MemAvailable, use MemFree
                                                     // instead.
static const std::regex reTotalCache(R"(^total_(cache|inactive_file)\s+(\d+)$)");

std::atomic_bool MemoryUtils::kHitMemoryHighWatermark{false};
int64_t MemoryUtils::kLastPurge_{0};

StatusOr<bool> MemoryUtils::hitsHighWatermark() {
  if (FLAGS_system_memory_high_watermark_ratio >= 1.0) {
    return false;
  }
  double available = 0.0;
  int64_t total = 0;
  if (FLAGS_containerized) {
    bool cgroupsv2 = FileUtils::exist(FLAGS_cgroup_v2_controllers);
    std::string statPath =
        cgroupsv2 ? FLAGS_cgroup_v2_memory_stat_path : FLAGS_cgroup_v1_memory_stat_path;
    FileUtils::FileLineIterator iter(statPath, &reTotalCache);
    uint64_t cacheSize = 0;
    for (; iter.valid(); ++iter) {
      auto& sm = iter.matched();
      cacheSize += std::stoul(sm[2].str(), nullptr);
    }

    std::string limitPath =
        cgroupsv2 ? FLAGS_cgroup_v2_memory_max_path : FLAGS_cgroup_v1_memory_max_path;
    auto limitStatus = MemoryUtils::readSysContents(limitPath);
    NG_RETURN_IF_ERROR(limitStatus);
    uint64_t limitInBytes = std::move(limitStatus).value();

    std::string usagePath =
        cgroupsv2 ? FLAGS_cgroup_v2_memory_current_path : FLAGS_cgroup_v1_memory_current_path;
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

  // set limit
  memory::MemoryStats::instance().setLimit(total * FLAGS_system_memory_high_watermark_ratio);

  // purge if enabled
  if (FLAGS_memory_purge_enabled) {
    int64_t now = time::WallClock::fastNowInSec();
    if (now - kLastPurge_ > FLAGS_memory_purge_interval_seconds) {
      mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", nullptr, nullptr, nullptr, 0);
      kLastPurge_ = now;
    }
  }

  // print system & application level memory stats
  DLOG(INFO) << " sys_used: " << static_cast<int64_t>(total - available) << " sys_total: " << total
             << " sys_ratio:" << (1 - available / total)
             << " usr_used:" << memory::MemoryStats::instance().used()
             << " usr_total:" << memory::MemoryStats::instance().getLimit()
             << " usr_ratio:" << memory::MemoryStats::instance().usedRatio();

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
