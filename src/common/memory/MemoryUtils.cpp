/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/memory/MemoryUtils.h"

#include <gflags/gflags.h>

#include <limits>

#if ENABLE_JEMALLOC
#include <jemalloc/jemalloc.h>

#include "common/time/WallClock.h"
#endif
#include <algorithm>
#include <fstream>

#include "common/fs/FileUtils.h"
#include "common/memory/MemoryTracker.h"

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
DEFINE_bool(memory_tracker_detail_log, false, "print memory stats detail log");
DEFINE_int32(memory_tracker_detail_log_interval_ms,
             60000,
             "print memory stats detail log interval in ms");

DEFINE_double(memory_tracker_untracked_reserved_memory_mb,
              50,
              "memory tacker tracks memory of new/delete, this flag defined reserved untracked "
              "memory (direct call malloc/free)");
DEFINE_double(memory_tracker_limit_ratio,
              0.8,
              "memory tacker usable memory ratio to (total - untracked_reserved_memory)");

DEFINE_double(memory_tracker_available_ratio,
              0.8,
              "memory tacker available memory ratio to (available - untracked_reserved_memory)");

using nebula::fs::FileUtils;

namespace nebula {
namespace memory {

static const std::regex reMemAvailable(
    R"(^Mem(Available|Free|Total):\s+(\d+)\skB$)");  // when can't use MemAvailable, use MemFree
                                                     // instead.
static const std::regex reTotalCache(R"(^total_(cache|inactive_file)\s+(\d+)$)");

std::atomic_bool MemoryUtils::kHitMemoryHighWatermark{false};
int64_t MemoryUtils::kLastPurge_{0};
int64_t MemoryUtils::kLastPrintMemoryTrackerStats_{0};
int64_t MemoryUtils::kCurrentTotal_{0};
double MemoryUtils::kCurrentLimitRatio_{0.0};

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

  handleMemoryTracker(total, available);

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

void MemoryUtils::handleMemoryTracker(int64_t total, int64_t available) {
#ifdef ENABLE_MEMORY_TRACKER
  double limitRatio = FLAGS_memory_tracker_limit_ratio;
  double availableRatio = FLAGS_memory_tracker_available_ratio;

  double untrackedMb = FLAGS_memory_tracker_untracked_reserved_memory_mb;

  // update MemoryTracker limit memory by Flags
  if (limitRatio > 0.0 && limitRatio <= 1.0) {
    /**
     * (<= 1.0): Normal limit is set to ratio of trackable memory;
     *           limit = limitRatio * (total - untrackedMb)
     */
    if (kCurrentTotal_ != total || kCurrentLimitRatio_ != limitRatio) {
      double trackable = total - (untrackedMb * MiB);
      if (trackable > 0) {
        MemoryStats::instance().setLimit(trackable * limitRatio);
      } else {
        LOG(ERROR) << "Total memory less than untracked " << untrackedMb << " Mib,"
                   << " MemoryTracker limit is not set properly, "
                   << " Current memory stats:" << MemoryStats::instance().toString();
      }
      LOG(INFO) << "MemoryTracker set static ratio: " << limitRatio;
    }
  } else if (std::fabs(limitRatio - 2.0) < std::numeric_limits<double>::epsilon()) {
    /**
     * (== 2.0): Special value for dynamic self adaptive;
     *           limit = current_used_memory + (available - untracked) * availableRatio
     */
    double trackableAvailable = available - (untrackedMb * MiB);
    if (trackableAvailable > 0) {
      MemoryStats::instance().updateLimit(trackableAvailable * availableRatio);
    } else {
      MemoryStats::instance().updateLimit(0);
    }
    DLOG(INFO) << "MemoryTracker set to dynamic self adaptive";
  } else if (std::fabs(limitRatio - 3.0) < std::numeric_limits<double>::epsilon()) {
    /**
     * (== 3.0): Special value for disable memory_tracker;
     *           limit is set to max
     */
    if (kCurrentLimitRatio_ != limitRatio) {
      MemoryStats::instance().setLimit(std::numeric_limits<int64_t>::max());
      LOG(INFO) << "MemoryTracker disabled";
    }
  } else {
    LOG(ERROR) << "Invalid memory_tracker_limit_ratio: " << limitRatio;
  }
  kCurrentTotal_ = total;
  kCurrentLimitRatio_ = limitRatio;

  // purge if enabled
  if (FLAGS_memory_purge_enabled) {
    int64_t now = time::WallClock::fastNowInSec();
    if (now - kLastPurge_ > FLAGS_memory_purge_interval_seconds) {
      // jemalloc seems has issue with address_sanitizer, do purge only when address_sanitizer is
      // off
      mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", nullptr, nullptr, nullptr, 0);
      kLastPurge_ = now;
    }
  }

  /**
   * print system & application level memory stats
   * sys: read from system environment, varies depends on environment:
   *      container: controlled by cgroup,
   *                 used: read from memory.current in cgroup path
   *                 total: read from memory.max in cgroup path
   *      physical machine: judge by system level memory consumption
   *                 used: current used memory of the system
   *                 total: all physical memory installed
   * usr: record by current process's MemoryStats
   *                 used: bytes allocated by new operator
   *                 total: sys_total * FLAGS_system_memory_high_watermark_ratio
   */
  int64_t now = time::WallClock::fastNowInMilliSec();
  if (FLAGS_memory_tracker_detail_log) {
    if (now - kLastPrintMemoryTrackerStats_ >= FLAGS_memory_tracker_detail_log_interval_ms) {
      LOG(INFO) << fmt::format("sys:{}/{} {:.2f}%",
                               ReadableSize(total - available),
                               ReadableSize(total),
                               (1 - available / static_cast<double>(total)) * 100)
                << fmt::format(" usr:{}/{} {:.2f}%",
                               ReadableSize(MemoryStats::instance().used()),
                               ReadableSize(MemoryStats::instance().getLimit()),
                               MemoryStats::instance().usedRatio() * 100);
      kLastPrintMemoryTrackerStats_ = now;
    }
  }
#else
  LOG_FIRST_N(WARNING, 1) << "WARNING: MemoryTracker was disabled at compile time";
  UNUSED(total);
  UNUSED(available);
#endif
}  // namespace memory

}  // namespace memory
}  // namespace nebula
