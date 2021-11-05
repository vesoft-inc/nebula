/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "common/base/Base.h"

namespace nebula {
namespace cpu {

class CpuInfo final {
 public:
  // Info for each processor
  struct ProcessorInfo {
    friend class CpuInfo;

    int16_t processorId_;
    int16_t physicalId_;
    int16_t coreId_;
    int16_t cpuFamily_;
    int32_t model_;
    std::string modelName_;
    std::string vendor_;
    double cpuMHz_;
    int32_t cacheSize_;  // KB
    bool fpu_;
    bool fpuException_;
    int32_t cpuIdLevel_;

    ProcessorInfo() { reset(); }

    void reset() {
      processorId_ = -1;
      physicalId_ = -1;
      coreId_ = -1;
      cpuFamily_ = -1;
      model_ = -1;
      modelName_ = "";
      vendor_ = "";
      cpuMHz_ = 0.0;
      cacheSize_ = 0;
      fpu_ = false;
      fpuException_ = false;
      cpuIdLevel_ = -1;

      flags_.clear();
    }

   private:
    std::unordered_set<std::string> flags_;
  };

  // Default constructor
  // Will read /proc/cpuinfo every time when constructing
  CpuInfo();

  // physical id
  uint32_t numOfCpus() const;

  // core Id
  uint32_t numOfCores() const;

  // how many cores on a specfic cpu
  uint32_t numOfCoresOnCpu(uint32_t cpuId) const;

  // how many processors, hyper thread is included
  uint32_t numOfProcessors() const;

  // how many processors on a specific core
  const std::vector<uint32_t>& processorsOnCore(uint32_t physicalId, uint32_t coreId);

  // whether hyper thread is enabled
  bool hasHyperThread() const;

  // return specific processor's info
  const ProcessorInfo& processor(uint32_t processorId) const;

  std::string toString();

 private:
  std::vector<ProcessorInfo> processors_;
  // map<cpuId, map<coreId, vector<processorsId>>>
  std::unordered_map<uint32_t, std::unordered_map<uint32_t, std::vector<uint32_t>>> info_;

  void readCpuInfo();
};

}  // namespace cpu
}  // namespace nebula
