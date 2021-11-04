/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/cpu/CpuInfo.h"

#include <folly/Conv.h>
#include <folly/String.h>

namespace nebula {
namespace cpu {

CpuInfo::CpuInfo() { readCpuInfo(); }

void CpuInfo::readCpuInfo() {
  std::ifstream cif("/proc/cpuinfo");
  std::string line;
  ProcessorInfo processor;

  // Read the first line
  std::getline(cif, line);
  while (!cif.eof() && cif.good()) {
    std::vector<std::string> kv;
    folly::split(":", line, kv);
    std::string key = kv[0].erase(kv[0].find_last_not_of(" \t\r\n") + 1);

    if (key.size() == 0) {
      // all info in a processor has been read, add to info
      CHECK_GE(processor.physicalId_, 0);
      CHECK_GE(processor.coreId_, 0);
      info_[processor.physicalId_][processor.coreId_].emplace_back(processor.processorId_);
      processors_.push_back(std::move(processor));
      processor.reset();

      // Read the next line
      std::getline(cif, line);
      continue;
    }

    std::string val = kv[1]
                          .erase(kv[1].find_last_not_of(" \t\r\n") + 1)
                          .erase(0, kv[1].find_first_not_of(" \t\r\n"));

    if (key == "processor") {
      processor.processorId_ = folly::to<int16_t>(val);
    } else if (key == "physical id") {
      processor.physicalId_ = folly::to<int16_t>(val);
    } else if (key == "core id") {
      processor.coreId_ = folly::to<int16_t>(val);
    } else if (key == "cpu family") {
      processor.cpuFamily_ = folly::to<int16_t>(val);
    } else if (key == "model") {
      processor.model_ = folly::to<int32_t>(val);
    } else if (key == "model name") {
      processor.modelName_ = val;
    } else if (key == "vendor_id") {
      processor.vendor_ = val;
    } else if (key == "cpu MHz") {
      processor.cpuMHz_ = folly::to<double>(val);
    } else if (key == "cache size") {
      std::vector<std::string> parts;
      folly::split(" ", val, parts, true);
      CHECK_EQ(parts.size(), 2);
      processor.cacheSize_ = folly::to<int32_t>(parts[0]);
    } else if (key == "fpu") {
      processor.fpu_ = (val == "yes");
    } else if (key == "fpu_exception") {
      processor.fpuException_ = (val == "yes");
    } else if (key == "cpuid level") {
      processor.cpuIdLevel_ = folly::to<int32_t>(val);
    } else if (key == "flags") {
      std::vector<std::string> flags;
      folly::split(" ", val, flags, true);
      processor.flags_.insert(flags.begin(), flags.end());
    } else {
      VLOG(3) << "Ignore line [" << line << "]";
    }

    // Read the next line
    std::getline(cif, line);
  }
}

std::string CpuInfo::toString() {
  std::stringstream buf;

  for (const auto& proc : processors_) {
    buf << "Processor       : " << proc.processorId_ << "\n";
    buf << "Core Id         : " << proc.coreId_ << "\n";
    buf << "Physical Id     : " << proc.physicalId_ << "\n";
    buf << "Vendor          : " << proc.vendor_ << "\n";
    buf << "CPU Family      : " << proc.cpuFamily_ << "\n";
    buf << "Model           : " << proc.model_ << "\n";
    buf << "Model Name      : " << proc.modelName_ << "\n";
    buf << "CPU MHz         : " << proc.cpuMHz_ << "\n";
    buf << "Cache Size      : " << proc.cacheSize_ << " KB\n";
    buf << "FPU             : " << proc.fpu_ << "\n";
    buf << "FPU Exception   : " << proc.fpuException_ << "\n";
    buf << "CpuId Level     : " << proc.cpuIdLevel_ << "\n";
    buf << "Flags           : " << folly::join(' ', proc.flags_) << "\n";
    buf << "\n";
  }

  return buf.str();
}

uint32_t CpuInfo::numOfCpus() const { return info_.size(); }

uint32_t CpuInfo::numOfCores() const {
  uint32_t numCores = 0;
  for (auto& info : info_) {
    numCores += info.second.size();
  }

  return numCores;
}

uint32_t CpuInfo::numOfCoresOnCpu(uint32_t cpuId) const {
  auto iter = info_.find(cpuId);
  CHECK(iter != info_.end());
  return iter->second.size();
}

uint32_t CpuInfo::numOfProcessors() const { return processors_.size(); }

const std::vector<uint32_t>& CpuInfo::processorsOnCore(uint32_t physicalId, uint32_t coreId) {
  auto cpuIter = info_.find(physicalId);
  CHECK(cpuIter != info_.end());
  auto coreIter = cpuIter->second.find(coreId);
  CHECK(coreIter != cpuIter->second.end());
  return coreIter->second;
}

bool CpuInfo::hasHyperThread() const {
  DCHECK(numOfCores() == numOfProcessors() || numOfCores() * 2 == numOfProcessors());
  return numOfCores() != numOfProcessors();
}

const CpuInfo::ProcessorInfo& CpuInfo::processor(uint32_t processorId) const {
  CHECK_LT(processorId, processors_.size()) << "ProcessorId is out of bound";
  CHECK_EQ(processorId, processors_[processorId].processorId_);
  return processors_[processorId];
}

}  // namespace cpu
}  // namespace nebula
