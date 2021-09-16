/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
#include "storage/BaseProcessor.h"
namespace nebula {
namespace storage {
template <typename REQ, typename RESP>
class LookupBaseProcessor : public BaseProcessor<RESP> {
 public:
  virtual ~LookupBaseProcessor() = default;
  virtual void process(const REQ& req) = 0;

 protected:
  LookupBaseProcessor(StorageEnv* env,
                      const ProcessorCounters* counters,
                      folly::Executor* executor = nullptr) {}

  virtual void onProcessFinished() = 0;

  StatusOr<StoragePlan<IndexID>> buildPlan();
  nebula::DataSet resultDataSet_;
  std::vector<nebula::DataSet> partResults_;
};
}  // namespace storage
}  // namespace nebula
#include "storage/index/LookupBaseProcessor-inl.h"
