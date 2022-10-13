// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/SamplingExecutor.h"

#include "common/algorithm/Sampler.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

using WeightType = float;

folly::Future<Status> SamplingExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *sampling = asNode<Sampling>(node());
  Result result = ectx_->getResult(sampling->inputVar());
  auto *iter = result.iterRef();
  if (UNLIKELY(iter == nullptr)) {
    return Status::Error(
        "Internal error: nullptr iterator in sampling executor");
  }
  if (UNLIKELY(!result.iter()->isSequentialIter())) {
    std::stringstream ss;
    ss << "Internal error: Sampling executor does not supported "
       << iter->kind();
    return Status::Error(ss.str());
  }
  auto &factors = sampling->factors();
  auto size = iter->size();
  if (size <= 0) {
    iter->clear();
    return finish(ResultBuilder()
                      .value(result.valuePtr())
                      .iter(std::move(result).iter())
                      .build());
  }
  auto colNames = result.value().getDataSet().colNames;
  DataSet dataset(std::move(colNames));
  for (auto factor : factors) {
    if (factor.count <= 0) {
      iter->clear();
      return finish(ResultBuilder()
                        .value(result.valuePtr())
                        .iter(std::move(result).iter())
                        .build());
    }
    if (factor.samplingType == SamplingFactor::SamplingType::BINARY) {
      executeBinarySample<SequentialIter>(iter, factor.colIdx, factor.count,
                                          dataset);
    } else {
      executeAliasSample<SequentialIter>(iter, factor.colIdx, factor.count,
                                         dataset);
    }
  }
  return finish(ResultBuilder()
                    .value(Value(std::move(dataset)))
                    .iter(Iterator::Kind::kSequential)
                    .build());
}

template <typename U>
void SamplingExecutor::executeBinarySample(Iterator *iter, size_t index,
                                           size_t count, DataSet &list) {
  auto uIter = static_cast<U *>(iter);
  std::vector<WeightType> accumulate_weights;
  auto it = uIter->begin();
  WeightType v;
  while (it != uIter->end()) {
    v = 1.0;
    if ((*it)[index].type() == Value::Type::NULLVALUE) {
      LOG(WARNING) << "Sampling type is nullvalue";
    } else if ((*it)[index].type() == Value::Type::FLOAT) {
      v = (float)((*it)[index].getFloat());
    } else if ((*it)[index].type() == Value::Type::INT) {
      v = (float)((*it)[index].getInt());
    } else {
      LOG(WARNING) << "Sampling type is wrong, must be int or float.";
    }
    if (!accumulate_weights.empty()) {
      v += accumulate_weights.back();
    }
    accumulate_weights.emplace_back(std::move(v));
    ++it;
  }
  nebula::algorithm::Normalization<WeightType>(accumulate_weights);
  auto beg = uIter->begin();
  for (size_t i = 0; i < count; ++i) {
    auto idx =
        nebula::algorithm::BinarySampleAcc<WeightType>(accumulate_weights);
    list.emplace_back(*(beg + idx));
  }
  uIter->clear();
}

template <typename U>
void SamplingExecutor::executeAliasSample(Iterator *iter, size_t index,
                                          size_t count, DataSet &list) {
  auto uIter = static_cast<U *>(iter);
  std::vector<WeightType> weights;
  auto it = uIter->begin();
  WeightType v;
  while (it != uIter->end()) {
    v = 1.0;
    if ((*it)[index].type() == Value::Type::NULLVALUE) {
      LOG(WARNING) << "Sampling type is nullvalue";

    } else if ((*it)[index].type() == Value::Type::FLOAT) {
      v = (float)((*it)[index].getFloat());
    } else if ((*it)[index].type() == Value::Type::INT) {
      v = (float)((*it)[index].getInt());
    } else {
      LOG(WARNING) << "Sampling type is wrong, must be int or float.";
    }
    LOG(ERROR) << "lyj debug v:" << v;
    weights.emplace_back(std::move(v));
    ++it;
  }
  nebula::algorithm::AliasSampler<WeightType> sampler_;
  sampler_.Init(weights);
  auto beg = uIter->begin();
  for (size_t i = 0; i < count; ++i) {
    auto idx = sampler_.Sample();
    list.emplace_back(*(beg + idx));
  }
  uIter->clear();
}

}  // namespace graph
}  // namespace nebula
