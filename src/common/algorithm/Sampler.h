/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_ALGORITHM_SAMPLER_H_
#define COMMON_ALGORITHM_SAMPLER_H_

#include <cfloat>
#include <ctime>
#include <random>
#include <type_traits>
#include <utility>
#include <vector>

namespace nebula {
namespace algorithm {
template <typename T = float>
T UniformRandom() {
  static_assert(std::is_floating_point<T>::value, "Only support float point type");
#if defined(__clang__)
  static std::default_random_engine e(std::time(nullptr));
  static std::uniform_real_distribution<T> u(0., 1.);
#elif defined(__GNUC__) || defined(__GNUG__)
  static thread_local std::default_random_engine e(std::time(nullptr));
  static thread_local std::uniform_real_distribution<T> u(0., 1.);
#endif
  return u(e);
}

template <typename T>
void Normalization(std::vector<T>& distribution) {
  static_assert(std::is_floating_point<T>::value, "Only support float point type");
  T norm_sum = 0.0f;
  for (auto& dist : distribution) {
    norm_sum += dist;
  }
  if (norm_sum <= FLT_EPSILON && !distribution.empty()) {
    for (size_t i = 0; i < distribution.size(); ++i) {
      distribution[i] = 1.0f / static_cast<T>(distribution.size());
    }
    return;
  }
  for (size_t i = 0; i < distribution.size(); ++i) {
    distribution[i] /= norm_sum;
  }
}

// https://en.wikipedia.org/wiki/Alias_method
template <typename T = float>
class AliasSampler {
 public:
  static_assert(std::is_floating_point<T>::value, "Only support float point type");
  using AliasType = uint32_t;
  bool Init(std::vector<T>& distribution);
  inline bool Init(const std::vector<T>& distribution);
  AliasType Sample() const;
  inline size_t Size() const;

 private:
  std::vector<T> prob_;
  std::vector<AliasType> alias_;
};

template <typename T>
bool AliasSampler<T>::Init(std::vector<T>& distribution) {
  // normalization sum of distribution to 1
  Normalization(distribution);

  prob_.resize(distribution.size());
  alias_.resize(distribution.size());
  std::vector<AliasType> smaller, larger;
  smaller.reserve(distribution.size());
  larger.reserve(distribution.size());

  for (size_t i = 0; i < distribution.size(); ++i) {
    prob_[i] = distribution[i] * distribution.size();
    if (prob_[i] < 1.0) {
      smaller.push_back(i);
    } else {
      larger.push_back(i);
    }
  }
  // Construct the probability and alias tables
  AliasType small, large;
  while (!smaller.empty() && !larger.empty()) {
    small = smaller.back();
    smaller.pop_back();
    large = larger.back();
    larger.pop_back();
    alias_[small] = large;
    prob_[large] = prob_[large] + prob_[small] - 1.0;
    if (prob_[large] < 1.0) {
      smaller.push_back(large);
    } else {
      larger.push_back(large);
    }
  }
  while (!smaller.empty()) {
    small = smaller.back();
    smaller.pop_back();
    prob_[small] = 1.0;
  }
  while (!larger.empty()) {
    large = larger.back();
    larger.pop_back();
    prob_[large] = 1.0;
  }
  return true;
}

template <typename T>
bool AliasSampler<T>::Init(const std::vector<T>& distribution) {
  std::vector<T> dist = distribution;
  return Init(dist);
}

template <typename T>
typename AliasSampler<T>::AliasType AliasSampler<T>::Sample() const {
  AliasType roll = floor(prob_.size() * UniformRandom());
  bool coin = UniformRandom() < prob_[roll];
  return coin ? roll : alias_[roll];
}

template <typename T>
size_t AliasSampler<T>::Size() const {
  return prob_.size();
}

/**
 * binary sample in accumulation weights
 */
template <typename T = float>
size_t BinarySampleAcc(const std::vector<T>& accumulate_weights) {
  if (accumulate_weights.empty()) {
    return 0;
  }
  T rnd = UniformRandom() * accumulate_weights.back();
  size_t low = 0, high = accumulate_weights.size() - 1, mid = 0;
  while (low <= high) {
    mid = ((high - low) >> 1) + low;
    if (rnd < accumulate_weights[mid]) {
      if (mid == 0) {
        return mid;
      }
      high = mid - 1;
      if (high >= 0 && rnd >= accumulate_weights[high]) {
        // rnd in [mid-1, mid)
        return mid;
      }
    } else {
      low = mid + 1;
      if (low < accumulate_weights.size() && rnd < accumulate_weights[low]) {
        // rnd in [mid, mid+1)
        return low;
      }
    }
  }
  return mid;
}

/**
 * binary sample in weights
 */
template <typename T = float>
size_t BinarySample(const std::vector<T>& weights) {
  std::vector<T> accumulate_weights(weights.size(), 0.0f);
  T cur_weight = 0.0f;
  for (size_t i = 0; i < weights.size(); ++i) {
    cur_weight += weights[i];
    accumulate_weights[i] = cur_weight;
  }
  Normalization(accumulate_weights);
  return BinarySampleAcc(accumulate_weights);
}

}  // namespace algorithm
}  // namespace nebula
#endif
