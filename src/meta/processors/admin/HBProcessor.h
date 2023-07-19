/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_HBPROCESSOR_H_
#define META_HBPROCESSOR_H_

#include <gtest/gtest_prod.h>

#include "common/stats/StatsManager.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

struct HBCounters final {
  stats::CounterId numCalls_;
  stats::CounterId latency_;

  void init() {
    if (!numCalls_.valid()) {
      numCalls_ = stats::StatsManager::registerStats("num_heartbeats", "rate, sum");
      latency_ = stats::StatsManager::registerHisto(
          "heartbeat_latency_us", 1000, 0, 20000, "avg, p75, p95, p99");
      VLOG(1) << "Succeeded in initializing the HBCounters";
    } else {
      VLOG(1) << "HBCounters has been initialized";
    }
  }
};
extern HBCounters kHBCounters;

/**
 * @brief storaged/graphd/listener report info to metad periodically,
 *        and update host alive info.
 *
 */
class HBProcessor : public BaseProcessor<cpp2::HBResp> {
  FRIEND_TEST(HBProcessorTest, HBTest);
  FRIEND_TEST(MetaClientTest, HeartbeatTest);

  using Base = BaseProcessor<cpp2::HBResp>;

 public:
  static HBProcessor* instance(kvstore::KVStore* kvstore,
                               const HBCounters* counters = &kHBCounters,
                               ClusterID clusterId = 0) {
    return new HBProcessor(kvstore, counters, clusterId);
  }

  void process(const cpp2::HBReq& req);

 protected:
  void onFinished() override;

 private:
  HBProcessor(kvstore::KVStore* kvstore, const HBCounters* counters, ClusterID clusterId)
      : BaseProcessor<cpp2::HBResp>(kvstore), clusterId_(clusterId), counters_(counters) {}

  void setLeaderInfo();

  ClusterID clusterId_{0};
  const HBCounters* counters_{nullptr};
  static std::atomic<int64_t> metaVersion_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_HBPROCESSOR_H_
