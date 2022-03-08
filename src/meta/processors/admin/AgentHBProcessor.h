/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_PROCESSORS_ADMIN_AGENTHBPROCESSOR_H
#define META_PROCESSORS_ADMIN_AGENTHBPROCESSOR_H

#include <gtest/gtest_prod.h>

#include "common/stats/StatsManager.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

struct AgentHBCounters final {
  stats::CounterId numCalls_;
  stats::CounterId latency_;

  void init() {
    if (!numCalls_.valid()) {
      numCalls_ = stats::StatsManager::registerStats("num_agent_heartbeats", "rate, sum");
      latency_ = stats::StatsManager::registerHisto(
          "agent_heartbeat_latency_us", 1000, 0, 20000, "avg, p75, p95, p99");
      VLOG(1) << "Succeeded in initializing the AgentHBCounters";
    } else {
      VLOG(1) << "AgentHBCounters has been initialized";
    }
  }
};
extern AgentHBCounters kAgentHBCounters;

/**
 * @brief Agent heartbeat register agent to meta and pull all services info in agent's host
 */
class AgentHBProcessor : public BaseProcessor<cpp2::AgentHBResp> {
  FRIEND_TEST(AgentHBProcessorTest, AgentHBTest);

  using Base = BaseProcessor<cpp2::AgentHBResp>;

 public:
  static AgentHBProcessor* instance(kvstore::KVStore* kvstore,
                                    const AgentHBCounters* counters = &kAgentHBCounters) {
    return new AgentHBProcessor(kvstore, counters);
  }

  void process(const cpp2::AgentHBReq& req);

 protected:
  void onFinished() override;

 private:
  AgentHBProcessor(kvstore::KVStore* kvstore, const AgentHBCounters* counters)
      : BaseProcessor<cpp2::AgentHBResp>(kvstore), counters_(counters) {}

  const AgentHBCounters* counters_{nullptr};
};

}  // namespace meta
}  // namespace nebula
#endif
