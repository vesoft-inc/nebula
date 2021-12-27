/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/meta/stats/MetaClientStats.h"

namespace nebula {

stats::CounterId kNumRpcSentToStoraged;
stats::CounterId kNumRpcSentToStoragedFailed;

void initStorageClientStats() {
  kNumRpcSentToStoraged =
      stats::StatsManager::registerStats("num_rpc_sent_to_storaged", "rate, sum");
  kNumRpcSentToStoragedFailed =
      stats::StatsManager::registerStats("num_rpc_sent_to_storaged_failed", "rate, sum");
}

}  // namespace nebula
