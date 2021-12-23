/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/meta/stats/MetaClientStats.h"

namespace nebula {

stats::CounterId kNumRpcSentToMetad;
stats::CounterId kNumRpcSentToMetadFailed;

void initMetaClientStats() {
  kNumRpcSentToMetad = stats::StatsManager::registerStats("num_rpc_sent_to_metad", "rate, sum");
  kNumRpcSentToMetadFailed =
      stats::StatsManager::registerStats("num_rpc_sent_to_metad_failed", "rate, sum");
}

}  // namespace nebula
