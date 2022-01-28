/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/meta/stats/MetaClientStats.h"
#include "kvstore/stats/KVStats.h"

namespace nebula {

stats::CounterId kNumEdgesInserted;
stats::CounterId kNumVerticesInserted;
stats::CounterId kNumEdgesDeleted;
stats::CounterId kNumTagsDeleted;
stats::CounterId kNumVerticesDeleted;

void initStorageStats() {
  kNumEdgesInserted = stats::StatsManager::registerStats("num_edges_inserted", "rate, sum");
  kNumVerticesInserted = stats::StatsManager::registerStats("num_vertices_inserted", "rate, sum");
  kNumEdgesDeleted = stats::StatsManager::registerStats("num_edges_deleted", "rate, sum");
  kNumTagsDeleted = stats::StatsManager::registerStats("num_tags_deleted", "rate, sum");
  kNumVerticesDeleted = stats::StatsManager::registerStats("num_vertices_deleted", "rate, sum");

#ifndef BUILD_STANDALONE
  initMetaClientStats();
  initKVStats();
#endif
}

}  // namespace nebula
