/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/kv/RemoveProcessor.h"

#include <algorithm>      // for for_each
#include <string>         // for string, basic_string
#include <unordered_map>  // for unordered_map
#include <vector>         // for vector

#include "common/base/Logging.h"          // for CheckNotNull, CHECK_NOTNULL
#include "common/utils/NebulaKeyUtils.h"  // for NebulaKeyUtils
#include "storage/BaseProcessor.h"        // for BaseProcessor::doRemove

namespace nebula {
namespace storage {

ProcessorCounters kRemoveCounters;

void RemoveProcessor::process(const cpp2::KVRemoveRequest& req) {
  CHECK_NOTNULL(env_->kvstore_);
  const auto& pairs = req.get_parts();
  auto space = req.get_space_id();
  callingNum_ = pairs.size();

  std::for_each(pairs.begin(), pairs.end(), [&](auto& value) {
    auto part = value.first;
    std::vector<std::string> keys;
    for (auto& key : value.second) {
      keys.emplace_back(std::move(NebulaKeyUtils::kvKey(part, key)));
    }
    doRemove(space, part, std::move(keys));
  });
}

}  // namespace storage
}  // namespace nebula
