/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/kv/GetProcessor.h"

#include <stddef.h>                    // for size_t
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <algorithm>      // for transform
#include <iterator>       // for back_insert_iterator
#include <memory>         // for allocator_traits<>::val...
#include <string>         // for string, basic_string, hash
#include <unordered_map>  // for unordered_map, _Node_co...
#include <vector>         // for vector

#include "common/base/Logging.h"              // for CheckNotNull, CHECK_NOT...
#include "common/thrift/ThriftTypes.h"        // for GraphSpaceID
#include "common/utils/NebulaKeyUtils.h"      // for NebulaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode::E...
#include "kvstore/KVStore.h"                  // for KVStore
#include "storage/BaseProcessor.h"            // for BaseProcessor::handleEr...

namespace nebula {
namespace storage {

ProcessorCounters kGetCounters;

void GetProcessor::process(const cpp2::KVGetRequest& req) {
  CHECK_NOTNULL(env_->kvstore_);
  GraphSpaceID spaceId = req.get_space_id();
  bool returnPartly = req.get_return_partly();

  std::unordered_map<std::string, std::string> pairs;
  size_t size = 0;
  for (auto& part : req.get_parts()) {
    size += part.second.size();
  }
  pairs.reserve(size);

  for (auto& part : req.get_parts()) {
    auto partId = part.first;
    auto& keys = part.second;
    std::vector<std::string> kvKeys;
    kvKeys.reserve(part.second.size());
    std::transform(keys.begin(), keys.end(), std::back_inserter(kvKeys), [partId](const auto& key) {
      return NebulaKeyUtils::kvKey(partId, key);
    });
    std::vector<std::string> values;
    auto ret = env_->kvstore_->multiGet(spaceId, partId, kvKeys, &values);
    if ((ret.first == nebula::cpp2::ErrorCode::SUCCEEDED) ||
        (ret.first == nebula::cpp2::ErrorCode::E_PARTIAL_RESULT && returnPartly)) {
      auto& status = ret.second;
      for (size_t i = 0; i < kvKeys.size(); i++) {
        if (status[i].ok()) {
          pairs.emplace(keys[i], values[i]);
        }
      }
    } else {
      handleErrorCode(ret.first, spaceId, partId);
    }
  }

  resp_.key_values_ref() = std::move(pairs);
  this->onFinished();
}

}  // namespace storage
}  // namespace nebula
