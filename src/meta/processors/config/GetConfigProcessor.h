/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETCONFIGPROCESSOR_H
#define META_GETCONFIGPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...

#include <string>   // for string
#include <utility>  // for move
#include <vector>   // for vector

#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"    // for GetConfigResp, ConfigIt...
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

/**
 * @brief Get dynamic configuration by given name in given module(all, meta, graph, storage)
 *
 */
class GetConfigProcessor : public BaseProcessor<cpp2::GetConfigResp> {
 public:
  static GetConfigProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetConfigProcessor(kvstore);
  }

  void process(const cpp2::GetConfigReq& req);

 private:
  nebula::cpp2::ErrorCode getOneConfig(const cpp2::ConfigModule& module,
                                       const std::string& name,
                                       std::vector<cpp2::ConfigItem>& items);

  explicit GetConfigProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetConfigResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETCONFIGPROCESSOR_H
