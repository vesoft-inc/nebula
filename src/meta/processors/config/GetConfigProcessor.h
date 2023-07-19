/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETCONFIGPROCESSOR_H
#define META_GETCONFIGPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
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
