/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_SETCONFIGPROCESSOR_H
#define META_SETCONFIGPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...

#include <string>         // for string
#include <unordered_set>  // for unordered_set
#include <utility>        // for move
#include <vector>         // for vector

#include "common/datatypes/Value.h"           // for Value
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"    // for ExecResp, ConfigModule
#include "kvstore/Common.h"                   // for KV
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

/**
 * @brief Set configuration of given module's given name,
 *
 */
class SetConfigProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static SetConfigProcessor* instance(kvstore::KVStore* kvstore) {
    return new SetConfigProcessor(kvstore);
  }

  void process(const cpp2::SetConfigReq& req);

  /**
   * @brief Helper function to set configuration to a single module.
   *
   * @param module should not be cpp2::ConfigModule::ALL
   * @param name
   * @param value
   * @param data
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode setConfig(const cpp2::ConfigModule& module,
                                    const std::string& name,
                                    const Value& value,
                                    std::vector<kvstore::KV>& data);

 private:
  explicit SetConfigProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  static std::unordered_set<std::string> mutableFields_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SETCONFIGPROCESSOR_H
