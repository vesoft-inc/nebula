/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_REGCONFIGPROCESSOR_H
#define META_REGCONFIGPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Register configuration in meta service, configuration item could be
 *        set only after registered.
 *
 */
class RegConfigProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static RegConfigProcessor* instance(kvstore::KVStore* kvstore) {
    return new RegConfigProcessor(kvstore);
  }

  void process(const cpp2::RegConfigReq& req);

 private:
  explicit RegConfigProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REGCONFIGPROCESSOR_H
