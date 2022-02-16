/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_RENAMEZONEPROCESSOR_H
#define META_RENAMEZONEPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for ExecResp, RenameZoneReq (...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

/**
 * @brief Rename a zone
 */
class RenameZoneProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static RenameZoneProcessor* instance(kvstore::KVStore* kvstore) {
    return new RenameZoneProcessor(kvstore);
  }

  void process(const cpp2::RenameZoneReq& req);

 private:
  explicit RenameZoneProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_RENAMEZONEPROCESSOR_H
