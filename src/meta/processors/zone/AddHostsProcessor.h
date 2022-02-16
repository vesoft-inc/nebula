/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_ADDHOSTSPROCESSOR_H
#define META_ADDHOSTSPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for ExecResp, AddHostsReq (pt...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

/**
 * @brief For each host, make a default zone and add it into the zone.
 */
class AddHostsProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static AddHostsProcessor* instance(kvstore::KVStore* kvstore) {
    return new AddHostsProcessor(kvstore);
  }

  void process(const cpp2::AddHostsReq& req);

 private:
  explicit AddHostsProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADDHOSTSPROCESSOR_H
