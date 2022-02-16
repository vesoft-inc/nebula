/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef NEBULA_GRAPH_ALTERSPACEPROCESSOR_H
#define NEBULA_GRAPH_ALTERSPACEPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...

#include <string>   // for string
#include <utility>  // for move
#include <vector>   // for vector

#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"    // for ExecResp, AlterSpaceReq...
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {
class AlterSpaceProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static AlterSpaceProcessor* instance(kvstore::KVStore* kvstore) {
    return new AlterSpaceProcessor(kvstore);
  }

  void process(const cpp2::AlterSpaceReq& req);

 private:
  nebula::cpp2::ErrorCode addZones(const std::string& spaceName,
                                   const std::vector<std::string>& zones);

 private:
  explicit AlterSpaceProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // NEBULA_GRAPH_ALTERSPACEPROCESSOR_H
