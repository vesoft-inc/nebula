/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPTAGPROCESSOR_H
#define META_DROPTAGPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...

#include <string>   // for string
#include <utility>  // for move
#include <vector>   // for vector

#include "common/base/ErrorOr.h"              // for ErrorOr
#include "common/thrift/ThriftTypes.h"        // for GraphSpaceID, TagID
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"    // for ExecResp, DropTagReq (p...
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

class DropTagProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropTagProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropTagProcessor(kvstore);
  }

  void process(const cpp2::DropTagReq& req);

 private:
  explicit DropTagProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> getTagKeys(GraphSpaceID id,
                                                                        TagID tagId);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPTAGPROCESSOR_H
