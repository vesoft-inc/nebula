/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef META_PROCESSORS_PARTS_CREATESPACEASPROCESSOR_H
#define META_PROCESSORS_PARTS_CREATESPACEASPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...

#include <string>   // for string
#include <utility>  // for move
#include <vector>   // for vector

#include "common/base/ErrorOr.h"              // for ErrorOr
#include "common/datatypes/HostAddr.h"        // for HostAddr
#include "common/thrift/ThriftTypes.h"        // for GraphSpaceID
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode::S...
#include "interface/gen-cpp2/meta_types.h"    // for ExecResp, CreateSpaceAs...
#include "kvstore/Common.h"                   // for KV
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor

namespace nebula {
namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {

using Hosts = std::vector<HostAddr>;

class CreateSpaceAsProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static CreateSpaceAsProcessor* instance(kvstore::KVStore* kvstore) {
    return new CreateSpaceAsProcessor(kvstore);
  }

  void process(const cpp2::CreateSpaceAsReq& req);

 protected:
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> makeNewSpaceData(
      GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId, const std::string& spaceName);

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> makeNewTags(GraphSpaceID oldSpaceId,
                                                                         GraphSpaceID newSpaceId);

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> makeNewEdges(GraphSpaceID oldSpaceId,
                                                                          GraphSpaceID newSpaceId);

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> makeNewIndexes(
      GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId);

  nebula::cpp2::ErrorCode rc_{nebula::cpp2::ErrorCode::SUCCEEDED};

 private:
  explicit CreateSpaceAsProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif
