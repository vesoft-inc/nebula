/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPHOSTSPROCESSOR_H
#define META_DROPHOSTSPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class DropHostsProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropHostsProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropHostsProcessor(kvstore);
  }

  void process(const cpp2::DropHostsReq& req);

 private:
  explicit DropHostsProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> checkRelatedSpaceAndCollect(
      const std::string& zoneName);

  void rewriteSpaceProperties(const std::string& spaceKey, const meta::cpp2::SpaceDesc& properties);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPHOSTSPROCESSOR_H
