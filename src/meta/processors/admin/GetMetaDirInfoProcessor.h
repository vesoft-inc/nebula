/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETMETADIRINFOPROCESSOR_H_
#define META_GETMETADIRINFOPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

/**
 * @brief Get meta dir info for each metad service, now only used in BR
 *        Dir info contains root dir path and kv store data dir path.
 *
 */
class GetMetaDirInfoProcessor : public BaseProcessor<cpp2::GetMetaDirInfoResp> {
 public:
  static GetMetaDirInfoProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetMetaDirInfoProcessor(kvstore);
  }
  void process(const cpp2::GetMetaDirInfoReq& req);

 private:
  explicit GetMetaDirInfoProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetMetaDirInfoResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula

#endif  // META_GETMETADIRINFOPROCESSOR_H_
