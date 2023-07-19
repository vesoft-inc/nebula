/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_VERIFYCLIENTVERSIONPROCESSOR_H_
#define META_VERIFYCLIENTVERSIONPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {
class VerifyClientVersionProcessor final : public BaseProcessor<cpp2::VerifyClientVersionResp> {
 public:
  static VerifyClientVersionProcessor* instance(kvstore::KVStore* kvstore) {
    return new VerifyClientVersionProcessor(kvstore);
  }

  void process(const cpp2::VerifyClientVersionReq& req);

 private:
  explicit VerifyClientVersionProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::VerifyClientVersionResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula
#endif  // META_VERIFYCLIENTVERSIONPROCESSOR_H_
