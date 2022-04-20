/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_SAVEGRAPHVERSIONPROCESSOR_H_
#define META_SAVEGRAPHVERSIONPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {
class SaveGraphVersionProcessor final : public BaseProcessor<cpp2::SaveGraphVersionResp> {
 public:
  static SaveGraphVersionProcessor* instance(kvstore::KVStore* kvstore) {
    return new SaveGraphVersionProcessor(kvstore);
  }

  void process(const cpp2::SaveGraphVersionReq& req);

 private:
  explicit SaveGraphVersionProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::SaveGraphVersionResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula
#endif  // META_SAVEGRAPHVERSIONPROCESSOR_H_
