/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADDHOSTSPROCESSOR_H
#define META_ADDHOSTSPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

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
