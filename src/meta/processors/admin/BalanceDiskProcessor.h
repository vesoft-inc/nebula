/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_BALANCEDISKPROCESSOR_H_
#define META_BALANCEDISKPROCESSOR_H_

#include <gtest/gtest_prod.h>

#include "meta/ActiveHostsMan.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class BalanceDiskProcessor : public BaseProcessor<cpp2::BalanceResp> {
 public:
  static BalanceDiskProcessor* instance(kvstore::KVStore* kvstore) {
    return new BalanceDiskProcessor(kvstore);
  }

  void process(const cpp2::BalanceDiskReq& req);

 private:
  explicit BalanceDiskProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::BalanceResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BALANCEDISKPROCESSOR_H_
