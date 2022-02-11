/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_PROCESSORS_COMMON_H_
#define META_PROCESSORS_COMMON_H_

#include "common/utils/Types.h"

namespace nebula {
namespace meta {

class LockUtils {
 public:
  LockUtils() = delete;

  static folly::SharedMutex& lock() {
    static folly::SharedMutex lock;
    return lock;
  }
};

}  // namespace meta
}  // namespace nebula
#endif  // META_PROCESSORS_COMMON_H_
