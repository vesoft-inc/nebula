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
#define GENERATE_LOCK(Entry)                 \
  static folly::SharedMutex& Entry##Lock() { \
    static folly::SharedMutex l;             \
    return l;                                \
  }

  GENERATE_LOCK(lastUpdateTime);
  GENERATE_LOCK(space);
  GENERATE_LOCK(id);
  GENERATE_LOCK(workerId);
  GENERATE_LOCK(localId);
  GENERATE_LOCK(tag);
  GENERATE_LOCK(edge);
  GENERATE_LOCK(tagIndex);
  GENERATE_LOCK(edgeIndex);
  GENERATE_LOCK(service);
  GENERATE_LOCK(fulltextIndex);
  GENERATE_LOCK(user);
  GENERATE_LOCK(config);
  GENERATE_LOCK(snapshot);
  GENERATE_LOCK(group);
  GENERATE_LOCK(zone);
  GENERATE_LOCK(listener);
  GENERATE_LOCK(session);
  GENERATE_LOCK(machine);

#undef GENERATE_LOCK
};

}  // namespace meta
}  // namespace nebula
#endif  // META_PROCESSORS_COMMON_H_
