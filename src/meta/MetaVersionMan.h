/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_METAVERSIONMAN_H_
#define META_METAVERSIONMAN_H_

#include "common/base/Base.h"
#include "common/utils/MetaKeyUtils.h"
#include "kvstore/KVEngine.h"
#include "kvstore/KVStore.h"

namespace nebula {
namespace meta {

enum class MetaVersion {
  UNKNOWN = 0,
  V1 = 1,
  V2 = 2,
  V3 = 3,
};

/**
 * This class manages the version of meta's data.
 * */
class MetaVersionMan final {
 public:
  MetaVersionMan() = delete;

  /**
   * @brief Get meta version
   *
   * @param kv
   * @return
   */
  static MetaVersion getMetaVersionFromKV(kvstore::KVStore* kv);

  static bool setMetaVersionToKV(kvstore::KVEngine* engine, MetaVersion version);

  static Status updateMetaV2ToV3(kvstore::KVEngine* engine);

 private:
  static MetaVersion getVersionByHost(kvstore::KVStore* kv);

  static Status doUpgradeV2ToV3(kvstore::KVEngine* engine);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METAVERSIONMAN_H_
