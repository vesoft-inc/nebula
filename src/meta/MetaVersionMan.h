/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METAVERSIONMAN_H_
#define META_METAVERSIONMAN_H_

#include "common/base/Base.h"
#include "kvstore/KVStore.h"
#include "meta/MetaServiceUtils.h"

namespace nebula {
namespace meta {

/**
 * This class manages the version of meta's data.
 * */
class MetaVersionMan final {
public:
    MetaVersionMan() = delete;

    static int32_t getMetaVersionFromKV(kvstore::KVStore* kv);

    static bool setMetaVersionToKV(kvstore::KVStore* kv);

    static Status updateMetaV1ToV2(kvstore::KVStore* kv);

private:
    static bool isV1(kvstore::KVStore* kv);
    static Status doUpgrade(kvstore::KVStore* kv);
};

}  // namespace meta
}  // namespace nebula

#endif   // META_ROOTUSERMAN_H_
