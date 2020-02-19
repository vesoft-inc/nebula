/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CFHELPER_H_
#define META_CFHELPER_H_

#include "base/Base.h"
#include "kvstore/NebulaStore.h"

namespace nebula {
namespace meta {

class MetaCFHelper {
public:
    MetaCFHelper() = default;

    void registerKv(kvstore::NebulaStore* kv) {
        kv_ = kv;
    }

private:
    kvstore::NebulaStore* kv_;
};

}   // namespace meta
}   // namespace nebula
#endif   // META_CFHELPER_H_
