/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_KVBASEDGFLAGSMANAGER_H_
#define META_KVBASEDGFLAGSMANAGER_H_

#include "base/Base.h"
#include "base/Status.h"
#include "meta/GflagsManager.h"
#include "kvstore/NebulaStore.h"

namespace nebula {
namespace meta {

class KVBasedGflagsManager : public GflagsManager {
public:
    explicit KVBasedGflagsManager(kvstore::KVStore* kv);

    ~KVBasedGflagsManager();

    folly::Future<StatusOr<bool>> setConfig(const cpp2::ConfigModule& module,
                                            const std::string& name,
                                            const cpp2::ConfigType& type,
                                            const VariantType& value,
                                            const bool isForce) override;

    folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
    getConfig(const cpp2::ConfigModule& module, const std::string& name) override;

    folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
    listConfigs(const cpp2::ConfigModule& module) override;

    Status init();

private:
    Status registerGflags(const std::vector<cpp2::ConfigItem>& gflagsDeclared);

    std::vector<cpp2::ConfigItem> gflagsDeclared_;

    kvstore::NebulaStore* kvstore_ = nullptr;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_KVBASEDGFLAGSMANAGER_H_
