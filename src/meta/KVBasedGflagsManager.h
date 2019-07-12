/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_KVBASEDGFLAGSMANAGER_H_
#define META_KVBASEDGFLAGSMANAGER_H_

#include "base/Base.h"
#include "meta/GflagsManager.h"
#include "kvstore/NebulaStore.h"

namespace nebula {
namespace meta {

class KVBasedGflagsManager : public GflagsManager {
    FRIEND_TEST(ConfigManTest, KVConfigManTest);

public:
    static KVBasedGflagsManager* instance(kvstore::KVStore* kv = nullptr);

    ~KVBasedGflagsManager();

    folly::Future<Status> setConfig(const cpp2::ConfigModule& module,
                                    folly::StringPiece name,
                                    const cpp2::ConfigType& type,
                                    const VariantType& value) override;

    folly::Future<StatusOr<std::vector<ConfigItem>>>
    getConfig(const cpp2::ConfigModule& module, folly::StringPiece name) override;

    folly::Future<StatusOr<std::vector<ConfigItem>>>
    listConfigs(const cpp2::ConfigModule& module) override;

    folly::Future<Status> registerConfig(const cpp2::ConfigModule& module,
                                         const std::string& name,
                                         const cpp2::ConfigType& type,
                                         const cpp2::ConfigMode& mode,
                                         const VariantType& defaultValue) override;

protected:
    void loadCfgThreadFunc() override;

    void regCfgThreadFunc() override;

private:
    explicit KVBasedGflagsManager(kvstore::KVStore* kv);

    void init();

    kvstore::NebulaStore* kvstore_ = nullptr;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_KVBASEDGFLAGSMANAGER_H_
