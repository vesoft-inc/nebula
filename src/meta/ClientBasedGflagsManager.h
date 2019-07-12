/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CLIENTBASEDGFLAGSMANAGER_H_
#define META_CLIENTBASEDGFLAGSMANAGER_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "meta/GflagsManager.h"
#include "meta/client/MetaClient.h"

namespace nebula {
namespace meta {

class ClientBasedGflagsManager : public GflagsManager {
    FRIEND_TEST(ConfigManTest, MetaConfigManTest);
    FRIEND_TEST(ConfigManTest, KVConfigManTest);

public:
    static ClientBasedGflagsManager* instance(MetaClient* client = nullptr);

    ~ClientBasedGflagsManager();

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
    explicit ClientBasedGflagsManager(MetaClient *metaClient);

    void init();

    void updateGflagsValue(const ConfigItem& item);

    template<typename ValueType>
    folly::Future<Status> set(const cpp2::ConfigModule& module, const std::string& name,
                              const cpp2::ConfigType& type, const ValueType& value);

    MetaClient                          *metaClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula
#endif  // META_CLIENTBASEDGFLAGSMANAGER_H_

