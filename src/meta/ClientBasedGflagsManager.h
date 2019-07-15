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
    FRIEND_TEST(ConfigManTest, MockConfigTest);

public:
    explicit ClientBasedGflagsManager(MetaClient *metaClient);

    ~ClientBasedGflagsManager();

    folly::Future<StatusOr<bool>> setConfig(const cpp2::ConfigModule& module,
                                            const std::string& name,
                                            const cpp2::ConfigType& type,
                                            const VariantType& value) override;

    folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
    getConfig(const cpp2::ConfigModule& module, const std::string& name) override;

    folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
    listConfigs(const cpp2::ConfigModule& module) override;

    folly::Future<StatusOr<bool>> registerConfig(const cpp2::ConfigModule& module,
                                                 const std::string& name,
                                                 const cpp2::ConfigType& type,
                                                 const cpp2::ConfigMode& mode,
                                                 const std::string& defaultValue) override;

    Status init() override;

private:
    Status registerGflags();

    // methods for code to get config from cache
    StatusOr<int64_t> getConfigAsInt64(const std::string& name);

    StatusOr<double> getConfigAsDouble(const std::string& name);

    StatusOr<bool> getConfigAsBool(const std::string& name);

    StatusOr<std::string> getConfigAsString(const std::string& name);

    template<typename ValueType>
    folly::Future<StatusOr<bool>> set(const cpp2::ConfigModule& module, const std::string& name,
                                      const cpp2::ConfigType& type, const ValueType& value);

    MetaClient                          *metaClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula
#endif  // META_CLIENTBASEDGFLAGSMANAGER_H_

