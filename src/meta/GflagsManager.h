/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CONFIGMANAGER_H_
#define META_CONFIGMANAGER_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "meta/client/MetaClient.h"
#include "thread/GenericWorker.h"

namespace nebula {
namespace meta {

class GflagsManager {
    FRIEND_TEST(ConfigManTest, MetaConfigManTest);
    FRIEND_TEST(ConfigManTest, MacroTest);
    FRIEND_TEST(ConfigTest, ConfigTest);

public:
    static GflagsManager* instance(MetaClient* client = nullptr);

    ~GflagsManager();

    void init();
    void declareGflags();

    // methods for consoles, reg/set/get/list configs from meta server
    folly::Future<Status> setConfig(const cpp2::ConfigModule& module,
                                    folly::StringPiece name,
                                    const cpp2::ConfigType& type,
                                    const VariantType& value);

    folly::Future<Status> setConfig(const cpp2::ConfigModule& module,
                                    folly::StringPiece name,
                                    const cpp2::ConfigType& type,
                                    const char* value);

    folly::Future<StatusOr<std::vector<ConfigItem>>> getConfig(const cpp2::ConfigModule& module,
                                                               folly::StringPiece name);

    folly::Future<StatusOr<std::vector<ConfigItem>>> listConfigs(const cpp2::ConfigModule& module);

    folly::Future<Status> registerConfig(const cpp2::ConfigModule& module,
                                         const std::string& name,
                                         const cpp2::ConfigType& type,
                                         const cpp2::ConfigMode& mode,
                                         const VariantType& defaultValue);

    // methods for code to get config from meta client cache
    StatusOr<int64_t> getConfigAsInt64(folly::StringPiece name);

    StatusOr<double> getConfigAsDouble(folly::StringPiece name);

    StatusOr<bool> getConfigAsBool(folly::StringPiece name);

    StatusOr<std::string> getConfigAsString(folly::StringPiece name);

    // check the config is registered in cache or not
    Status isCfgRegistered(const cpp2::ConfigModule& module,
                           const std::string& name, const cpp2::ConfigType& type);

private:
    explicit GflagsManager(MetaClient *metaClient);

    template<typename ValueType>
    std::string gflagsValueToThriftValue(const gflags::CommandLineFlagInfo& flag);

    template<typename ValueType>
    folly::Future<Status> set(const cpp2::ConfigModule& module, const std::string& name,
                              const cpp2::ConfigType& type, const ValueType& value);

    cpp2::ConfigItem toThriftConfigItem(const cpp2::ConfigModule& module,
                                        const std::string& name,
                                        const cpp2::ConfigType& type,
                                        const cpp2::ConfigMode& mode,
                                        const std::string& value);

    MetaClient                             *metaClient_{nullptr};
    cpp2::ConfigModule                      module_{cpp2::ConfigModule::UNKNOWN};
    std::vector<cpp2::ConfigItem>           gflagsDeclared_;
};

std::string ConfigModuleToString(const cpp2::ConfigModule& module);
std::string ConfigModeToString(const cpp2::ConfigMode& mode);
std::string ConfigTypeToString(const cpp2::ConfigType& type);

}  // namespace meta
}  // namespace nebula
#endif  // META_SCHEMAMANAGER_H_
