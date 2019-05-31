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

// register/set/get config from meta server
#define REGISTER(space, module, name, type, defaultValue) \
    ConfigManager::instance() != nullptr ? \
        ConfigManager::instance()->registerConfig(space, module, name, type, defaultValue).get() : \
        Status::Error()

#define SET_CONFIG(space, module, name, value) \
    ConfigManager::instance() != nullptr ? \
        ConfigManager::instance()->setConfig(space, module, name, value).get() : \
        Status::Error()

#define GET_CONFIG(space, module, name, type) \
    ConfigManager::instance() != nullptr ? \
        ConfigManager::instance()->getConfig(space, module, name, type).get() : \
        Status::Error()

// declare default value in cache
#define DECLARE(space, module, name, type, defaultValue) \
    ConfigManager::instance() != nullptr ? \
        ConfigManager::instance()->declare(space, module, name, type, defaultValue) : \
        Status::Error()

// get config value from cache
#define GET_CONFIG_INT(space, module, name) \
    ConfigManager::instance() != nullptr ? \
        ConfigManager::instance()->getConfigAsInt64(space, module, name) : \
        Status::Error()

#define GET_CONFIG_DOUBLE(space, module, name) \
    ConfigManager::instance() != nullptr ? \
        ConfigManager::instance()->getConfigAsDouble(space, module, name) : \
        Status::Error()

#define GET_CONFIG_BOOL(space, module, name) \
    ConfigManager::instance() != nullptr ? \
        ConfigManager::instance()->getConfigAsBool(space, module, name) : \
        Status::Error()

#define GET_CONFIG_STRING(space, module, name) \
    ConfigManager::instance() != nullptr ? \
        ConfigManager::instance()->getConfigAsString(space, module, name) : \
        Status::Error()

namespace nebula {
namespace meta {

class ConfigManager {
    FRIEND_TEST(ConfigManTest, MetaConfigManTest);
    FRIEND_TEST(ConfigManTest, MacroTest);

public:
    static ConfigManager* instance(MetaClient* client = nullptr);

    ~ConfigManager();

    // declare configs with default value
    void init();

    // methods for consoles, set/get/list configs from meta server
    folly::Future<Status> setConfig(folly::StringPiece space,
                                    const cpp2::ConfigModule& module,
                                    folly::StringPiece name,
                                    const VariantType &value,
                                    bool checkRegistered = true);

    folly::Future<Status> setConfig(folly::StringPiece space,
                                    const cpp2::ConfigModule& module,
                                    folly::StringPiece name,
                                    const char* value,
                                    bool checkRegistered = true);

    folly::Future<StatusOr<ConfigItem>> getConfig(folly::StringPiece space,
                                                  const cpp2::ConfigModule& module,
                                                  folly::StringPiece name,
                                                  const VariantTypeEnum& type);

    folly::Future<StatusOr<std::vector<ConfigItem>>>
    listConfigs(folly::StringPiece space, const cpp2::ConfigModule& module);

    folly::Future<Status> registerConfig(const std::string& space,
                                         const cpp2::ConfigModule& module,
                                         const std::string& name,
                                         const VariantTypeEnum& type,
                                         const VariantType& defaultValue);

    folly::Future<Status> registerConfig(const std::string& space,
                                         const cpp2::ConfigModule& module,
                                         const std::string& name,
                                         const VariantTypeEnum& type,
                                         const char* defaultValue);

    // declare is for using ConfigManager in code, We don't check whether config cache ready or not.
    // We just put default value in cache, and when we load configs stored in meta successfully, it
    // would replace the default value.
    Status declare(const std::string& space, const cpp2::ConfigModule& module,
                   const std::string& name, const VariantTypeEnum& type,
                   const VariantType& defaultValue);

    Status declare(const std::string& space, const cpp2::ConfigModule& module,
                   const std::string& name, const VariantTypeEnum& type,
                   const int& defaultValue);

    Status declare(const std::string& space, const cpp2::ConfigModule& module,
                   const std::string& name, const VariantTypeEnum& type,
                   const char* defaultValue);

    // methods for code to get config from meta client cache
    StatusOr<int64_t>
    getConfigAsInt64(folly::StringPiece space, const cpp2::ConfigModule& module,
                     folly::StringPiece name);

    StatusOr<double>
    getConfigAsDouble(folly::StringPiece space, const cpp2::ConfigModule& module,
                      folly::StringPiece name);

    StatusOr<bool>
    getConfigAsBool(folly::StringPiece space, const cpp2::ConfigModule& module,
                    folly::StringPiece name);

    StatusOr<std::string>
    getConfigAsString(folly::StringPiece space, const cpp2::ConfigModule& module,
                      folly::StringPiece name);

private:
    explicit ConfigManager(MetaClient *metaClient);

    template<typename ValueType>
    folly::Future<Status> set(const std::string& space, const cpp2::ConfigModule& module,
                              const std::string& name, const cpp2::ConfigType& type,
                              const ValueType& value);

    MetaClient *metaClient_{nullptr};

    static std::unique_ptr<ConfigManager>  instance_;
};


}  // namespace meta
}  // namespace nebula
#endif  // META_SCHEMAMANAGER_H_
