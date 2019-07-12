/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_GFLAGSMANAGER_H_
#define META_GFLAGSMANAGER_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "thread/GenericWorker.h"
#include "base/Status.h"
#include "base/StatusOr.h"
#include "gen-cpp2/MetaServiceAsyncClient.h"

namespace nebula {
namespace meta {

struct ConfigItem {
    ConfigItem() {}

    ConfigItem(const cpp2::ConfigModule& module, const std::string& name,
               const cpp2::ConfigType& type, const cpp2::ConfigMode& mode,
               const VariantType& value)
        : module_(module)
        , name_(name)
        , type_(type)
        , mode_(mode)
        , value_(value) {
    }

    cpp2::ConfigModule  module_;
    std::string         name_;
    cpp2::ConfigType    type_;
    cpp2::ConfigMode    mode_;
    VariantType         value_;
};

// get config via module and name
using MetaConfigMap = std::unordered_map<std::pair<cpp2::ConfigModule, std::string>, ConfigItem>;

class GflagsManager {
    FRIEND_TEST(ConfigManTest, MetaConfigManTest);
    FRIEND_TEST(ConfigManTest, KVConfigManTest);

public:
    // methods for consoles, reg/set/get/list configs from meta server
    virtual folly::Future<Status> setConfig(const cpp2::ConfigModule& module,
                                            folly::StringPiece name,
                                            const cpp2::ConfigType& type,
                                            const VariantType& value) = 0;

    virtual folly::Future<StatusOr<std::vector<ConfigItem>>>
    getConfig(const cpp2::ConfigModule& module, folly::StringPiece name) = 0;

    virtual folly::Future<StatusOr<std::vector<ConfigItem>>>
    listConfigs(const cpp2::ConfigModule& module) = 0;

    virtual folly::Future<Status> registerConfig(const cpp2::ConfigModule& module,
                                                 const std::string& name,
                                                 const cpp2::ConfigType& type,
                                                 const cpp2::ConfigMode& mode,
                                                 const VariantType& defaultValue) = 0;

    // check the config is registered in cache or not
    Status isCfgRegistered(const cpp2::ConfigModule& module,
                           const std::string& name, const cpp2::ConfigType& type);

    // methods for code to get config from cache
    StatusOr<int64_t> getConfigAsInt64(folly::StringPiece name);

    StatusOr<double> getConfigAsDouble(folly::StringPiece name);

    StatusOr<bool> getConfigAsBool(folly::StringPiece name);

    StatusOr<std::string> getConfigAsString(folly::StringPiece name);

protected:
    virtual ~GflagsManager();
    virtual void loadCfgThreadFunc() = 0;
    virtual void regCfgThreadFunc() = 0;

    void init();

    void declareGflags();

    StatusOr<ConfigItem> getConfigFromCache(const cpp2::ConfigModule& module,
                                            const std::string& name,
                                            const cpp2::ConfigType& type);

    // declare is for using GflagsManager in code, We don't check whether config cache ready or not.
    // We just put default value in cache, and when we load configs stored in meta successfully, it
    // would replaced by value from meta.
    Status declareConfig(const cpp2::ConfigModule& module, const std::string& name,
                         const cpp2::ConfigType& type, const cpp2::ConfigMode& mode,
                         const VariantType& defaultValue);

    void updateConfigCache(const std::vector<ConfigItem>& items);

    void updateGflagsValue(const ConfigItem& item);

    template<typename ValueType>
    std::string gflagsValueToThriftValue(const gflags::CommandLineFlagInfo& flag);


    static cpp2::ConfigItem toThriftConfigItem(const cpp2::ConfigModule& module,
                                        const std::string& name,
                                        const cpp2::ConfigType& type,
                                        const cpp2::ConfigMode& mode,
                                        const std::string& value);

    static ConfigItem toConfigItem(const cpp2::ConfigItem& tConfigs);

    cpp2::ConfigModule                  module_{cpp2::ConfigModule::UNKNOWN};
    std::vector<cpp2::ConfigItem>       gflagsDeclared_;
    thread::GenericWorker               bgThread_;
    MetaConfigMap                       metaConfigMap_;
    folly::RWSpinLock                   configCacheLock_;
    std::atomic_bool                    configReady_{false};
};

}  // namespace meta
}  // namespace nebula
#endif  // META_GFLAGSMANAGER_H_
