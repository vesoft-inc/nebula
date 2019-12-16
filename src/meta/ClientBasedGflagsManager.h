/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CLIENTBASEDGFLAGSMANAGER_H_
#define META_CLIENTBASEDGFLAGSMANAGER_H_

#include "base/Base.h"
#include "meta/GflagsManager.h"
#include "meta/client/MetaClient.h"

namespace nebula {
namespace meta {

class ClientBasedGflagsManager : public GflagsManager {
public:
    explicit ClientBasedGflagsManager(MetaClient *metaClient);

    ~ClientBasedGflagsManager();

    folly::Future<StatusOr<bool>> setConfig(const cpp2::ConfigModule& module,
                                            const std::string& name,
                                            const cpp2::ConfigType& type,
                                            const VariantType& value,
                                            const bool isForce = false) override;

    folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
    getConfig(const cpp2::ConfigModule& module, const std::string& name) override;

    folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
    listConfigs(const cpp2::ConfigModule& module) override;

    Status registerGflags(const std::vector<cpp2::ConfigItem>& items);

private:
    template<typename ValueType>
    folly::Future<StatusOr<bool>> set(const cpp2::ConfigModule& module, const std::string& name,
                                      const cpp2::ConfigType& type, const ValueType& value,
                                      const bool isForce);

    MetaClient                          *metaClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula
#endif  // META_CLIENTBASEDGFLAGSMANAGER_H_

