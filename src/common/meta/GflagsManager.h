/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_META_GFLAGSMANAGER_H_
#define COMMON_META_GFLAGSMANAGER_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/base/StatusOr.h"
#include "common/interface/gen-cpp2/MetaServiceAsyncClient.h"

namespace nebula {
namespace meta {

class GflagsManager final {
public:
    GflagsManager() = delete;

    static void getGflagsModule(cpp2::ConfigModule& gflagsModule);

    static std::vector<cpp2::ConfigItem> declareGflags(const cpp2::ConfigModule& module);

    static Value gflagsValueToValue(const std::string &type,
                                    const std::string &val);

    static std::string ValueToGflagString(const Value &values);
private:
    static std::unordered_map<std::string, std::pair<cpp2::ConfigMode, bool>>
           parseConfigJson(const std::string& json);
};
}  // namespace meta
}  // namespace nebula
#endif  // COMMON_META_GFLAGSMANAGER_H_
