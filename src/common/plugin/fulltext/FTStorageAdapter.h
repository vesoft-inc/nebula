/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_PLUGIN_FULLTEXT_S_ADAPTER_H_
#define COMMON_PLUGIN_FULLTEXT_S_ADAPTER_H_

#include "common/base/Base.h"
#include "common/plugin/fulltext/FTUtils.h"

namespace nebula {
namespace plugin {

class FTStorageAdapter {
public:
    virtual ~FTStorageAdapter() = default;

    virtual StatusOr<bool> put(const HttpClient& client, const DocItem& item) const = 0;

    virtual StatusOr<bool> bulk(const HttpClient& client,
                                const std::vector<DocItem>& items) const = 0;

protected:
    FTStorageAdapter() = default;
};

}  // namespace plugin
}  // namespace nebula

#endif  // COMMON_PLUGIN_FULLTEXT_S_ADAPTER_H_
