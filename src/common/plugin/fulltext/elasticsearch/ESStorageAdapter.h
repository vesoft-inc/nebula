/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_PLUGIN_ESSTORAGEADAPTER_H
#define NEBULA_PLUGIN_ESSTORAGEADAPTER_H

#include <gtest/gtest.h>
#include "common/plugin/fulltext/FTStorageAdapter.h"

namespace nebula {
namespace plugin {

class ESStorageAdapter final : public FTStorageAdapter {
    FRIEND_TEST(FulltextPluginTest, ESPutTest);
    FRIEND_TEST(FulltextPluginTest, ESBulkTest);

public:
    static std::unique_ptr<FTStorageAdapter> kAdapter;

    StatusOr<bool> put(const HttpClient& client, const DocItem& item) const override;

    StatusOr<bool> bulk(const HttpClient& client, const std::vector<DocItem>& items) const override;

private:
    ESStorageAdapter() {}

    std::string putHeader(const HttpClient& client, const DocItem& item) const noexcept;

    std::string putBody(const DocItem& item) const noexcept;

    std::string putCmd(const HttpClient& client, const DocItem& item) const noexcept;

    std::string bulkHeader(const HttpClient& client) const noexcept;

    std::string bulkBody(const std::vector<DocItem>& items) const noexcept;

    std::string bulkCmd(const HttpClient& client, const std::vector<DocItem>& items) const noexcept;

    bool checkPut(const std::string& ret, const std::string& cmd) const;

    bool checkBulk(const std::string& ret) const;
};

}  // namespace plugin
}  // namespace nebula

#endif   // NEBULA_PLUGIN_ESSTORAGEADAPTER_H
