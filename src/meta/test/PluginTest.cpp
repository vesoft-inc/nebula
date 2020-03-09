/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include <fstream>
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "meta/processors/admin/UninstallPluginProcessor.h"
#include "meta/processors/admin/InstallPluginProcessor.h"
#include "meta/processors/admin/ListPluginsProcessor.h"
#include "meta/processors/admin/GetPluginProcessor.h"
#include <unistd.h>

namespace nebula {
namespace meta {

TEST(InstallUninstallPluginTest, Test) {
    fs::TempDir rootPath("/tmp/CreateUserTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    // Create a fake so file
    using fs::FileUtils;
    auto dir = FileUtils::readLink("/proc/self/exe").value();
    dir = FileUtils::dirname(dir.c_str()) + "/../share";

    bool isMkdir = false;
    if (!FileUtils::exist(dir)) {
        bool ret = FileUtils::makeDir(dir);
        ASSERT_TRUE(ret);
        isMkdir = true;
    }
    std::string soname = dir + "/ldap.so";

    int fd = open(soname.c_str(), O_CREAT|O_RDWR, 0755);
    ASSERT_NE(-1, fd);

    {
        // So file not exists
        cpp2::PluginItem item;
        item.set_plugin_name("auth");
        item.set_so_name("auth.so");
        cpp2::InstallPluginReq req;
        req.set_item(std::move(item));

        auto* processor = InstallPluginProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Succeed
        cpp2::PluginItem item;
        item.set_plugin_name("ldap");
        item.set_so_name("ldap.so");
        cpp2::InstallPluginReq req;
        req.set_item(std::move(item));

        auto* processor = InstallPluginProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // List plugins, succeed
        cpp2::ListPluginsReq req;
        auto* processor = ListPluginsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_items().size());
    }
    {
        // Get plugin, succeed
        cpp2::GetPluginReq req;
        req.set_plugin_name("ldap");
        auto* processor = GetPluginProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ("ldap", resp.item.plugin_name);
        ASSERT_EQ("ldap.so", resp.item.so_name);
    }
    {
        // Repeated, failed to register
        cpp2::PluginItem item;
        item.set_plugin_name("ldap");
        item.set_so_name("ldap.so");
        cpp2::InstallPluginReq req;
        req.set_item(std::move(item));

        auto* processor = InstallPluginProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Plugin name not exist
        cpp2::UninstallPluginReq req;
        req.set_plugin_name("auth");
        auto* processor = UninstallPluginProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Succeed
        cpp2::UninstallPluginReq req;
        req.set_plugin_name("ldap");
        auto* processor = UninstallPluginProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Get plugin, succeed
        cpp2::GetPluginReq req;
        req.set_plugin_name("ldap");
        auto* processor = GetPluginProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // List plugins,succeed
        cpp2::ListPluginsReq req;
        auto* processor = ListPluginsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(0, resp.get_items().size());
    }
    {
        // Repeated, failed
        cpp2::UninstallPluginReq req;
        req.set_plugin_name("ldap");
        auto* processor = UninstallPluginProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Delete file
    if (isMkdir) {
        FileUtils::remove(dir.c_str(), true);
    }
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
