/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "meta/GflagsManager.h"
#include "meta/ClientBasedGflagsManager.h"
#include "meta/KVBasedGflagsManager.h"
#include "meta/processors/configMan/GetConfigProcessor.h"
#include "meta/processors/configMan/SetConfigProcessor.h"
#include "meta/processors/configMan/ListConfigsProcessor.h"
#include "meta/processors/configMan/RegConfigProcessor.h"

DECLARE_int32(load_data_interval_secs);
DECLARE_int32(load_config_interval_secs);

namespace nebula {
namespace meta {

TEST(ConfigManTest, ConfigProcessorTest) {
    fs::TempDir rootPath("/tmp/ConfigProcessorTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));

    cpp2::ConfigItem item1;
    item1.set_module(cpp2::ConfigModule::STORAGE);
    item1.set_name("k1");
    item1.set_type(cpp2::ConfigType::STRING);
    item1.set_mode(cpp2::ConfigMode::MUTABLE);
    item1.set_value("v1");

    cpp2::ConfigItem item2;
    item2.set_module(cpp2::ConfigModule::STORAGE);
    item2.set_name("k2");
    item2.set_type(cpp2::ConfigType::STRING);
    item2.set_value("v2");

    // set and get without register
    {
        cpp2::SetConfigReq req;
        req.set_item(item1);

        auto* processor = SetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::ConfigItem item;
        item.set_module(cpp2::ConfigModule::STORAGE);
        item.set_name("k1");
        cpp2::GetConfigReq req;
        req.set_item(item);

        auto* processor = GetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // register config item1 and item2
    {
        std::vector<cpp2::ConfigItem> items;
        items.emplace_back(item1);
        items.emplace_back(item2);
        cpp2::RegConfigReq req;
        req.set_items(items);

        auto* processor = RegConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // set and get string config item1
    {
        cpp2::SetConfigReq req;
        req.set_item(item1);

        auto* processor = SetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::ConfigItem item;
        item.set_module(cpp2::ConfigModule::STORAGE);
        item.set_name("k1");
        cpp2::GetConfigReq req;
        req.set_item(item);

        auto* processor = GetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(item1, resp.get_items().front());
    }
    // get config not existed
    {
        cpp2::ConfigItem item;
        item.set_module(cpp2::ConfigModule::STORAGE);
        item.set_name("not_existed");
        cpp2::GetConfigReq req;
        req.set_item(item);

        auto* processor = GetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(0, resp.get_items().size());
    }
    // list all configs in a module
    {
        cpp2::ListConfigsReq req;
        req.set_module(cpp2::ConfigModule::STORAGE);

        auto* processor = ListConfigsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(2, resp.get_items().size());
        auto ret1 = resp.get_items().front();
        auto ret2 = resp.get_items().back();
        if (ret1.get_name() == "k1") {
            ASSERT_EQ(ret1, item1);
            ASSERT_EQ(ret2, item2);
        } else {
            ASSERT_EQ(ret1, item2);
            ASSERT_EQ(ret2, item1);
        }
    }

    // register another config in other module, list all configs in all module
    cpp2::ConfigItem item3;
    item3.set_module(cpp2::ConfigModule::META);
    item3.set_name("k1");
    item3.set_type(cpp2::ConfigType::STRING);
    item3.set_value("v1");

    {
        std::vector<cpp2::ConfigItem> items;
        items.emplace_back(item3);
        cpp2::RegConfigReq req;
        req.set_items(items);

        auto* processor = RegConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::ListConfigsReq req;
        req.set_module(cpp2::ConfigModule::ALL);

        auto* processor = ListConfigsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(3, resp.get_items().size());
    }
}

TEST(ConfigManTest, MetaConfigManTest) {
    FLAGS_load_data_interval_secs = 1;
    FLAGS_load_config_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaConfigManTest.XXXXXX");
    uint32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());
    TestUtils::createSomeHosts(sc->kvStore_.get());

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    uint32_t localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto client = std::make_shared<MetaClient>(threadPool,
        std::vector<HostAddr>{HostAddr(localIp, sc->port_)});
    auto space = "test_space";
    client->createSpace(space, 1, 1).get();
    sleep(FLAGS_load_data_interval_secs + 1);

    auto module = cpp2::ConfigModule::STORAGE;
    auto mode = cpp2::ConfigMode::MUTABLE;

    ClientBasedGflagsManager cfgMan(client.get());
    cfgMan.module_ = module;
    CHECK(cfgMan.bgThread_.start());
    cfgMan.loadCfgThreadFunc();

    // immutable configs
    {
        std::string name = "int64_key_immutable";
        VariantType defaultValue = 100l;
        auto type = cpp2::ConfigType::INT64;

        sleep(FLAGS_load_config_interval_secs + 1);
        // get/set without register
        auto setRet = cfgMan.setConfig(module, name, type, 101l).get();
        ASSERT_FALSE(setRet.ok());
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_FALSE(getRet.ok());

        // register config as immutable and try to update
        auto regRet = cfgMan.registerConfig(module, name, type, cpp2::ConfigMode::IMMUTABLE,
                                            defaultValue).get();
        ASSERT_TRUE(regRet.ok());
        setRet = cfgMan.setConfig(module, name, cpp2::ConfigType::INT64, 101l).get();
        ASSERT_FALSE(setRet.ok());

        // get immutable config
        getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto value = boost::get<int64_t>(getRet.value().front().value_);
        ASSERT_EQ(value, 100);

        sleep(FLAGS_load_config_interval_secs + 1);
        // get immutable config from cache
        auto cacheRet = cfgMan.getConfigAsInt64(name);
        ASSERT_TRUE(cacheRet.ok());
        ASSERT_EQ(cacheRet.value(), 100);
    }
    // mutable config
    {
        std::string name = "int64_key";
        VariantType defaultValue = 100l;
        auto type = cpp2::ConfigType::INT64;

        // register and update config
        auto regRet = cfgMan.registerConfig(module, name, type, mode, defaultValue).get();
        ASSERT_TRUE(regRet.ok());
        auto setRet = cfgMan.setConfig(module, name, type, 102l).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto value = boost::get<int64_t>(getRet.value().front().value_);
        ASSERT_EQ(value, 102);

        // get from cache
        sleep(FLAGS_load_config_interval_secs + 1);
        auto cacheRet = cfgMan.getConfigAsInt64(name);
        ASSERT_TRUE(cacheRet.ok());
        ASSERT_EQ(cacheRet.value(), 102);
    }
    {
        std::string name = "bool_key";
        VariantType defaultValue = false;
        auto type = cpp2::ConfigType::BOOL;

        // register and update config
        auto regRet = cfgMan.registerConfig(module, name, type, mode, defaultValue).get();
        ASSERT_TRUE(regRet.ok());
        auto setRet = cfgMan.setConfig(module, name, type, true).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto value = boost::get<bool>(getRet.value().front().value_);
        ASSERT_EQ(value, true);

        // get from cache
        sleep(FLAGS_load_config_interval_secs + 1);
        auto cacheRet = cfgMan.getConfigAsBool(name);
        ASSERT_TRUE(cacheRet.ok());
        ASSERT_EQ(cacheRet.value(), true);
    }
    {
        std::string name = "double_key";
        VariantType defaultValue = 1.23;
        auto type = cpp2::ConfigType::DOUBLE;

        // register and update config
        auto regRet = cfgMan.registerConfig(module, name, type, mode, defaultValue).get();
        ASSERT_TRUE(regRet.ok());
        auto setRet = cfgMan.setConfig(module, name, type, 3.14).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto value = boost::get<double>(getRet.value().front().value_);
        ASSERT_EQ(value, 3.14);

        // get from cache
        sleep(FLAGS_load_config_interval_secs + 1);
        auto cacheRet = cfgMan.getConfigAsDouble(name);
        ASSERT_TRUE(cacheRet.ok());
        ASSERT_EQ(cacheRet.value(), 3.14);
    }
    {
        std::string name = "string_key";
        VariantType defaultValue = std::string("something");
        auto type = cpp2::ConfigType::STRING;

        // register and update config
        auto regRet = cfgMan.registerConfig(module, name, type, mode, defaultValue).get();
        ASSERT_TRUE(regRet.ok());
        std::string newValue = "abc";
        auto setRet = cfgMan.setConfig(module, name, type, newValue).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto value = boost::get<std::string>(getRet.value().front().value_);
        ASSERT_EQ(value, "abc");

        // get from cache
        sleep(FLAGS_load_config_interval_secs + 1);
        auto cacheRet = cfgMan.getConfigAsString(name);
        ASSERT_TRUE(cacheRet.ok());
        ASSERT_EQ(cacheRet.value(), "abc");
    }
    {
        auto ret = cfgMan.listConfigs(module).get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(ret.value().size(), 5);
    }
}

TEST(ConfigManTest, KVConfigManTest) {
    FLAGS_load_config_interval_secs = 1;
    fs::TempDir rootPath("/tmp/KVConfigManTest.XXXXXX");
    uint32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());

    KVBasedGflagsManager kvCfgMan(sc->kvStore_.get());
    kvCfgMan.module_ = cpp2::ConfigModule::META;
    CHECK(kvCfgMan.bgThread_.start());

    // test declare and load config in KVBasedGflagsManager
    auto module = cpp2::ConfigModule::META;
    auto type = cpp2::ConfigType::STRING;
    auto mode = cpp2::ConfigMode::MUTABLE;
    for (int i = 0; i < 10; i++) {
        std::string name = "k" + std::to_string(i);
        std::string value = "v" + std::to_string(i);
        kvCfgMan.declareConfig(module, name, type, mode, value);
        kvCfgMan.gflagsDeclared_.emplace_back(
                GflagsManager::toThriftConfigItem(module, name, type, mode, value));
    }
    kvCfgMan.regCfgThreadFunc();
    kvCfgMan.loadCfgThreadFunc();

    sleep(FLAGS_load_config_interval_secs + 1);
    for (int i = 0; i < 10; i++) {
        std::string name = "k" + std::to_string(i);
        std::string value = "v" + std::to_string(i);
        auto cacheRet = kvCfgMan.getConfigAsString(name);
        ASSERT_TRUE(cacheRet.ok());
        ASSERT_EQ(cacheRet.value(), value);
    }

    // mock one ClientBaseGflagsManager and one KVBasedGflagsManager, and do some update
    // value in console, check if it works
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    uint32_t localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    auto client = std::make_shared<MetaClient>(threadPool,
        std::vector<HostAddr>{HostAddr(localIp, sc->port_)});
    ClientBasedGflagsManager clientCfgMan(client.get());
    CHECK(clientCfgMan.bgThread_.start());
    clientCfgMan.module_ = cpp2::ConfigModule::META;
    clientCfgMan.loadCfgThreadFunc();

    sleep(FLAGS_load_config_interval_secs + 1);
    for (int i = 0; i < 10; i++) {
        std::string name = "k" + std::to_string(i);
        std::string value = "v" + std::to_string(i);
        auto cacheRet = clientCfgMan.getConfigAsString(name);
        ASSERT_TRUE(cacheRet.ok());
        ASSERT_EQ(cacheRet.value(), value);
    }

    auto consoleClient = std::make_shared<MetaClient>(threadPool,
        std::vector<HostAddr>{HostAddr(localIp, sc->port_)});
    ClientBasedGflagsManager console(consoleClient.get());
    // update in console
    for (int i = 0; i < 10; i++) {
        std::string name = "k" + std::to_string(i);
        std::string value = "updated" + std::to_string(i);
        auto setRet = console.setConfig(module, name, type, value).get();
        ASSERT_TRUE(setRet.ok());
    }
    // get in console
    for (int i = 0; i < 10; i++) {
        std::string name = "k" + std::to_string(i);
        std::string value = "updated" + std::to_string(i);

        auto getRet = console.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        ASSERT_EQ(boost::get<std::string>(getRet.value().front().value_), value);
    }

    // check values in KVBaseGflagsManager
    sleep(FLAGS_load_config_interval_secs + 1);
    for (int i = 0; i < 10; i++) {
        std::string name = "k" + std::to_string(i);
        std::string value = "updated" + std::to_string(i);
        auto cacheRet = kvCfgMan.getConfigAsString(name);
        ASSERT_TRUE(cacheRet.ok());
        ASSERT_EQ(cacheRet.value(), value);
    }
    // check values in ClientBaseGflagsManager
    for (int i = 0; i < 10; i++) {
        std::string name = "k" + std::to_string(i);
        std::string value = "updated" + std::to_string(i);
        auto cacheRet = clientCfgMan.getConfigAsString(name);
        ASSERT_TRUE(cacheRet.ok());
        ASSERT_EQ(cacheRet.value(), value);
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

