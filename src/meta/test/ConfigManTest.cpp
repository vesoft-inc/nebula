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
#include "meta/ConfigManager.h"
#include "meta/processors/configMan/GetConfigProcessor.h"
#include "meta/processors/configMan/SetConfigProcessor.h"
#include "meta/processors/configMan/ListConfigsProcessor.h"

DECLARE_int32(load_data_interval_secs);
DECLARE_int32(load_config_interval_secs);

namespace nebula {
namespace meta {

TEST(ConfigManTest, ConfigProcessorTest) {
    fs::TempDir rootPath("/tmp/ConfigProcessorTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));

    // set and get string config
    cpp2::ConfigItem item1;
    item1.set_space("myspace");
    item1.set_module(cpp2::ConfigModule::STORAGE);
    item1.set_name("k1");
    item1.set_type(cpp2::ConfigType::STRING);
    item1.set_value("v1");

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
        item.set_space("myspace");
        item.set_module(cpp2::ConfigModule::STORAGE);
        item.set_name("k1");
        item.set_type(cpp2::ConfigType::STRING);
        cpp2::GetConfigReq req;
        req.set_item(item);

        auto* processor = GetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(item1, resp.get_item());
    }
    // get config not existed
    {
        cpp2::ConfigItem item;
        item.set_space("not_existed");
        item.set_module(cpp2::ConfigModule::STORAGE);
        item.set_name("not_existed");
        item.set_type(cpp2::ConfigType::STRING);
        cpp2::GetConfigReq req;
        req.set_item(item);

        auto* processor = GetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }

    cpp2::ConfigItem item2;
    item2.set_space("myspace");
    item2.set_module(cpp2::ConfigModule::STORAGE);
    item2.set_name("k2");
    item2.set_type(cpp2::ConfigType::STRING);
    item2.set_value("v2");
    {
        cpp2::SetConfigReq req;
        req.set_item(item2);

        auto* processor = SetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // list all configs in a module
    {
        cpp2::ListConfigsReq req;
        req.set_space("myspace");
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
            ASSERT_EQ(ret1.get_value(), "v1");
            ASSERT_EQ(ret2.get_value(), "v2");
        } else {
            ASSERT_EQ(ret1.get_value(), "v2");
            ASSERT_EQ(ret2.get_value(), "v1");
        }
    }

    cpp2::ConfigItem item3;
    item3.set_space("myspace");
    item3.set_module(cpp2::ConfigModule::META);
    item3.set_name("k1");
    item3.set_type(cpp2::ConfigType::STRING);
    item3.set_value("v1");
    {
        cpp2::SetConfigReq req;
        req.set_item(item3);

        auto* processor = SetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }

    // list all configs in all module
    {
        cpp2::ListConfigsReq req;
        req.set_space("");
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
    client->init();
    auto space = "test_space";
    client->createSpace(space, 1, 1).get();
    sleep(FLAGS_load_data_interval_secs + 1);

    auto cfgMan = ConfigManager(client.get());
    auto module = cpp2::ConfigModule::STORAGE;
    // set and get config from meta client cache
    {
        cfgMan.setConfig(space, module, "int64_key", 60002l, false);
        auto ret = cfgMan.getConfigAsInt64(space, module, "int64_key");
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(ret.value(), 60002);
    }
    {
        cfgMan.setConfig(space, module, "bool_key", true, false);
        auto ret = cfgMan.getConfigAsBool(space, module, "bool_key");
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(ret.value(), true);
    }
    {
        cfgMan.setConfig(space, module, "double_key", 3.14, false);
        auto ret = cfgMan.getConfigAsDouble(space, module, "double_key");
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(ret.value(), 3.14);
    }
    {
        cfgMan.setConfig(space, module, "string_key", "abc", false);
        auto ret = cfgMan.getConfigAsString(space, module, "string_key");
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(ret.value(), "abc");
    }
    {
        // try to get config not existed
        auto ret = cfgMan.getConfigAsString(space, module, "not_existed");
        ASSERT_FALSE(ret.ok());
    }
    {
        // try to get config of error type
        auto ret = cfgMan.getConfigAsDouble(space, module, "string_key");
        ASSERT_FALSE(ret.ok());
    }
    {
        auto ret = cfgMan.listConfigs(space, module).get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(ret.value().size(), 4);
    }
}

TEST(ConfigManTest, MacroTest) {
    FLAGS_load_data_interval_secs = 1;
    FLAGS_load_config_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MacroTest.XXXXXX");
    uint32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());
    TestUtils::createSomeHosts(sc->kvStore_.get());

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    uint32_t localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto client = std::make_shared<MetaClient>(threadPool,
        std::vector<HostAddr>{HostAddr(localIp, sc->port_)});
    client->init();

    auto cfgMan = *ConfigManager::instance(client.get());
    auto space = "test_space";
    auto module = cpp2::ConfigModule::META;
    client->createSpace(space, 1, 1).get();
    sleep(FLAGS_load_data_interval_secs + 1);

    {
        auto name = "int_key";
        auto type = VariantTypeEnum::INT64;

        // register config, read default value
        REGISTER(space, module, name, type, 1l);
        auto status = GET_CONFIG(space, module, name, type);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<int64_t>(status.value().value_), 1);

        // set and read configs
        SET_CONFIG(space, module, name, 3l);
        status = GET_CONFIG(space, module, name, type);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<int64_t>(status.value().value_), 3);

        // read config from cache
        auto cacheValue = GET_CONFIG_INT(space, module, name);
        ASSERT_TRUE(cacheValue.ok());
        ASSERT_EQ(cacheValue.value(), 3);
    }
    {
        auto name = "double_key";
        auto type = VariantTypeEnum::DOUBLE;

        REGISTER(space, module, name, type, 3.14);
        auto status = GET_CONFIG(space, module, name, type);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<double>(status.value().value_), 3.14);

        SET_CONFIG(space, module, name, 1.23);
        status = GET_CONFIG(space, module, name, type);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<double>(status.value().value_), 1.23);

        auto cacheValue = GET_CONFIG_DOUBLE(space, module, name);
        ASSERT_TRUE(cacheValue.ok());
        ASSERT_EQ(cacheValue.value(), 1.23);
    }
    {
        auto name = "bool_key";
        auto type = VariantTypeEnum::BOOL;

        REGISTER(space, module, name, type, true);
        auto status = GET_CONFIG(space, module, name, type);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<bool>(status.value().value_), true);

        SET_CONFIG(space, module, name, false);
        status = GET_CONFIG(space, module, name, type);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<bool>(status.value().value_), false);

        auto cacheValue = GET_CONFIG_BOOL(space, module, name);
        ASSERT_TRUE(cacheValue.ok());
        ASSERT_EQ(cacheValue.value(), false);
    }
    {
        auto name = "string_key";
        auto type = VariantTypeEnum::STRING;

        REGISTER(space, module, name, type, "abc");
        auto status = GET_CONFIG(space, module, name, type);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<std::string>(status.value().value_), "abc");

        SET_CONFIG(space, module, name, "nebula");
        status = GET_CONFIG(space, module, name, type);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(boost::get<std::string>(status.value().value_), "nebula");

        auto cacheValue = GET_CONFIG_STRING(space, module, name);
        ASSERT_TRUE(cacheValue.ok());
        ASSERT_EQ(cacheValue.value(), "nebula");
    }
    {
        auto name = "load_data_interval_secs";
        auto type = VariantTypeEnum::INT64;

        // only declare config in cache with default value, but not set in meta
        DECLARE(space, module, name, type, 10);
        auto status = GET_CONFIG(space, module, name, type);
        ASSERT_FALSE(status.ok());

        // read config from cache
        auto cacheValue = GET_CONFIG_INT(space, module, name);
        ASSERT_TRUE(cacheValue.ok());
        ASSERT_EQ(cacheValue.value(), 10);

        // update in another thread
        auto update = [&space, &module, &name, &type] () {
            // set and read configs
            SET_CONFIG(space, module, name, 3l);
            auto st = GET_CONFIG(space, module, name, type);
            ASSERT_TRUE(st.ok());
            ASSERT_EQ(boost::get<int64_t>(st.value().value_), 3);
        };
        std::thread t(update);
        t.join();

        cacheValue = GET_CONFIG_INT(space, module, name);
        ASSERT_TRUE(cacheValue.ok());
        ASSERT_EQ(cacheValue.value(), 3);
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

