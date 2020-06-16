/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/conf/Configuration.h"
#include "common/meta/GflagsManager.h"
#include "common/meta/ClientBasedGflagsManager.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/options_util.h>
#include "meta/test/TestUtils.h"
#include "meta/processors/configMan/GetConfigProcessor.h"
#include "meta/processors/configMan/SetConfigProcessor.h"
#include "meta/processors/configMan/ListConfigsProcessor.h"
#include "meta/processors/configMan/RegConfigProcessor.h"
#include "storage/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_string(rocksdb_db_options);
DECLARE_string(rocksdb_column_family_options);

// some gflags to register
DEFINE_int64(int64_key_immutable, 100, "test");
DEFINE_int64(int64_key, 101, "test");
DEFINE_bool(bool_key, false, "test");
DEFINE_double(double_key, 1.23, "test");
DEFINE_string(string_key, "something", "test");
DEFINE_string(nested_key, R"({"max_background_jobs":"4"})", "test");
DEFINE_string(test0, "v0", "test");
DEFINE_string(test1, "v1", "test");
DEFINE_string(test2, "v2", "test");
DEFINE_string(test3, "v3", "test");
DEFINE_string(test4, "v4", "test");

namespace nebula {
namespace meta {

TEST(ConfigManTest, ConfigProcessorTest) {
    fs::TempDir rootPath("/tmp/ConfigProcessorTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

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
    item2.set_mode(cpp2::ConfigMode::MUTABLE);
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

    // register a nested config in other module
    cpp2::ConfigItem item3;
    item3.set_module(cpp2::ConfigModule::META);
    item3.set_name("nested");
    item3.set_type(cpp2::ConfigType::NESTED);
    item3.set_mode(cpp2::ConfigMode::MUTABLE);
    // default value is a json string
    std::string defaultValue = R"({
        "max_background_jobs":"4"
    })";
    item3.set_value(defaultValue);

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
    // update the nested config
    {
        cpp2::ConfigItem updated;
        updated.set_module(cpp2::ConfigModule::META);
        updated.set_name("nested");
        updated.set_type(cpp2::ConfigType::NESTED);
        updated.set_mode(cpp2::ConfigMode::MUTABLE);
        // update from consle as format of update list
        updated.set_value("max_background_jobs=8,level0_file_num_compaction_trigger=10");

        cpp2::SetConfigReq req;
        req.set_item(updated);

        auto* processor = SetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // get the nested config after updated
    {
        cpp2::ConfigItem item;
        item.set_module(cpp2::ConfigModule::META);
        item.set_name("nested");
        cpp2::GetConfigReq req;
        req.set_item(item);

        auto* processor = GetConfigProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        // verify the updated nested config
        Configuration conf;
        auto confRet = conf.parseFromString(resp.get_items().front().get_value());
        ASSERT_TRUE(confRet.ok());

        std::string val;
        auto status = conf.fetchAsString("max_background_jobs", val);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(val, "8");
        status = conf.fetchAsString("level0_file_num_compaction_trigger", val);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(val, "10");
    }
    // list all configs in all module
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

ConfigItem toConfigItem(const cpp2::ConfigItem& item) {
    VariantType value;
    switch (item.get_type()) {
        case cpp2::ConfigType::INT64:
            value = *reinterpret_cast<const int64_t*>(item.get_value().data());
            break;
        case cpp2::ConfigType::BOOL:
            value = *reinterpret_cast<const bool*>(item.get_value().data());
            break;
        case cpp2::ConfigType::DOUBLE:
            value = *reinterpret_cast<const double*>(item.get_value().data());
            break;
        case cpp2::ConfigType::STRING:
        case cpp2::ConfigType::NESTED:
            value = item.get_value();
            break;
    }
    return ConfigItem(item.get_module(), item.get_name(), item.get_type(), item.get_mode(), value);
}

TEST(ConfigManTest, MetaConfigManTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaConfigManTest.XXXXXX");
    uint32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());
    TestUtils::createSomeHosts(sc->kvStore_.get());

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    auto module = cpp2::ConfigModule::STORAGE;
    auto client = std::make_shared<MetaClient>(threadPool,
        std::vector<HostAddr>{HostAddr(localIp, sc->port_)});
    client->waitForMetadReady();
    client->gflagsModule_ = module;

    ClientBasedGflagsManager cfgMan(client.get());
    // mock some test gflags to meta
    {
        auto mode = meta::cpp2::ConfigMode::MUTABLE;
        std::vector<cpp2::ConfigItem> configItems;
        configItems.emplace_back(toThriftConfigItem(
            module, "int64_key_immutable", cpp2::ConfigType::INT64, cpp2::ConfigMode::IMMUTABLE,
            toThriftValueStr(cpp2::ConfigType::INT64, 100L)));
        configItems.emplace_back(toThriftConfigItem(
            module, "int64_key", cpp2::ConfigType::INT64,
            mode, toThriftValueStr(cpp2::ConfigType::INT64, 101L)));
        configItems.emplace_back(toThriftConfigItem(
            module, "bool_key", cpp2::ConfigType::BOOL,
            mode, toThriftValueStr(cpp2::ConfigType::BOOL, false)));
        configItems.emplace_back(toThriftConfigItem(
            module, "double_key", cpp2::ConfigType::DOUBLE,
            mode, toThriftValueStr(cpp2::ConfigType::DOUBLE, 1.23)));
        std::string defaultValue = "something";
        configItems.emplace_back(toThriftConfigItem(
            module, "string_key", cpp2::ConfigType::STRING,
            mode, toThriftValueStr(cpp2::ConfigType::STRING, defaultValue)));
        configItems.emplace_back(toThriftConfigItem(
            module, "nested_key", cpp2::ConfigType::NESTED,
            mode, toThriftValueStr(cpp2::ConfigType::NESTED, FLAGS_nested_key)));
        cfgMan.registerGflags(configItems);
    }

    // try to set/get config not registered
    {
        std::string name = "not_existed";
        auto type = cpp2::ConfigType::INT64;

        sleep(FLAGS_heartbeat_interval_secs + 1);
        // get/set without register
        auto setRet = cfgMan.setConfig(module, name, type, 101l).get();
        ASSERT_FALSE(setRet.ok());
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_FALSE(getRet.ok());
    }
    // immutable configs
    {
        std::string name = "int64_key_immutable";
        auto type = cpp2::ConfigType::INT64;

        // register config as immutable and try to update
        auto setRet = cfgMan.setConfig(module, name, type, 101l).get();
        ASSERT_FALSE(setRet.ok());

        // get immutable config
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = toConfigItem(getRet.value().front());
        auto value = boost::get<int64_t>(item.value_);
        ASSERT_EQ(value, 100);

        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(FLAGS_int64_key_immutable, 100);
    }
    // mutable config
    {
        std::string name = "int64_key";
        auto type = cpp2::ConfigType::INT64;
        ASSERT_EQ(FLAGS_int64_key, 101);

        // update config
        auto setRet = cfgMan.setConfig(module, name, type, 102l).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = toConfigItem(getRet.value().front());
        auto value = boost::get<int64_t>(item.value_);
        ASSERT_EQ(value, 102);

        // get from cache
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(FLAGS_int64_key, 102);
    }
    {
        std::string name = "bool_key";
        auto type = cpp2::ConfigType::BOOL;
        ASSERT_EQ(FLAGS_bool_key, false);

        // update config
        auto setRet = cfgMan.setConfig(module, name, type, true).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = toConfigItem(getRet.value().front());
        auto value = boost::get<bool>(item.value_);
        ASSERT_EQ(value, true);

        // get from cache
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(FLAGS_bool_key, true);
    }
    {
        std::string name = "double_key";
        auto type = cpp2::ConfigType::DOUBLE;
        ASSERT_EQ(FLAGS_double_key, 1.23);

        // update config
        auto setRet = cfgMan.setConfig(module, name, type, 3.14).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = toConfigItem(getRet.value().front());
        auto value = boost::get<double>(item.value_);
        ASSERT_EQ(value, 3.14);

        // get from cache
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(FLAGS_double_key, 3.14);
    }
    {
        std::string name = "string_key";
        auto type = cpp2::ConfigType::STRING;
        ASSERT_EQ(FLAGS_string_key, "something");

        // update config
        std::string newValue = "abc";
        auto setRet = cfgMan.setConfig(module, name, type, newValue).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = toConfigItem(getRet.value().front());
        auto value = boost::get<std::string>(item.value_);
        ASSERT_EQ(value, "abc");

        // get from cache
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(FLAGS_string_key, "abc");
    }
    {
        std::string name = "nested_key";
        auto type = cpp2::ConfigType::NESTED;
        ASSERT_EQ(FLAGS_nested_key, R"({"max_background_jobs":"4"})");

        // update config
        std::string newValue = "max_background_jobs=8";
        auto setRet = cfgMan.setConfig(module, name, type, newValue).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = toConfigItem(getRet.value().front());
        auto value = boost::get<std::string>(item.value_);

        Configuration conf;
        auto confRet = conf.parseFromString(value);
        ASSERT_TRUE(confRet.ok());
        std::string val;
        auto status = conf.fetchAsString("max_background_jobs", val);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(val, "8");

        // get from cache
        sleep(FLAGS_heartbeat_interval_secs + 1);
        confRet = conf.parseFromString(FLAGS_nested_key);
        ASSERT_TRUE(confRet.ok());
        status = conf.fetchAsString("max_background_jobs", val);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(val, "8");
    }
    {
        auto ret = cfgMan.listConfigs(module).get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(ret.value().size(), 6);
    }
}

TEST(ConfigManTest, MockConfigTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MockConfigTest.XXXXXX");
    uint32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());

    // mock one ClientBaseGflagsManager, and do some update value in console, check if it works
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto module = cpp2::ConfigModule::STORAGE;
    auto type = cpp2::ConfigType::STRING;
    auto mode = cpp2::ConfigMode::MUTABLE;

    auto client = std::make_shared<MetaClient>(threadPool,
        std::vector<HostAddr>{HostAddr(localIp, sc->port_)});
    client->waitForMetadReady();
    client->gflagsModule_ = module;
    ClientBasedGflagsManager clientCfgMan(client.get());

    std::vector<cpp2::ConfigItem> configItems;
    for (int i = 0; i < 5; i++) {
        std::string name = "test" + std::to_string(i);
        std::string value = "v" + std::to_string(i);
        configItems.emplace_back(toThriftConfigItem(module, name, type, mode, value));
    }
    clientCfgMan.registerGflags(configItems);

    auto consoleClient = std::make_shared<MetaClient>(threadPool,
        std::vector<HostAddr>{HostAddr(localIp, sc->port_)});
    consoleClient->waitForMetadReady();
    ClientBasedGflagsManager console(consoleClient.get());
    // update in console
    for (int i = 0; i < 5; i++) {
        std::string name = "test" + std::to_string(i);
        std::string value = "updated" + std::to_string(i);
        auto setRet = console.setConfig(module, name, type, value).get();
        ASSERT_TRUE(setRet.ok());
    }
    // get in console
    for (int i = 0; i < 5; i++) {
        std::string name = "test" + std::to_string(i);
        std::string value = "updated" + std::to_string(i);

        auto getRet = console.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = toConfigItem(getRet.value().front());
        ASSERT_EQ(boost::get<std::string>(item.value_), value);
    }

    // check values in ClientBaseGflagsManager
    sleep(FLAGS_heartbeat_interval_secs + 1);
    ASSERT_EQ(FLAGS_test0, "updated0");
    ASSERT_EQ(FLAGS_test1, "updated1");
    ASSERT_EQ(FLAGS_test2, "updated2");
    ASSERT_EQ(FLAGS_test3, "updated3");
    ASSERT_EQ(FLAGS_test4, "updated4");
}

TEST(ConfigManTest, RocksdbOptionsTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/RocksdbOptionsTest.XXXXXX");
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    const nebula::ClusterID kClusterId = 10;

    uint32_t localMetaPort = network::NetworkUtils::getAvailablePort();
    LOG(INFO) << "Start meta server....";
    std::string metaPath = folly::stringPrintf("%s/meta", rootPath.path());
    auto metaServerContext = meta::TestUtils::mockMetaServer(localMetaPort, metaPath.c_str(),
                                                             kClusterId);
    localMetaPort = metaServerContext->port_;

    auto module = cpp2::ConfigModule::STORAGE;
    auto type = cpp2::ConfigType::NESTED;
    auto mode = meta::cpp2::ConfigMode::MUTABLE;
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(10);
    std::vector<HostAddr> metaAddr = {HostAddr(localIp, localMetaPort)};

    LOG(INFO) << "Create meta client...";
    uint32_t storagePort = network::NetworkUtils::getAvailablePort();
    HostAddr storageAddr(localIp, storagePort);
    meta::MetaClientOptions options;
    options.localHost_ = HostAddr(localIp, storagePort);
    options.clusterId_ = kClusterId;
    options.role_ = meta::cpp2::HostRole::STORAGE;
    options.skipConfig_ = false;
    auto mClient = std::make_unique<meta::MetaClient>(threadPool,
                                                      metaAddr,
                                                      options);
    mClient->waitForMetadReady();
    mClient->gflagsModule_ = module;

    ClientBasedGflagsManager cfgMan(mClient.get());
    // mock some rocksdb gflags to meta
    {
        std::vector<cpp2::ConfigItem> configItems;
        FLAGS_rocksdb_db_options = R"({
            "max_background_jobs":"4"
        })";
        configItems.emplace_back(toThriftConfigItem(
            module, "rocksdb_db_options", type,
            mode, toThriftValueStr(type, FLAGS_rocksdb_db_options)));
        FLAGS_rocksdb_column_family_options = R"({
            "disable_auto_compactions":"false"
        })";
        configItems.emplace_back(toThriftConfigItem(
            module, "rocksdb_column_family_options", type,
            mode, toThriftValueStr(type, FLAGS_rocksdb_column_family_options)));
        cfgMan.registerGflags(configItems);
    }

    std::string dataPath = folly::stringPrintf("%s/storage", rootPath.path());
    auto sc = storage::TestUtils::mockStorageServer(mClient.get(),
                                                    dataPath.c_str(),
                                                    localIp,
                                                    storagePort,
                                                    true);

    SpaceDesc spaceDesc("storage", 9, 1);
    auto ret = mClient->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok());
    auto spaceId = ret.value();
    sleep(FLAGS_heartbeat_interval_secs + 1);
    storage::TestUtils::waitUntilAllElected(sc->kvStore_.get(), spaceId, 9);
    {
        std::string name = "rocksdb_db_options";
        std::string updateValue = "max_background_jobs=10";
        // update config
        auto setRet = cfgMan.setConfig(module, name, type, updateValue).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();
        auto value = boost::get<std::string>(item.get_value());

        sleep(FLAGS_heartbeat_interval_secs + 3);
        ASSERT_EQ(FLAGS_rocksdb_db_options, value);
    }
    {
        std::string name = "rocksdb_column_family_options";
        std::string updateValue = "disable_auto_compactions=true,"
                                  "level0_file_num_compaction_trigger=8,"
                                  "write_buffer_size=1048576";
        // update config
        auto setRet = cfgMan.setConfig(module, name, type, updateValue).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();
        auto value = boost::get<std::string>(item.get_value());

        sleep(FLAGS_heartbeat_interval_secs + 3);
        ASSERT_EQ(FLAGS_rocksdb_column_family_options, value);
    }
    {
        // need to sleep a bit to take effect on rocksdb
        sleep(FLAGS_heartbeat_interval_secs + 2);
        rocksdb::DBOptions loadedDbOpt;
        std::vector<rocksdb::ColumnFamilyDescriptor> loadedCfDescs;
        std::string rocksPath = folly::stringPrintf("%s/disk1/nebula/%d/data",
                                                    dataPath.c_str(), spaceId);
        rocksdb::Status status = rocksdb::LoadLatestOptions(rocksPath, rocksdb::Env::Default(),
                                                            &loadedDbOpt, &loadedCfDescs);
        ASSERT_TRUE(status.ok());
        EXPECT_EQ(10, loadedDbOpt.max_background_jobs);
        EXPECT_EQ(true, loadedCfDescs[0].options.disable_auto_compactions);
        EXPECT_EQ(8, loadedCfDescs[0].options.level0_file_num_compaction_trigger);
        EXPECT_EQ(1048576, loadedCfDescs[0].options.write_buffer_size);
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

