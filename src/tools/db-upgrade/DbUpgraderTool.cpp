/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/FileUtil.h>
#include <folly/json.h>

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/RocksEngineConfig.h"
#include "tools/db-upgrade/DbUpgrader.h"
#include "tools/db-upgrade/NebulaKeyUtilsV1.h"

void printHelp() {
  fprintf(
      stderr,
      R"(  ./db_upgrade --src_db_path=<path to rocksdb> --dst_db_path=<path to rocksdb> --upgrade_meta_server=<ip:port,...> --raw_data_version=<1|2>

desc:
        This tool is used to upgrade data from nebula 1.x or the previous versions of nebula 2.0 RC
        to nebula 2.6 version.

required:
       --src_db_path=<path to rocksdb>
         Source data path(data_path in storage 1.x conf) to the rocksdb data directory.
         This is an absolute path, multi paths should be split by comma.
         If nebula 1.x was installed in /usr/local/nebula,
         the db_path would be /usr/local/nebula/data/storage
         Default: ""

       --dst_db_path=<path to rocksdb>
         Destination data path(data_path in storage 2.0 conf) to the rocksdb data directory.
         This is an absolute path, multi paths should be split by comma.
         If nebula 2.0 was installed in /usr/local/nebulav2,
         the db_path would be /usr/local/nebulav2/data/storage
         Default: ""

         note:
         The number of paths in src_db_path is equal to the number of paths in dst_db_path, and
         src_db_path and dst_db_path must be different.

       --upgrade_meta_server=<ip:port,...>
         A list of meta severs' ip:port seperated by comma.
         Default: 127.0.0.1:45500

       --raw_data_version=<1|2>
         This tool can only upgrade 1.x data or 2.0 RC data.
         When the value is 1, upgrade the data from 1.x to 2.6.
         When the value is 2, upgrade the data from 2.0 RC to 2.6.
         Default: 0

       --partIds
         Specify all partIds to be processed, separated by comma.

       --auto_gen_part_schema=<true|false>
         Whether to generate only part schema
         Default: true

 optional:
       --write_batch_num=<N>
         The size of the batch written to rocksdb.
         Default: 100000

       --compactions=<true|false>
         When the data upgrade finished, whether to compact all data.
         Default: true

       --max_concurrent_parts<N>
         Maximum number of concurrent parts allowed.
         Default: 10

       --max_concurrent_spaces<N>
         Maximum number of concurrent spaces allowed.
         Default: 5

       --dst_part_file=<file to dataPath and parts>
         File name of datapaht and part distribution
         Default: ""

       --gen_data=<true|false>
         When auto_gen_part_schema is false, it is used to identify whether to generate data or schema
         Default: true
)");
}

void printParams() {
  std::cout << "=========================== PARAMS ============================\n";
  std::cout << "Meta server: " << FLAGS_upgrade_meta_server << "\n";
  std::cout << "Source data path: " << FLAGS_src_db_path << "\n";
  std::cout << "Destination data path: " << FLAGS_dst_db_path << "\n";
  std::cout << "The size of the batch written: " << FLAGS_write_batch_num << "\n";
  std::cout << "Raw data from version: " << FLAGS_raw_data_version << "\n";
  std::cout << "Specify all partIds to be processed: " << FLAGS_partIds << "\n ";
  std::cout << "whether to compact all data: " << (FLAGS_compactions == true ? "true" : "false")
            << "\n";
  std::cout << "Maximum number of concurrent parts allowed:" << FLAGS_max_concurrent_parts << "\n";
  std::cout << "Maximum number of concurrent spaces allowed: " << FLAGS_max_concurrent_spaces
            << "\n";
  std::cout << "whether to auto_gen_part_schema: "
            << (FLAGS_auto_gen_part_schema == true ? "true" : "false") << "\n";
  std::cout << "File name of datapaht and part distribution is " << FLAGS_dst_part_file << "\n";
  std::cout << "whether to gen data or part schema: "
            << (FLAGS_compactions == true ? "true" : "false") << "\n";

  std::cout << "=========================== PARAMS ============================\n\n";
}

using nebula::GraphSpaceID;
using nebula::PartitionID;

bool upgradeOneDataPathPartSchem(nebula::meta::MetaClient* metaClient,
                                 GraphSpaceID spaceId,
                                 const std::string& path,
                                 std::vector<PartitionID> partIds) {
  if (!nebula::fs::FileUtils::exist(path)) {
    LOG(ERROR) << "Data path " << path << " not exists!";
    return false;
  }

  auto spaceVidLenRet = metaClient->getSpaceVidLen(spaceId);
  if (!spaceVidLenRet.ok()) {
    LOG(ERROR) << "Get space vid len failed, space id " << spaceId;
    return false;
  }
  auto spaceVidLen = spaceVidLenRet.value();
  std::unique_ptr<nebula::kvstore::RocksEngine> writeEngine(
      new nebula::kvstore::RocksEngine(spaceId, spaceVidLen, path));
  std::vector<nebula::kvstore::KV> data;
  for (auto& part : partIds) {
    auto key = nebula::NebulaKeyUtils::systemPartKey(part);
    data.emplace_back(std::move(key), "");
  }
  auto code = writeEngine->multiPut(data);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(FATAL) << "Write multi part system data in space id " << spaceId << " dataPath " << path
               << " failed.";
  }
  return true;
}

// case:  bin/db_upgrader --auto_gen_part_schema=false  --upgrade_meta_server=xxx
// --dst_part_file=/xxxxxx.json
/*
The dst_part_file file is a json format file, the content of the file is as follows:
{
    "space_id": 1,

    "instances": [
        {
            "data_path": "/home/dataPath1",
            "parts": [1, 2]
        },
        {
            "data_path": "/home/dataPath2",
            "parts": [3, 4]
        },
        {
            "data_path": "/home/dataPath3",
            "parts": [5, 6]
        }
    ]
}
*/
void upgradePartSchem(nebula::meta::MetaClient* metaClient, const std::string& filename) {
  std::string jsonStr;
  if (access(filename.c_str(), F_OK) != 0) {
    LOG(ERROR) << "File not exists " << filename;
    return;
  }

  if (!folly::readFile(filename.c_str(), jsonStr)) {
    LOG(ERROR) << "Parse file " << filename << " failed!";
    return;
  }
  auto jsonObj = folly::parseJson(jsonStr);
  LOG(INFO) << folly::toPrettyJson(jsonObj);
  GraphSpaceID spaceId = jsonObj.at("space_id").asInt();
  auto instances = jsonObj.at("instances");
  CHECK(instances.isArray());
  auto it = instances.begin();

  std::unordered_map<std::string, std::vector<PartitionID>> dataPathToParts;
  while (it != instances.end()) {
    CHECK(it->isObject());
    auto dataPath = it->at("data_path").asString();
    auto partIds = it->at("parts");
    std::vector<PartitionID> pathParts;
    for (auto& partstr : partIds) {
      auto part = partstr.asInt();
      pathParts.push_back(part);
    }
    dataPathToParts.emplace(dataPath, pathParts);
    it++;
  }
  // upgrade
  for (auto& elem : dataPathToParts) {
    auto ret = upgradeOneDataPathPartSchem(metaClient, spaceId, elem.first, elem.second);
    if (!ret) {
      LOG(ERROR) << "Generate part schema failed, in space  " << spaceId << " dataPath "
                 << elem.first;
      return;
    }
  }
  LOG(INFO) << "Generate part schema success";
  return;
}

int main(int argc, char* argv[]) {
  // When begin to upgrade the data, close compaction
  // When upgrade finished, perform compaction.
  FLAGS_rocksdb_column_family_options = R"({
        "disable_auto_compactions":"true",
        "write_buffer_size":"134217728",
        "max_write_buffer_number":"12",
        "max_bytes_for_level_base":"268435456",
        "level0_slowdown_writes_trigger":"999999",
        "level0_stop_writes_trigger":"999999",
        "soft_pending_compaction_bytes_limit":"137438953472",
        "hard_pending_compaction_bytes_limit":"274877906944"
    })";

  FLAGS_rocksdb_db_options = R"({
        "max_background_jobs":"10",
        "max_subcompactions":"10"
    })";

  if (argc == 1) {
    printHelp();
    return EXIT_FAILURE;
  } else {
    folly::init(&argc, &argv, true);
  }

  google::SetStderrLogging(google::INFO);

  printParams();

  // Handle arguments
  LOG(INFO) << "================== Prepare phase begin ==================";
  auto addrs = nebula::network::NetworkUtils::toHosts(FLAGS_upgrade_meta_server);
  if (!addrs.ok()) {
    LOG(ERROR) << "Get meta host address failed " << FLAGS_upgrade_meta_server;
    return EXIT_FAILURE;
  }

  auto ioExecutor = std::make_shared<folly::IOThreadPoolExecutor>(1);
  nebula::meta::MetaClientOptions options;
  options.skipConfig_ = true;
  auto metaClient =
      std::make_unique<nebula::meta::MetaClient>(ioExecutor, std::move(addrs.value()), options);
  CHECK_NOTNULL(metaClient);
  if (!metaClient->waitForMetadReady(1)) {
    LOG(ERROR) << "Meta is not ready: " << FLAGS_upgrade_meta_server;
    return EXIT_FAILURE;
  }

  if (!FLAGS_auto_gen_part_schema && !FLAGS_gen_data) {
    if (FLAGS_dst_part_file.empty()) {
      LOG(ERROR) << "auto_gen_part_schema is false, and dst_part_file should not be empty.";
      return EXIT_FAILURE;
    }
    upgradePartSchem(metaClient.get(), FLAGS_dst_part_file);
    return 0;
  }

  if (FLAGS_src_db_path.empty() || FLAGS_dst_db_path.empty()) {
    LOG(ERROR) << "Source data path or destination data path should be not empty.";
    return EXIT_FAILURE;
  }

  std::vector<std::string> srcPaths;
  folly::split(",", FLAGS_src_db_path, srcPaths, true);
  std::transform(srcPaths.begin(), srcPaths.end(), srcPaths.begin(), [](auto& p) {
    return folly::trimWhitespace(p).str();
  });
  if (srcPaths.empty()) {
    LOG(ERROR) << "Bad source data path format: " << FLAGS_src_db_path;
    return EXIT_FAILURE;
  }

  std::vector<std::string> dstPaths;
  folly::split(",", FLAGS_dst_db_path, dstPaths, true);
  std::transform(dstPaths.begin(), dstPaths.end(), dstPaths.begin(), [](auto& p) {
    return folly::trimWhitespace(p).str();
  });
  if (dstPaths.empty()) {
    LOG(ERROR) << "Bad destination data path format: " << FLAGS_dst_db_path;
    return EXIT_FAILURE;
  }

  if (srcPaths.size() != dstPaths.size()) {
    LOG(ERROR) << "The size of source data paths is not equal the "
               << "size of destination data paths.";
    return EXIT_FAILURE;
  }

  auto schemaMan = nebula::meta::ServerBasedSchemaManager::create(metaClient.get());
  auto indexMan = nebula::meta::ServerBasedIndexManager::create(metaClient.get());
  CHECK_NOTNULL(schemaMan);
  CHECK_NOTNULL(indexMan);

  if (FLAGS_raw_data_version != 1 && FLAGS_raw_data_version != 2) {
    LOG(ERROR) << "Flag raw_data_version : " << FLAGS_raw_data_version
               << " illegal, raw_data_version can only be 1 or 2";
    return EXIT_FAILURE;
  }

  // one space, check partId
  std::vector<std::string> parts;
  std::unordered_set<PartitionID> partsAll;
  folly::split(",", FLAGS_partIds, parts, true);
  for (auto& part : parts) {
    PartitionID partId;
    try {
      partId = folly::to<PartitionID>(part);
    } catch (const std::exception& ex) {
      LOG(ERROR) << "Cannot convert part id " << part;
      return EXIT_FAILURE;
    }
    partsAll.emplace(partId);
  }

  for (auto& path : srcPaths) {
    auto dataPath = nebula::fs::FileUtils::joinPath(path, "nebula");
    if (!nebula::fs::FileUtils::exist(dataPath)) {
      LOG(ERROR) << "Source data path " << dataPath << " not exists!";
      return EXIT_FAILURE;
    }
    auto subDirs = nebula::fs::FileUtils::listAllDirsInDir(dataPath.c_str());
    if (subDirs.size() != 1) {
      LOG(ERROR) << "Only handle one space!";
      return EXIT_FAILURE;
    }
    auto space = subDirs[0];
    GraphSpaceID spaceId;
    try {
      spaceId = folly::to<GraphSpaceID>(space);
    } catch (const std::exception& ex) {
      LOG(ERROR) << "Cannot convert space id " << space;
      return EXIT_FAILURE;
    }

    auto sRet = schemaMan->toGraphSpaceName(spaceId);
    if (!sRet.ok()) {
      LOG(ERROR) << "Space id " << spaceId << " no found";
      return EXIT_FAILURE;
    }

    auto spaceVidLenRet = metaClient->getSpaceVidLen(spaceId);
    if (!spaceVidLenRet.ok()) {
      LOG(ERROR) << "Space id " << spaceId << " vid len no found";
      return EXIT_FAILURE;
    }
    auto spaceVidLen = spaceVidLenRet.value();
    std::unique_ptr<nebula::kvstore::RocksEngine> readEngine(
        new nebula::kvstore::RocksEngine(spaceId, spaceVidLen, path, "", nullptr, nullptr, true));

    auto prefix = nebula::NebulaKeyUtilsV1::systemPrefix();
    std::unique_ptr<nebula::kvstore::KVIterator> iter;
    auto retCode = readEngine->prefix(prefix, &iter);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Space id " << spaceId << " get part system data failed";
      return EXIT_FAILURE;
    }

    while (iter && iter->valid()) {
      auto key = iter->key();
      auto partId = nebula::NebulaKeyUtilsV1::getPart(key);
      if (nebula::NebulaKeyUtilsV1::isSystemCommit(key)) {
        // do nothing
      } else if (nebula::NebulaKeyUtilsV1::isSystemPart(key)) {
        LOG(INFO) << "Find part id " << partId << " system data.";
        partsAll.erase(partId);
      } else {
        LOG(ERROR) << "Space id " << spaceId << " part " << partId << " illegal data!";
        return EXIT_FAILURE;
      }
      iter->next();
    }
  }
  if (!partsAll.empty()) {
    std::string partstr;
    for (auto part : partsAll) {
      partstr += folly::stringPrintf("%d ", part);
    }
    LOG(ERROR) << "There are some part " << partstr << "not found";
    return EXIT_FAILURE;
  }
  LOG(INFO) << "All parts has found, part id " << FLAGS_partIds;
  LOG(INFO) << "================== Prepare phase end ==================";

  // Upgrade data
  LOG(INFO) << "================== Upgrade phase bengi ==================";

  // The data path in storage conf is generally one, not too many.
  // So there is no need to control the number of threads here.
  std::vector<std::thread> threads;
  for (size_t i = 0; i < srcPaths.size(); i++) {
    threads.emplace_back(std::thread([mclient = metaClient.get(),
                                      sMan = schemaMan.get(),
                                      iMan = indexMan.get(),
                                      srcPath = srcPaths[i],
                                      dstPath = dstPaths[i]] {
      LOG(INFO) << "Upgrade from path " << srcPath << " to path " << dstPath << " begin";
      nebula::storage::DbUpgrader upgrader;
      auto ret = upgrader.init(mclient, sMan, iMan, srcPath, dstPath);
      if (!ret.ok()) {
        LOG(ERROR) << "Upgrader from path " << srcPath << " to path " << dstPath << " init failed.";
        return;
      }
      upgrader.run();
      LOG(INFO) << "Upgrade from path " << srcPath << " to path " << dstPath << " end";
    }));
  }

  // Wait for all threads to finish
  for (auto& t : threads) {
    t.join();
  }

  LOG(INFO) << "================== Upgrade phase end ==================";
  return 0;
}
