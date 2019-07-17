/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "DbDumper.h"
#include "EngineDumper.h"
#include "storage/CompactionFilter.h"
#include "fs/FileUtils.h"

namespace nebula {

using nebula::GraphSpaceID;
using nebula::kvstore::RocksEngine;

void DbDumper::init(SchemaManager *schemaMngPtr, std::vector<std::string> &&storagePaths) {
    schemaMngPtr_ = schemaMngPtr;
    storagePaths_ = std::move(storagePaths);
}


void DbDumper::dump() {
    for (auto& storagePath : storagePaths_) {
        auto dbPath = folly::stringPrintf("%s/nebula", storagePath.c_str());
        auto allSpaces = fs::FileUtils::listAllDirsInDir(dbPath.c_str());
        for (auto &spaceIdInDir : allSpaces) {
            LOG(INFO) << "Scan path \"" << dbPath << "/" << spaceIdInDir << "\"";

            auto spaceId = folly::to<GraphSpaceID>(spaceIdInDir);
            auto cfFactory = std::shared_ptr<nebula::kvstore::KVCompactionFilterFactory>(
                new storage::NebulaCompactionFilterFactory(schemaMngPtr_));
            cfFactory->construct(spaceId, 24*3600);
            kvstore::RocksEngine engine(spaceId, storagePath, nullptr, cfFactory, false);

            EngineDumper dumper;
            dumper.init(spaceId, schemaMngPtr_, &engine);
            dumper.dump();
        }
    }
}

}  // namespace nebula
