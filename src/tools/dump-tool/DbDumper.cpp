/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "DbDumper.h"
#include "EngineDumper.h"
#include "storage/CompactionFilter.h"
#include "fs/FileUtils.h"
#include <sstream>
#include <iostream>

DEFINE_string(spaces, "", "list of spaces wanted to dump, separate by comma");

namespace nebula {

using nebula::kvstore::RocksEngine;

void DbDumper::init(SchemaManager *schemaMngPtr, MetaClient *metaPtr,
        std::vector<std::string> &&storagePaths) {
    schemaMngPtr_ = schemaMngPtr;
    metaPtr_ = metaPtr;
    storagePaths_ = std::move(storagePaths);

    initDumpSpaces();
}


void DbDumper::initDumpSpaces() {
    if (FLAGS_spaces.empty()) {
        return;
    }

    std::istringstream in(FLAGS_spaces);
    std::string space;
    while (getline(in, space, ',')) {
        LOG(INFO) << "we want dump space" << space;
        dumpSpaces_.insert(space);
    }
}


void DbDumper::dump() {
    for (auto& storagePath : storagePaths_) {
        auto dbPath = folly::stringPrintf("%s/nebula", storagePath.c_str());
        auto allSpaces = fs::FileUtils::listAllDirsInDir(dbPath.c_str());
        for (auto &spaceIdInDir : allSpaces) {
            if (dumpSpaces_.size() > 0) {
                if (dumpSpaces_.find(spaceIdInDir) == dumpSpaces_.end()) {
                    continue;
                }
            }

            auto spaceId = folly::to<GraphSpaceID>(spaceIdInDir);
            auto ret = metaPtr_->getSpaceNameByIdFromCache(spaceId);
            if (!ret.ok()) {
                LOG(INFO) << "can not get space name for " << spaceId;
            }

            LOG(INFO) << "Scan path \"" << dbPath << "/" << spaceIdInDir << "\"" << ret.value();

            kvstore::RocksEngine engine(spaceId, storagePath, nullptr, nullptr, false);

            EngineDumper dumper;
            dumper.init(spaceId, ret.value(), schemaMngPtr_, metaPtr_, &engine);
            dumper.dump();
        }
    }
}

}  // namespace nebula
