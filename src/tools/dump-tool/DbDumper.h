/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#ifndef DB_DUMPER_H
#define DB_DUMPER_H

#include <string>
#include <vector>
#include "meta/SchemaManager.h"
#include "kvstore/RocksEngine.h"
#include "storage/CompactionFilter.h"
#include "fs/FileUtils.h"

namespace nebula {

using nebula::meta::SchemaManager;
using nebula::meta::MetaClient;

class DbDumper {
public:
    DbDumper() = default;
    ~DbDumper() = default;

    void init(SchemaManager *schemaMngPtr, MetaClient *metaPtr,
        std::vector<std::string> &&storagePaths);
    void dump();
    void initDumpSpaces();

private:
    SchemaManager *schemaMngPtr_;
    MetaClient *metaPtr_;
    std::vector<std::string> storagePaths_;
    std::set<std::string> dumpSpaces_;
};

}  // namespace nebula

#endif  // DB_DUMPER_H
