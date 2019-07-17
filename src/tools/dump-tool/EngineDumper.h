/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#ifndef ENGINE_DUMPER_H
#define ENGINE_DUMPER_H

#include "meta/SchemaManager.h"
#include "kvstore/RocksEngine.h"
#include "gen-cpp2/meta_types.h"
#include "gen-cpp2/graph_types.h"

namespace nebula {

using nebula::GraphSpaceID;
using nebula::meta::SchemaManager;
using nebula::kvstore::RocksEngine;

class EngineDumper {
public:
    EngineDumper() = default;
    ~EngineDumper() = default;

    void init(GraphSpaceID &spaceId, SchemaManager *schemaMngPtr, RocksEngine *engine);
    void dump();

private:
    GraphSpaceID spaceId_;
    SchemaManager *schemaMngPtr_;
    RocksEngine *engine_;
};

}  // namespace nebula


#endif  // ENGINE_DUMPER_H
