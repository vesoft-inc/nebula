/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "EngineDumper.h"
#include "RowDumper.h"
#include "base/NebulaKeyUtils.h"
#include <iostream>

namespace nebula {

void EngineDumper::init(GraphSpaceID &spaceId, std::string spaceName, SchemaManager *schemaMngPtr,
    RocksEngine *engine) {
    spaceId_ = spaceId;
    spaceName_ = spaceName;
    schemaMngPtr_ = schemaMngPtr;
    engine_ = engine;
}


void EngineDumper::dump() {
    auto allParts = engine_->allParts();
    for (auto& partId : allParts) {
        LOG(INFO) << "Load part " << partId << " for space " << spaceId_ << " from disk";

        auto prefix = NebulaKeyUtils::prefix(partId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retP = engine_->prefix(prefix, &iter);
        if (retP != kvstore::ResultCode::SUCCEEDED || !iter) {
            LOG(INFO) << "Load part " << partId << " for space " << spaceId_
                      << " from disk failed";
            continue;
        }

        std::vector<RowFormat> rows;
        for (; iter->valid(); iter->next()) {
            auto* dumper = RowDumperFactory::createRowDumper(iter.get(), spaceId_,  schemaMngPtr_);
            if (dumper) {
                auto row = dumper->dump();
                rows.emplace_back(std::move(row));
            }
        }

        if (rows.size() > 0) {
            std::cout << "dump space: " << spaceName_ << ", part: " << partId << std::endl;
            for (auto &row : rows) {
                std::cout << row.first << std::endl;
                std::cout << row.second << std::endl;
            }
        }
    }
}

}  // namespace nebula
