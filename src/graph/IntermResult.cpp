/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/IntermResult.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace graph {

IntermResult::IntermResult(RowSetWriter *rows) {
    data_ = rows->data();
    rsReader_ = std::make_unique<RowSetReader>(rows->schema(), data_);
}


IntermResult::IntermResult(std::vector<VertexID> vids) {
    vids_ = std::move(vids);
}


std::vector<VertexID> IntermResult::getVIDs(const std::string &col) const {
    if (!vids_.empty()) {
        DCHECK(rsReader_ == nullptr);
        return vids_;
    }
    DCHECK(rsReader_ != nullptr);
    std::vector<VertexID> result;
    auto iter = rsReader_->begin();
    while (iter) {
        VertexID vid;
        auto rc = iter->getVid(col, vid);
        CHECK(rc == ResultType::SUCCEEDED);
        result.push_back(vid);
        ++iter;
    }
    return result;
}


}   // namespace graph
}   // namespace nebula
