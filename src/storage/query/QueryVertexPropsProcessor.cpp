/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/QueryVertexPropsProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace storage {

void QueryVertexPropsProcessor::process(const cpp2::VertexPropRequest& vertexReq) {
    cpp2::GetNeighborsRequest req;
    req.set_space_id(vertexReq.get_space_id());
    req.set_parts(std::move(vertexReq.get_parts()));
    decltype(req.return_columns) tmpColumns;
    tmpColumns.reserve(vertexReq.get_return_columns().size());
    for (auto& col : vertexReq.get_return_columns()) {
        tmpColumns.emplace_back(std::move(col));
    }
    req.set_return_columns(std::move(tmpColumns));
    this->onlyVertexProps_ = true;
    QueryBoundProcessor::process(req);
}

}  // namespace storage
}  // namespace nebula
