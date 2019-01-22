/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/QueryVertexPropsProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "storage/KeyUtils.h"
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
    QueryBoundProcessor::process(req);
}

}  // namespace storage
}  // namespace nebula
