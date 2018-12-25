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
    req.space_id = vertexReq.get_space_id();
    req.ids = std::move(vertexReq.get_ids());
    req.return_columns = std::move(vertexReq.get_return_columns());
    QueryBoundProcessor::process(req);
}

}  // namespace storage
}  // namespace nebula
