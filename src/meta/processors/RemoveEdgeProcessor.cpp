/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/RemoveEdgeProcessor.h"

namespace nebula {
namespace meta {

void RemoveEdgeProcessor::process(const cpp2::RemoveEdgeReq& req) {
    GraphSpaceID spaceId;
    EdgeType edgeType;
    GET_SPACE_ID_AND_RETURN(req.get_space_name(), spaceId);
    GET_EDGE_TYPE_AND_RETURN(spaceId, req.get_edge_name(), edgeType);
UNUSED(edgeType);
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
}

}  // namespace meta
}  // namespace nebula

