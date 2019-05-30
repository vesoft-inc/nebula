/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/DropEdgeIndexProcessor.h"

namespace nebula {
namespace meta {

void DropEdgeIndexProcessor::process(const cpp2::DropEdgeIndexReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
}

}  // namespace meta
}  // namespace nebula

