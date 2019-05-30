/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/GetTagIndexProcessor.h"

namespace nebula {
namespace meta {

void GetTagIndexProcessor::process(const cpp2::GetTagIndexReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
}

}  // namespace meta
}  // namespace nebula
