/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "parser/GQLParser.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    nebula::graph::QueryContext qctx;
    nebula::GQLParser parser(&qctx);
    const char* ptr = reinterpret_cast<const char*>(data);
    std::string query = {ptr, size};
    auto result = parser.parse(query);
    return 0;
}
