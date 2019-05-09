/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/ExecutionContext.h"

namespace nebula {
namespace graph {

ExecutionContext::~ExecutionContext() {
    if (nullptr != sm_) {
        sm_ = nullptr;
    }

    if (nullptr != storage_) {
        storage_ = nullptr;
    }

    if (nullptr != metaClient_) {
        metaClient_ = nullptr;
    }
}

}   // namespace graph
}   // namespace nebula
