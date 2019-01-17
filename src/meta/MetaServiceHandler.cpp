/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "meta/MetaServiceHandler.h"
#include "meta/CreateNodeProcessor.h"

namespace nebula {
namespace meta {


folly::Future<cpp2::ExecResponse>
MetaServiceHandler::future_createNode(const cpp2::CreateNodeRequest& req) {
    auto* processor = CreateNodeProcessor::instance(kvstore_, &lock_);
    auto f = processor->getFuture();
    processor->process(req);
    return f;
}

folly::Future<cpp2::ExecResponse>
MetaServiceHandler::future_setNode(const cpp2::SetNodeRequest& req) {
    UNUSED(req);
    folly::Promise<cpp2::ExecResponse> p;
    return p.getFuture();
}

folly::Future<cpp2::GetNodeResponse>
MetaServiceHandler::future_getNode(const cpp2::GetNodeRequest& req) {
    UNUSED(req);
    folly::Promise<cpp2::GetNodeResponse> p;
    return p.getFuture();
}

folly::Future<cpp2::ListChildrenResponse>
MetaServiceHandler::future_listChildren(const cpp2::ListChildrenRequest& req) {
    UNUSED(req);
    folly::Promise<cpp2::ListChildrenResponse> p;
    return p.getFuture();
}

}  // namespace meta
}  // namespace nebula
