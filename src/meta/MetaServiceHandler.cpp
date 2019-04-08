/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "meta/MetaServiceHandler.h"
#include "meta/processors/CreateSpaceProcessor.h"
#include "meta/processors/AddHostsProcessor.h"
#include "meta/processors/ListHostsProcessor.h"
#include "meta/processors/ListSpacesProcessor.h"
#include "meta/processors/GetPartsAllocProcessor.h"
#include "meta/processors/AddTagProcessor.h"
#include "meta/processors/AddEdgeProcessor.h"
#include "meta/processors/ListTagsProcessor.h"
#include "meta/processors/ListEdgesProcessor.h"

#define RETURN_FUTURE(processor) \
    auto f = processor->getFuture(); \
    processor->process(req); \
    return f;

namespace nebula {
namespace meta {

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_createSpace(const cpp2::CreateSpaceReq& req) {
    auto* processor = CreateSpaceProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListSpacesResp>
MetaServiceHandler::future_listSpaces(const cpp2::ListSpacesReq& req) {
    auto* processor = ListSpacesProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_addHosts(const cpp2::AddHostsReq& req) {
    auto* processor = AddHostsProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListHostsResp>
MetaServiceHandler::future_listHosts(const cpp2::ListHostsReq& req) {
    auto* processor = ListHostsProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetPartsAllocResp>
MetaServiceHandler::future_getPartsAlloc(const cpp2::GetPartsAllocReq& req) {
    auto* processor = GetPartsAllocProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_addTag(const cpp2::AddTagReq& req) {
    auto* processor = AddTagProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListTagsResp>
MetaServiceHandler::future_listTags(const cpp2::ListTagsReq& req) {
    auto* processor = ListTagsProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_addEdge(const cpp2::AddEdgeReq& req) {
    auto* processor = AddEdgeProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListEdgesResp>
MetaServiceHandler::future_listEdges(const cpp2::ListEdgesReq& req) {
    auto* processor = ListEdgesProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

}  // namespace meta
}  // namespace nebula
