/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/MetaServiceUtils.h"
#include "meta/MetaServiceHandler.h"
#include "meta/processors/CreateSpaceProcessor.h"
#include "meta/processors/ListSpacesProcessor.h"
#include "meta/processors/DropSpaceProcessor.h"
#include "meta/processors/AddHostsProcessor.h"
#include "meta/processors/ListHostsProcessor.h"
#include "meta/processors/RemoveHostsProcessor.h"
#include "meta/processors/RemoveTagProcessor.h"
#include "meta/processors/CreateTagProcessor.h"
#include "meta/processors/AlterTagProcessor.h"
#include "meta/processors/CreateEdgeProcessor.h"
#include "meta/processors/GetTagProcessor.h"
#include "meta/processors/ListTagsProcessor.h"
#include "meta/processors/ListEdgesProcessor.h"
#include "meta/processors/MultiPutProcessor.h"
#include "meta/processors/GetProcessor.h"
#include "meta/processors/MultiGetProcessor.h"
#include "meta/processors/ScanProcessor.h"
#include "meta/processors/ScanKeyProcessor.h"
#include "meta/processors/ScanValueProcessor.h"
#include "meta/processors/RemoveProcessor.h"
#include "meta/processors/RemoveRangeProcessor.h"
#include "meta/processors/GetPartsAllocProcessor.h"

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
MetaServiceHandler::future_dropSpace(const cpp2::DropSpaceReq& req) {
    auto* processor = DropSpaceProcessor::instance(kvstore_);
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

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_removeHosts(const cpp2::RemoveHostsReq& req) {
    auto* processor = RemoveHostsProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetPartsAllocResp>
MetaServiceHandler::future_getPartsAlloc(const cpp2::GetPartsAllocReq& req) {
    auto* processor = GetPartsAllocProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::MultiPutResp>
MetaServiceHandler::future_multiPut(const cpp2::MultiPutReq& req) {
    auto* processor = MultiPutProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetResp>
MetaServiceHandler::future_get(const cpp2::GetReq& req) {
    auto* processor = GetProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::MultiGetResp>
MetaServiceHandler::future_multiGet(const cpp2::MultiGetReq& req) {
    auto* processor = MultiGetProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ScanResp>
MetaServiceHandler::future_scan(const cpp2::ScanReq& req) {
    auto* processor = ScanProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::KeyOrValueScanResp>
MetaServiceHandler::future_scanKey(const cpp2::ScanReq& req) {
    auto* processor = ScanKeyProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::KeyOrValueScanResp>
MetaServiceHandler::future_scanValue(const cpp2::ScanReq& req) {
    auto* processor = ScanValueProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::RemoveResp>
MetaServiceHandler::future_remove(const cpp2::RemoveReq& req) {
    auto* processor = RemoveProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::RemoveRangeResp>
MetaServiceHandler::future_removeRange(const cpp2::RemoveRangeReq& req) {
    auto* processor = RemoveRangeProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_createTag(const cpp2::CreateTagReq& req) {
    auto* processor = CreateTagProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_alterTag(const cpp2::AlterTagReq& req) {
    auto* processor = AlterTagProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_removeTag(const cpp2::RemoveTagReq& req) {
    auto* processor = RemoveTagProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetTagResp>
MetaServiceHandler::future_getTag(const cpp2::ReadTagReq &req) {
    auto* processor = GetTagProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListTagsResp>
MetaServiceHandler::future_listTags(const cpp2::ListTagsReq& req) {
    auto* processor = ListTagsProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_createEdge(const cpp2::CreateEdgeReq& req) {
    auto* processor = CreateEdgeProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListEdgesResp>
MetaServiceHandler::future_listEdges(const cpp2::ListEdgesReq& req) {
    auto* processor = ListEdgesProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

}  // namespace meta
}  // namespace nebula
