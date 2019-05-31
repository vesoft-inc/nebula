/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include "meta/MetaServiceHandler.h"
#include "meta/processors/partsMan/CreateSpaceProcessor.h"
#include "meta/processors/partsMan/DropSpaceProcessor.h"
#include "meta/processors/partsMan/ListSpacesProcessor.h"
#include "meta/processors/partsMan/GetSpaceProcessor.h"
#include "meta/processors/partsMan/AddHostsProcessor.h"
#include "meta/processors/partsMan/ListHostsProcessor.h"
#include "meta/processors/partsMan/RemoveHostsProcessor.h"
#include "meta/processors/partsMan/GetPartsAllocProcessor.h"
#include "meta/processors/schemaMan/CreateTagProcessor.h"
#include "meta/processors/schemaMan/AlterTagProcessor.h"
#include "meta/processors/schemaMan/DropTagProcessor.h"
#include "meta/processors/schemaMan/GetTagProcessor.h"
#include "meta/processors/schemaMan/ListTagsProcessor.h"
#include "meta/processors/schemaMan/CreateEdgeProcessor.h"
#include "meta/processors/schemaMan/AlterEdgeProcessor.h"
#include "meta/processors/schemaMan/DropEdgeProcessor.h"
#include "meta/processors/schemaMan/GetEdgeProcessor.h"
#include "meta/processors/schemaMan/ListEdgesProcessor.h"
#include "meta/processors/customKV/MultiPutProcessor.h"
#include "meta/processors/customKV/GetProcessor.h"
#include "meta/processors/customKV/MultiGetProcessor.h"
#include "meta/processors/customKV/ScanProcessor.h"
#include "meta/processors/customKV/RemoveProcessor.h"
#include "meta/processors/customKV/RemoveRangeProcessor.h"
#include "meta/processors/admin/HBProcessor.h"

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

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_dropSpace(const cpp2::DropSpaceReq& req) {
    auto* processor = DropSpaceProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListSpacesResp>
MetaServiceHandler::future_listSpaces(const cpp2::ListSpacesReq& req) {
    auto* processor = ListSpacesProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetSpaceResp>
MetaServiceHandler::future_getSpace(const cpp2::GetSpaceReq& req) {
    auto* processor = GetSpaceProcessor::instance(kvstore_);
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

folly::Future<cpp2::ExecResp>
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

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_remove(const cpp2::RemoveReq& req) {
    auto* processor = RemoveProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
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
MetaServiceHandler::future_dropTag(const cpp2::DropTagReq& req) {
    auto* processor = DropTagProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetTagResp>
MetaServiceHandler::future_getTag(const cpp2::GetTagReq &req) {
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

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_alterEdge(const cpp2::AlterEdgeReq& req) {
    auto* processor = AlterEdgeProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_dropEdge(const cpp2::DropEdgeReq& req) {
    auto* processor = DropEdgeProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetEdgeResp>
MetaServiceHandler::future_getEdge(const cpp2::GetEdgeReq& req) {
    auto* processor = GetEdgeProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListEdgesResp>
MetaServiceHandler::future_listEdges(const cpp2::ListEdgesReq& req) {
    auto* processor = ListEdgesProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::HBResp>
MetaServiceHandler::future_heartBeat(const cpp2::HBReq& req) {
    auto* processor = HBProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

}  // namespace meta
}  // namespace nebula
