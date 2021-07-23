/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/container/Enumerate.h>
#include <folly/executors/QueuedImmediateExecutor.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include <algorithm>

#include "codec/RowWriterV2.h"
#include "storage/mutate/AddEdgesAtomicProcessor.h"
#include "storage/transaction/TransactionManager.h"
#include "storage/transaction/TransactionUtils.h"
#include "utils/IndexKeyUtils.h"
#include "utils/NebulaKeyUtils.h"


namespace nebula {
namespace storage {

ProcessorCounters kAddEdgesAtomicCounters;

// use localPart vs remotePart to identify different channel.
using ChainId = std::pair<PartitionID, PartitionID>;

void AddEdgesAtomicProcessor::process(const cpp2::AddEdgesRequest& req) {
    propNames_ = req.get_prop_names();
    spaceId_ = req.get_space_id();

    auto stVidLen = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!stVidLen.ok()) {
        LOG(ERROR) << stVidLen.status();
        for (auto& part : req.get_parts()) {
            pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
        }
        onFinished();
        return;
    }
    vIdLen_ = stVidLen.value();
    processByChain(req);
}

void AddEdgesAtomicProcessor::processByChain(const cpp2::AddEdgesRequest& req) {
    std::unordered_map<ChainId, std::vector<KV>> edgesByChain;
    std::unordered_map<PartitionID, nebula::cpp2::ErrorCode> failedPart;
    // split req into chains
    for (auto& part : *req.parts_ref()) {
        auto localPart = part.first;
        for (auto& edge : part.second) {
            auto remotePart = env_->metaClient_->partId(spaceId_,
                    (*(*edge.key_ref()).dst_ref()).getStr());
            ChainId cid{localPart, remotePart};
            if (FLAGS_trace_toss) {
                auto& ekey = *edge.key_ref();
                LOG(INFO) << "ekey.src.hex=" << folly::hexlify((*ekey.src_ref()).toString())
                          << ", ekey.dst.hex=" << folly::hexlify((*ekey.dst_ref()).toString());
            }
            auto key = TransactionUtils::edgeKey(vIdLen_, localPart, edge.get_key());
            std::string val;
            auto code = encodeSingleEdgeProps(edge, val);
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                failedPart[localPart] = code;
                break;;
            }
            edgesByChain[cid].emplace_back(std::make_pair(std::move(key), std::move(val)));
        }
    }

    if (!failedPart.empty()) {
        for (auto& part : failedPart) {
            pushResultCode(part.second, part.first);
        }
        onFinished();
        return;
    }

    CHECK_NOTNULL(env_->indexMan_);
    auto stIndex = env_->indexMan_->getEdgeIndexes(spaceId_);
    if (!stIndex.ok()) {
         for (auto& part : *req.parts_ref())  {
            pushResultCode(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, part.first);
        }
        onFinished();
        return;
    }
    if (!stIndex.value().empty()) {
        processor_.reset(AddEdgesProcessor::instance(env_));
        processor_->indexes_ = stIndex.value();
    }

    std::list<folly::Future<folly::Unit>> futures;
    for (auto& chain : edgesByChain) {
        auto localPart = chain.first.first;
        auto remotePart = chain.first.second;
        auto& localData = chain.second;

        futures.emplace_back(
            env_->txnMan_
                ->addSamePartEdges(
                    vIdLen_, spaceId_, localPart, remotePart, localData, processor_.get())
                .thenTry([=](auto&& t) {
                    auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
                    if (!t.hasValue()) {
                        code = nebula::cpp2::ErrorCode::E_UNKNOWN;
                    } else if (t.value() != nebula::cpp2::ErrorCode::SUCCEEDED) {
                        code = t.value();
                    }
                    LOG_IF(INFO, FLAGS_trace_toss) << folly::sformat(
                        "addSamePartEdges: (space,localPart,remotePart)=({},{},{}), code={}",
                        spaceId_,
                        localPart,
                        remotePart,
                        apache::thrift::util::enumNameSafe(code));
                    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                        pushResultCode(code, localPart);
                    }
                }));
    }
    folly::collectAll(futures).via(env_->txnMan_->getExecutor()).thenValue([=](auto&&){
        onFinished();
    });
}

nebula::cpp2::ErrorCode
AddEdgesAtomicProcessor::encodeSingleEdgeProps(const cpp2::NewEdge& e, std::string& encodedVal) {
    auto edgeType = e.get_key().get_edge_type();
    auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
    if (!schema) {
        LOG(ERROR) << "Space " << spaceId_ << ", Edge " << edgeType << " invalid";
        return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    WriteResult wRet;
    auto& edgeProps = e.get_props();
    auto stEncodedVal = encodeRowVal(schema.get(), propNames_, edgeProps, wRet);
    if (!stEncodedVal.ok()) {
        LOG(ERROR) << stEncodedVal.status();
        return nebula::cpp2::ErrorCode::E_DATA_TYPE_MISMATCH;
    }
    encodedVal = stEncodedVal.value();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}   // namespace storage
}   // namespace nebula
