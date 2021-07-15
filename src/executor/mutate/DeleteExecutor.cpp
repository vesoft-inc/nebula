/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "DeleteExecutor.h"
#include "planner/plan/Mutate.h"
#include "context/QueryContext.h"
#include "util/SchemaUtil.h"
#include "executor/mutate/DeleteExecutor.h"
#include "util/ScopedTimer.h"


namespace nebula {
namespace graph {

folly::Future<Status> DeleteVerticesExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return deleteVertices();
}

folly::Future<Status> DeleteVerticesExecutor::deleteVertices() {
    auto *dvNode = asNode<DeleteVertices>(node());
    auto vidRef = dvNode->getVidRef();
    std::vector<Value> vertices;
    const auto& spaceInfo = qctx()->rctx()->session()->space();
    if (vidRef != nullptr) {
        auto inputVar = dvNode->inputVar();
        // empty inputVar means using pipe and need to get the GetNeighbors's inputVar
        if (inputVar.empty()) {
            DCHECK(dvNode->dep() != nullptr);
            auto* gn = static_cast<const SingleInputNode*>(dvNode->dep())->dep();
            DCHECK(gn != nullptr);
            inputVar = static_cast<const SingleInputNode*>(gn)->inputVar();
        }
        DCHECK(!inputVar.empty());
        VLOG(2) << "inputVar: " << inputVar;
        auto& inputResult = ectx_->getResult(inputVar);
        auto iter = inputResult.iter();
        vertices.reserve(iter->size());
        QueryExpressionContext ctx(ectx_);
        for (; iter->valid(); iter->next()) {
            auto val = Expression::eval(vidRef, ctx(iter.get()));
            if (val.isNull() || val.empty()) {
                VLOG(3) << "NULL or EMPTY vid";
                continue;
            }
            if (!SchemaUtil::isValidVid(val, *spaceInfo.spaceDesc.vid_type_ref())) {
                std::stringstream ss;
                ss << "Wrong vid type `" << val.type() << "', value `" << val.toString() << "'";
                return Status::Error(ss.str());
            }
            vertices.emplace_back(std::move(val));
        }
    }

    if (vertices.empty()) {
        return Status::OK();
    }
    auto spaceId = spaceInfo.id;
    time::Duration deleteVertTime;
    return qctx()->getStorageClient()->deleteVertices(spaceId, std::move(vertices))
        .via(runner())
        .ensure([deleteVertTime]() {
            VLOG(1) << "Delete vertices time: " << deleteVertTime.elapsedInUSec() << "us";
        })
        .thenValue([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
            SCOPED_TIMER(&execTime_);
            NG_RETURN_IF_ERROR(handleCompleteness(resp, false));
            return Status::OK();
        });
}

folly::Future<Status> DeleteEdgesExecutor::execute() {
    return deleteEdges();
}

folly::Future<Status> DeleteEdgesExecutor::deleteEdges() {
    SCOPED_TIMER(&execTime_);

    auto *deNode = asNode<DeleteEdges>(node());
    auto *edgeKeyRef = DCHECK_NOTNULL(deNode->edgeKeyRef());
    std::vector<storage::cpp2::EdgeKey> edgeKeys;
    const auto& spaceInfo = qctx()->rctx()->session()->space();
    auto inputVar = deNode->inputVar();
    DCHECK(!inputVar.empty());
    auto& inputResult = ectx_->getResult(inputVar);
    auto iter = inputResult.iter();
    edgeKeys.reserve(iter->size());
    QueryExpressionContext ctx(ectx_);
    for (; iter->valid(); iter->next()) {
        storage::cpp2::EdgeKey edgeKey;
        auto srcId = Expression::eval(edgeKeyRef->srcid(), ctx(iter.get()));
        if (srcId.isNull() || srcId.empty()) {
            VLOG(3) << "NULL or EMPTY vid";
            continue;
        }
        if (!SchemaUtil::isValidVid(srcId, *spaceInfo.spaceDesc.vid_type_ref())) {
            std::stringstream ss;
            ss << "Wrong srcId type `" << srcId.type()
                << "`, value `" << srcId.toString() << "'";
            return Status::Error(ss.str());
        }
        auto dstId = Expression::eval(edgeKeyRef->dstid(), ctx(iter.get()));
        if (!SchemaUtil::isValidVid(dstId, *spaceInfo.spaceDesc.vid_type_ref())) {
            std::stringstream ss;
            ss << "Wrong dstId type `" << dstId.type()
                << "', value `" << dstId.toString() << "'";
            return Status::Error(ss.str());
        }
        auto rank = Expression::eval(edgeKeyRef->rank(), ctx(iter.get()));
        if (!rank.isInt()) {
            std::stringstream ss;
            ss << "Wrong rank type `" << rank.type()
                << "', value `" << rank.toString() << "'";
            return Status::Error(ss.str());
        }
        DCHECK(edgeKeyRef->type());
        auto type = Expression::eval(edgeKeyRef->type(), ctx(iter.get()));
        if (!type.isInt()) {
            std::stringstream ss;
            ss << "Wrong edge type `" << type.type()
                << "', value `" << type.toString() << "'";
            return Status::Error(ss.str());
        }

        // out edge
        edgeKey.set_src(srcId);
        edgeKey.set_dst(dstId);
        edgeKey.set_ranking(rank.getInt());
        edgeKey.set_edge_type(type.getInt());
        edgeKeys.emplace_back(edgeKey);

        // in edge
        edgeKey.set_src(std::move(dstId));
        edgeKey.set_dst(std::move(srcId));
        edgeKey.set_edge_type(-type.getInt());
        edgeKeys.emplace_back(std::move(edgeKey));
    }

    if (edgeKeys.empty()) {
        VLOG(2) << "Empty edgeKeys";
        return Status::OK();
    }

    auto spaceId = spaceInfo.id;
    time::Duration deleteEdgeTime;
    return qctx()->getStorageClient()->deleteEdges(spaceId, std::move(edgeKeys))
            .via(runner())
            .ensure([deleteEdgeTime]() {
                VLOG(1) << "Delete edge time: " << deleteEdgeTime.elapsedInUSec() << "us";
            })
            .thenValue([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
                SCOPED_TIMER(&execTime_);
                NG_RETURN_IF_ERROR(handleCompleteness(resp, false));
                return Status::OK();
            });
}
}   // namespace graph
}   // namespace nebula
