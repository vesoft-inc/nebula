/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "dataman/RowReader.h"
#include "graph/DeleteVerticesExecutor.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {

DeleteVerticesExecutor::DeleteVerticesExecutor(Sentence *sentence,
                                               ExecutionContext *ectx)
    : Executor(ectx, "delete_vertices") {
    sentence_ = static_cast<DeleteVerticesSentence*>(sentence);
}

Status DeleteVerticesExecutor::prepare() {
    return Status::OK();
}

void DeleteVerticesExecutor::execute() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    space_ = ectx()->rctx()->session()->space();
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setSpace(space_);
    expCtx_->setStorageClient(ectx()->getStorageClient());
    expCtx_->setOnVariableVariantGet(onVariableVariantGet_);

    auto vidList = sentence_->vidList();
    vidList->setContext(expCtx_.get());
    status = vidList->prepare();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    auto values = vidList->eval();
    for (auto value : values) {
        auto v = value.value();
        if (!Expression::isInt(v)) {
            status = Status::Error("Vertex ID should be of type integer");
            doError(std::move(status));
            return;
        }
        auto vid = Expression::asInt(v);
        vids_.emplace_back(vid);
    }

    auto edgeAllStatus = ectx()->schemaManager()->getAllEdge(space_);
    if (!edgeAllStatus.ok()) {
        doError(edgeAllStatus.status());
        return;
    }

    std::vector<EdgeType> edgeTypes;
    std::vector<std::string> columns = {_DST, _RANK};
    std::vector<nebula::storage::cpp2::PropDef> props;
    for (auto &e : edgeAllStatus.value()) {
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(space_, e);
        if (!edgeStatus.ok()) {
            LOG(ERROR) << "Can't get all of the edge types";
            doError(edgeStatus.status());
            return;
        }

        auto type = edgeStatus.value();
        VLOG(3) << "Get Edge Type: " << type << " and " << (-1 * type);
        edgeTypes.emplace_back(type);
        edgeTypes.emplace_back(-1 * type);

        for (auto& column : columns) {
            storage::cpp2::PropDef def;
            def.owner = storage::cpp2::PropOwner::EDGE;
            def.name = column;
            def.id.set_edge_type(type);
            props.emplace_back(std::move(def));

            storage::cpp2::PropDef reverseDef;
            reverseDef.owner = storage::cpp2::PropOwner::EDGE;
            reverseDef.name = column;
            reverseDef.id.set_edge_type(-1 * type);
            props.emplace_back(std::move(reverseDef));
        }
    }

    auto future = ectx()->getStorageClient()->getNeighbors(space_, vids_, edgeTypes, "", props);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&result) {
        auto completeness = result.completeness();
        if (completeness != 100) {
            LOG(ERROR) << "Get vertices partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << " error code: " << static_cast<int>(error.second);
                doError(Status::Error("Delete edge not complete, completeness: %d",
                                      completeness));
                return;
            }
        }

        auto rpcResp = std::move(result).responses();
        std::vector<storage::cpp2::EdgeKey> allEdges;
        for (auto& response : rpcResp) {
            std::unordered_map<EdgeType, std::shared_ptr<ResultSchemaProvider>> edgeSchema;
            auto *eschema = response.get_edge_schema();
            if (eschema == nullptr || eschema->empty()) {
                continue;
            }

            std::transform(eschema->cbegin(),
                           eschema->cend(),
                           std::inserter(edgeSchema, edgeSchema.begin()),
                           [](auto &schema) {
                               return std::make_pair(
                                   schema.first,
                                   std::make_shared<ResultSchemaProvider>(schema.second));
                           });

            for (auto &vdata : response.vertices) {
                auto src = vdata.get_vertex_id();
                for (auto &edata : vdata.get_edge_data()) {
                    auto edgeType = edata.get_type();
                    auto it = edgeSchema.find(edgeType);
                    if (it == edgeSchema.end()) {
                        LOG(ERROR) << "Can't find " << edgeType;
                        doError(Status::Error("Can't find edge type %d", edgeType));
                        return;
                    }

                    for (auto& edge : edata.get_edges()) {
                        auto dst = edge.get_dst();
                        auto reader = RowReader::getRowReader(edge.get_props(), it->second);
                        if (reader == nullptr) {
                            LOG(ERROR) << "Can't get row reader";
                            doError(Status::Error("Can't get reader! edge type %d", edgeType));
                            return;
                        }

                        auto rankRes = RowReader::getPropByName(reader.get(), _RANK);
                        if (!ok(rankRes)) {
                            LOG(ERROR) << "Can't get rank " << edgeType;
                            doError(Status::Error("Can't get rank! edge type %d", edgeType));
                            return;
                        }

                        auto rank = boost::get<int64_t>(value(rankRes));
                        storage::cpp2::EdgeKey edgeKey;
                        edgeKey.set_src(src);
                        edgeKey.set_edge_type(edgeType);
                        edgeKey.set_ranking(rank);
                        edgeKey.set_dst(dst);
                        allEdges.emplace_back(std::move(edgeKey));

                        storage::cpp2::EdgeKey reverseEdgeKey;
                        reverseEdgeKey.set_src(dst);
                        reverseEdgeKey.set_edge_type(-1 * edgeType);
                        reverseEdgeKey.set_ranking(rank);
                        reverseEdgeKey.set_dst(src);
                        allEdges.emplace_back(std::move(reverseEdgeKey));
                    }
                }
            }
        }

        if (allEdges.size() > 0) {
            deleteEdges(allEdges);
        } else {
            deleteVertices();
        }
        return;
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Internal error"));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void DeleteVerticesExecutor::deleteEdges(std::vector<storage::cpp2::EdgeKey>& edges) {
    auto future = ectx()->getStorageClient()->deleteEdges(space_, edges);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness != 100) {
            LOG(ERROR) << "Delete edges partially failed: "  << completeness << "%";
            doError(Status::Error("Internal error"));
            return;
        }
        deleteVertices();
        return;
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Internal error"));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

void DeleteVerticesExecutor::deleteVertices() {
    auto future = ectx()->getStorageClient()->deleteVertices(space_, vids_);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&resp) {
        auto completeness = resp.completeness();
        if (completeness != 100) {
            LOG(ERROR) << "Delete vertices partially failed: "  << completeness << "%";
            doError(Status::Error("Internal error"));
            return;
        }
        doFinish(Executor::ProcessControl::kNext, vids_.size());
        return;
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Internal error"));
        return;
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula

