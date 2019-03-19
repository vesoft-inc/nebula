/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/InsertEdgeExecutor.h"
#include "meta/SchemaManager.h"
#include "storage/client/StorageClient.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace graph {

InsertEdgeExecutor::InsertEdgeExecutor(Sentence *sentence,
                                       ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<InsertEdgeSentence*>(sentence);
}


Status InsertEdgeExecutor::prepare() {
    overwritable_ = sentence_->overwritable();
    edge_ = meta::SchemaManager::toEdgeType(*sentence_->edge());
    properties_ = sentence_->properties();
    rows_ = sentence_->rows();
    auto space = ectx()->rctx()->session()->space();
    schema_ = meta::SchemaManager::getEdgeSchema(space, edge_);
    if (schema_ == nullptr) {
        return Status::Error("No schema found for `%s'", sentence_->edge()->c_str());
    }
    return Status::OK();
}


void InsertEdgeExecutor::execute() {
    std::vector<storage::cpp2::Edge> outwards(rows_.size());
    std::vector<storage::cpp2::Edge> inwards(rows_.size());
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        auto src = row->srcid();
        auto dst = row->dstid();
        auto rank = row->rank();
        auto exprs = row->values();
        std::vector<VariantType> values;
        values.resize(exprs.size());
        auto eval = [] (auto *expr) { return expr->eval(); };
        std::transform(exprs.begin(), exprs.end(), values.begin(), eval);

        RowWriter writer(schema_);
        for (auto &value : values) {
            switch (value.which()) {
                case 0:
                    writer << boost::get<int64_t>(value);
                    break;
                case 1:
                    writer << boost::get<double>(value);
                    break;
                case 2:
                    writer << boost::get<bool>(value);
                    break;
                case 3:
                    writer << boost::get<std::string>(value);
                    break;
                default:
                    LOG(FATAL) << "Unknown value type: " << static_cast<uint32_t>(value.which());
            }
        }
        {
            auto &edge = outwards[i];
            edge.key.set_src(src);
            edge.key.set_dst(dst);
            edge.key.set_ranking(rank);
            edge.key.set_edge_type(edge_);
            edge.props = writer.encode();
            edge.__isset.key = true;
            edge.__isset.props = true;
        }
        {
            auto &edge = inwards[i];
            edge.key.set_src(dst);
            edge.key.set_dst(src);
            edge.key.set_ranking(rank);
            edge.key.set_edge_type(-edge_);
            edge.props = "";
            edge.__isset.key = true;
            edge.__isset.props = true;
        }
    }

    auto space = ectx()->rctx()->session()->space();
    using storage::cpp2::ExecResponse;
    std::array<folly::SemiFuture<storage::StorageRpcResponse<ExecResponse>>, 2> both = {
        ectx()->storage()->addEdges(space, std::move(outwards), overwritable_),
        ectx()->storage()->addEdges(space, std::move(inwards), overwritable_)
    };

    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&results) {
        // For insertion, we regard partial success as failure.
        for (auto &result : results) {
            if (result.hasException()) {
                LOG(ERROR) << "Insert edge failed: " << result.exception().what();
                onError_(Status::Error("Internal Error"));
                return;
            }
            auto resp = std::move(result).value();
            auto completeness = resp.completeness();
            if (completeness != 100) {
                DCHECK(onError_);
                onError_(Status::Error("Internal Error"));
                return;
            }
        }
        DCHECK(onFinish_);
        onFinish_();
    };

    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        DCHECK(onError_);
        onError_(Status::Error("Internal error"));
        return;
    };

    auto future = folly::collectAll(both);

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
