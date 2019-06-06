/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
    Status status;
    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }

        auto spaceId = ectx()->rctx()->session()->space();
        overwritable_ = sentence_->overwritable();
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId, *sentence_->edge());
        if (!edgeStatus.ok()) {
            status = edgeStatus.status();
            break;
        }
        edgeType_ = edgeStatus.value();
        properties_ = sentence_->properties();
        rows_ = sentence_->rows();
        schema_ = ectx()->schemaManager()->getEdgeSchema(spaceId, edgeType_);
        if (schema_ == nullptr) {
            status = Status::Error("No schema found for '%s'", sentence_->edge()->c_str());
            break;
        }
        ACL_CHECK_SPACE(spaceId);
    } while (false);

    return status;
}


void InsertEdgeExecutor::execute() {
    std::vector<storage::cpp2::Edge> edges(rows_.size() * 2);   // inbound and outbound
    auto index = 0;
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        auto src = row->srcid();
        auto dst = row->dstid();
        auto rank = row->rank();
        auto expressions = row->values();
        std::vector<VariantType> values;

        values.reserve(expressions.size());
        for (auto *expr : expressions) {
            values.emplace_back(expr->eval());
        }

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
            auto &out = edges[index++];
            out.key.set_src(src);
            out.key.set_dst(dst);
            out.key.set_ranking(rank);
            out.key.set_edge_type(edgeType_);
            out.props = writer.encode();
            out.__isset.key = true;
            out.__isset.props = true;
        }
        {
            auto &in = edges[index++];
            in.key.set_src(dst);
            in.key.set_dst(src);
            in.key.set_ranking(rank);
            in.key.set_edge_type(-edgeType_);
            in.props = "";
            in.__isset.key = true;
            in.__isset.props = true;
        }
    }

    auto space = ectx()->rctx()->session()->space();
    auto future = ectx()->storage()->addEdges(space, std::move(edges), overwritable_);
    auto *runner = ectx()->rctx()->runner();

    auto cb = [this] (auto &&resp) {
        // For insertion, we regard partial success as failure.
        auto completeness = resp.completeness();
        if (completeness != 100) {
            DCHECK(onError_);
            onError_(Status::Error("Internal Error"));
            return;
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

    std::move(future).via(runner).thenValue(cb).thenError(error);
}

}   // namespace graph
}   // namespace nebula
