/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/InsertVertexExecutor.h"
#include "storage/client/StorageClient.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace graph {

InsertVertexExecutor::InsertVertexExecutor(Sentence *sentence,
                                           ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<InsertVertexSentence*>(sentence);
}


Status InsertVertexExecutor::prepare() {
    auto status = checkIfGraphSpaceChosen();
    if (!status.ok()) {
        return status;
    }

    overwritable_ = sentence_->overwritable();
    vertex_ = sentence_->vertex();
    tagId_ = ectx()->schemaManager()->toTagID(*vertex_);
    properties_ = sentence_->properties();
    rows_ = sentence_->rows();
    // TODO(dutor) check on property names and types
    if (rows_.empty()) {
        return Status::Error("VALUES cannot be empty");
    }
    auto space = ectx()->rctx()->session()->space();
    schema_ = ectx()->schemaManager()->getTagSchema(space, tagId_);
    if (schema_ == nullptr) {
        return Status::Error("No schema found for `%s'", vertex_->c_str());
    }
    return Status::OK();
}


void InsertVertexExecutor::execute() {
    using storage::cpp2::Vertex;
    using storage::cpp2::Tag;

    std::vector<Vertex> vertices(rows_.size());
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        auto id = row->id();
        auto expressions = row->values();
        std::vector<VariantType> values;

        values.reserve(expressions.size());
        for (auto *expr : expressions) {
            values.emplace_back(expr->eval());
        }

        auto &vertex = vertices[i];
        std::vector<Tag> tags(1);
        auto &tag = tags[0];
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

        tag.set_tag_id(tagId_);
        tag.set_props(writer.encode());
        vertex.set_id(id);
        vertex.set_tags(std::move(tags));
    }

    auto space = ectx()->rctx()->session()->space();
    auto future = ectx()->storage()->addVertices(space, std::move(vertices), overwritable_);
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
