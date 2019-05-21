/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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

    // TODO(dutor) To support multi-tag insertion
    auto tagItems = sentence_->tagItems();
    if (tagItems.size() > 1) {
        return Status::Error("Multi-tag not supported yet");
    }

    auto *tagName = tagItems[0]->tagName();
    properties_ = tagItems[0]->properties();

    rows_ = sentence_->rows();
    // TODO(dutor) To check whether the number of props and values matches.
    if (rows_.empty()) {
        return Status::Error("VALUES cannot be empty");
    }

    overwritable_ = sentence_->overwritable();

    auto spaceId = ectx()->rctx()->session()->space();
    tagId_ = ectx()->schemaManager()->toTagID(spaceId, *tagName);
    schema_ = ectx()->schemaManager()->getTagSchema(spaceId, tagId_);
    if (schema_ == nullptr) {
        return Status::Error("No schema found for `%s'", tagName->c_str());
    }
    return Status::OK();
}


void InsertVertexExecutor::execute() {
    using storage::cpp2::Vertex;
    using storage::cpp2::Tag;
    using storage::cpp2::PropValue;

    std::vector<Vertex> vertices(rows_.size());
    for (auto i = 0u; i < rows_.size(); i++) {
        auto *row = rows_[i];
        auto id = row->id();
        auto expressions = row->values();
        std::vector<std::string> names;
        std::vector<PropValue> values;

        values.reserve(expressions.size());
        for (auto *expr : expressions) {
            auto value = expr->eval();
            PropValue propValue;
            switch (value.which()) {
                case 0:
                    propValue.set_int_val(boost::get<int64_t>(value));
                    values.emplace_back(std::move(propValue));
                    break;
                case 1:
                    propValue.set_double_val(boost::get<double>(value));
                    values.emplace_back(std::move(propValue));
                    break;
                case 2:
                    propValue.set_bool_val(boost::get<bool>(value));
                    values.emplace_back(std::move(propValue));
                    break;
                case 3:
                    propValue.set_string_val(boost::get<std::string>(value));
                    values.emplace_back(std::move(propValue));
                    break;
                default:
                    LOG(FATAL) << "Unknown value type: " << static_cast<uint32_t>(value.which());
                    onError_(Status::Error("Unknown value type"));
                    return;
            }
        }

        auto &vertex = vertices[i];
        std::vector<Tag> tags(1);
        auto &tag = tags[0];
        tag.set_tag_id(tagId_);
        for (auto property : properties_) {
            names.emplace_back(*property);
        }
        tag.set_props_name(names);
        tag.set_props_value(values);
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
