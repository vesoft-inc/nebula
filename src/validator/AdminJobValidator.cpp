/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/AdminJobValidator.h"
#include "planner/Admin.h"

namespace nebula {
namespace graph {

Status AdminJobValidator::validateImpl() {
    if (sentence_->getOp() == meta::cpp2::AdminJobOp::ADD) {
        auto cmd = sentence_->getCmd();
        if (requireSpace()) {
            const auto &spaceInfo = qctx()->rctx()->session()->space();
            auto spaceId = spaceInfo.id;
            const auto &spaceName = spaceInfo.name;
            sentence_->addPara(spaceName);

            if (cmd == meta::cpp2::AdminCmd::REBUILD_TAG_INDEX ||
                cmd == meta::cpp2::AdminCmd::REBUILD_EDGE_INDEX) {
                DCHECK_EQ(sentence_->getParas().size(), 2);

                const auto &indexName = sentence_->getParas()[0];
                auto ret = cmd == meta::cpp2::AdminCmd::REBUILD_TAG_INDEX
                               ? qctx()->getMetaClient()->getTagIndexesFromCache(spaceId)
                               : qctx()->getMetaClient()->getEdgeIndexesFromCache(spaceId);
                if (!ret.ok()) {
                    return Status::SemanticError(
                        "Index %s not found in space %s", indexName.c_str(), spaceName.c_str());
                }
                auto indexes = std::move(ret).value();
                auto it = std::find_if(indexes.begin(),
                                       indexes.end(),
                                       [&indexName](std::shared_ptr<meta::cpp2::IndexItem>& item) {
                                           return item->get_index_name() == indexName;
                                       });
                if (it == indexes.end()) {
                    return Status::SemanticError(
                        "Index %s not found in space %s", indexName.c_str(), spaceName.c_str());
                }
            }
        }
    }

    return Status::OK();
}

Status AdminJobValidator::toPlan() {
    auto* doNode = SubmitJob::make(
        qctx_, nullptr, sentence_->getOp(), sentence_->getCmd(), sentence_->getParas());
    root_ = doNode;
    tail_ = root_;
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
