/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/AdminJobValidator.h"
#include "planner/plan/Admin.h"

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
                auto ret = cmd == meta::cpp2::AdminCmd::REBUILD_TAG_INDEX
                           ? qctx()->indexMng()->getTagIndexes(spaceId)
                           : qctx()->indexMng()->getEdgeIndexes(spaceId);
                if (!ret.ok()) {
                    return Status::SemanticError("Get index failed in space `%s': %s",
                            spaceName.c_str(), ret.status().toString().c_str());
                }
                auto indexes = std::move(ret).value();
                const auto &paras = sentence_->getParas();
                if (paras.size() == 1 && indexes.empty()) {
                    return Status::SemanticError("Space `%s' without indexes", spaceName.c_str());
                }
                for (auto i = 0u; i < paras.size() - 1; i++) {
                    const auto &indexName = paras[i];
                    auto it = std::find_if(indexes.begin(), indexes.end(),
                            [&indexName](std::shared_ptr<meta::cpp2::IndexItem>& item) {
                                return item->get_index_name() == indexName;
                            });
                    if (it == indexes.end()) {
                        return Status::SemanticError(
                                "Index %s not found in space %s",
                                indexName.c_str(), spaceName.c_str());
                    }
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
