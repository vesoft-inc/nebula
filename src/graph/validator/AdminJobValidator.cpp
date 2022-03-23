/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/AdminJobValidator.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

Status AdminJobValidator::validateImpl() {
  if (sentence_->getJobType() == meta::cpp2::JobType::DATA_BALANCE ||
      sentence_->getJobType() == meta::cpp2::JobType::ZONE_BALANCE) {
    return Status::SemanticError("Data balance not support");
  }

  // Note: The last parameter of paras is no longer spacename
  if (sentence_->getOp() == meta::cpp2::JobOp::ADD) {
    auto jobType = sentence_->getJobType();
    if (requireSpace()) {
      const auto &spaceInfo = vctx_->whichSpace();
      auto spaceId = spaceInfo.id;
      const auto &spaceName = spaceInfo.name;

      if (jobType == meta::cpp2::JobType::REBUILD_TAG_INDEX ||
          jobType == meta::cpp2::JobType::REBUILD_EDGE_INDEX) {
        auto ret = jobType == meta::cpp2::JobType::REBUILD_TAG_INDEX
                       ? qctx()->indexMng()->getTagIndexes(spaceId)
                       : qctx()->indexMng()->getEdgeIndexes(spaceId);
        if (!ret.ok()) {
          return Status::SemanticError("Get index failed in space `%s': %s",
                                       spaceName.c_str(),
                                       ret.status().toString().c_str());
        }
        auto indexes = std::move(ret).value();
        const auto &paras = sentence_->getParas();
        if (paras.empty() && indexes.empty()) {
          return Status::SemanticError("Space `%s' without indexes", spaceName.c_str());
        }
        for (auto i = 0u; i < paras.size(); i++) {
          const auto &indexName = paras[i];
          auto it = std::find_if(indexes.begin(),
                                 indexes.end(),
                                 [&indexName](std::shared_ptr<meta::cpp2::IndexItem> &item) {
                                   return item->get_index_name() == indexName;
                                 });
          if (it == indexes.end()) {
            return Status::SemanticError(
                "Index %s not found in space %s", indexName.c_str(), spaceName.c_str());
          }
        }
      }
    }
  }
  return Status::OK();
}

Status AdminJobValidator::toPlan() {
  auto *doNode = SubmitJob::make(
      qctx_, nullptr, sentence_->getOp(), sentence_->getJobType(), sentence_->getParas());
  root_ = doNode;
  tail_ = root_;
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
