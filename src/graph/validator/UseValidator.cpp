/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/UseValidator.h"

#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move

#include "clients/meta/MetaClient.h"        // for MetaClient
#include "common/base/Logging.h"            // for LOG, LogMessage, _LOG_ERROR
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/meta/SchemaManager.h"      // for SchemaManager
#include "graph/context/QueryContext.h"     // for QueryContext
#include "graph/context/ValidateContext.h"  // for ValidateContext
#include "graph/planner/plan/Query.h"       // for SwitchSpace
#include "graph/session/ClientSession.h"    // for SpaceInfo
#include "interface/gen-cpp2/meta_types.h"  // for SpaceDesc
#include "parser/TraverseSentences.h"       // for UseSentence

namespace nebula {
namespace graph {
Status UseValidator::validateImpl() {
  auto useSentence = static_cast<UseSentence*>(sentence_);
  spaceName_ = useSentence->space();
  SpaceInfo spaceInfo;
  spaceInfo.name = *spaceName_;
  // firstly get from validate context
  if (!vctx_->hasSpace(*spaceName_)) {
    // secondly get from cache
    auto spaceId = qctx_->schemaMng()->toGraphSpaceID(*spaceName_);
    if (!spaceId.ok()) {
      LOG(ERROR) << "Unknown space: " << *spaceName_;
      return spaceId.status();
    }
    auto spaceDesc = qctx_->getMetaClient()->getSpaceDesc(spaceId.value());
    if (!spaceDesc.ok()) {
      return spaceDesc.status();
    }
    spaceInfo.id = spaceId.value();
    spaceInfo.spaceDesc = std::move(spaceDesc).value();
    vctx_->switchToSpace(std::move(spaceInfo));
    return Status::OK();
  }

  spaceInfo.id = -1;
  vctx_->switchToSpace(std::move(spaceInfo));
  return Status::OK();
}

Status UseValidator::toPlan() {
  // The input will be set by father validator later.
  auto reg = SwitchSpace::make(qctx_, nullptr, *spaceName_);
  root_ = reg;
  tail_ = root_;
  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
