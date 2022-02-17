/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/ExplainValidator.h"

#include <folly/String.h>  // for join

#include <algorithm>    // for find, transform
#include <cctype>       // for tolower
#include <string>       // for string, basic_string
#include <type_traits>  // for remove_reference<>:...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/Logging.h"                  // for GetReferenceableValue
#include "common/base/StatusOr.h"                 // for StatusOr
#include "graph/context/QueryContext.h"           // for QueryContext
#include "graph/context/Symbols.h"                // for ColsDef
#include "graph/planner/plan/ExecutionPlan.h"     // for ExecutionPlan
#include "graph/planner/plan/PlanNode.h"          // for operator<<, PlanNode
#include "graph/validator/SequentialValidator.h"  // for SequentialValidator
#include "parser/ExplainSentence.h"               // for ExplainSentence
#include "parser/Sentence.h"                      // for Sentence, Sentence:...

namespace nebula {
namespace graph {

static const std::vector<std::string> kAllowedFmtType = {"row", "dot", "dot:struct"};

ExplainValidator::ExplainValidator(Sentence* sentence, QueryContext* context)
    : Validator(sentence, context) {
  DCHECK_EQ(sentence->kind(), Sentence::Kind::kExplain);
  auto explain = static_cast<ExplainSentence*>(sentence_);
  auto sentences = explain->seqSentences();
  validator_ = std::make_unique<SequentialValidator>(sentences, qctx_);
  if (validator_->noSpaceRequired()) {
    setNoSpaceRequired();
  }
}

static StatusOr<std::string> toExplainFormatType(const std::string& formatType) {
  if (formatType.empty()) {
    return kAllowedFmtType.front();
  }

  std::string fmtType = formatType;
  std::transform(formatType.cbegin(), formatType.cend(), fmtType.begin(), [](char c) {
    return std::tolower(c);
  });

  auto found = std::find(kAllowedFmtType.cbegin(), kAllowedFmtType.cend(), fmtType);
  if (found != kAllowedFmtType.cend()) {
    return fmtType;
  }
  auto allowedStr = folly::join(",", kAllowedFmtType);
  return Status::SyntaxError(
      "Invalid explain/profile format type: \"%s\", only following values are "
      "supported: %s",
      fmtType.c_str(),
      allowedStr.c_str());
}

Status ExplainValidator::validateImpl() {
  auto explain = static_cast<ExplainSentence*>(sentence_);

  auto status = toExplainFormatType(explain->formatType());
  NG_RETURN_IF_ERROR(status);
  qctx_->plan()->setExplainFormat(std::move(status).value());

  NG_RETURN_IF_ERROR(validator_->validate());

  outputs_ = validator_->outputCols();
  return Status::OK();
}

Status ExplainValidator::toPlan() {
  // The execution plan has been generated in validateImpl function
  root_ = validator_->root();
  tail_ = validator_->tail();
  VLOG(1) << "root: " << root_->kind() << " tail: " << tail_->kind();
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
