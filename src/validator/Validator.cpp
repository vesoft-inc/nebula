/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/Validator.h"
#include "parser/Sentence.h"
#include "planner/Query.h"
#include "validator/GoValidator.h"
#include "validator/PipeValidator.h"
#include "validator/ReportError.h"
#include "validator/SequentialValidator.h"
#include "validator/AssignmentValidator.h"
#include "validator/SetValidator.h"
#include "validator/UseValidator.h"
#include "validator/GetSubgraphValidator.h"

namespace nebula {
namespace graph {
std::unique_ptr<Validator> Validator::makeValidator(Sentence* sentence, ValidateContext* context) {
    CHECK(!!sentence);
    CHECK(!!context);
    auto kind = sentence->kind();
    switch (kind) {
        case Sentence::Kind::kSequential:
            return std::make_unique<SequentialValidator>(sentence, context);
        case Sentence::Kind::kGo:
            return std::make_unique<GoValidator>(sentence, context);
        case Sentence::Kind::kPipe:
            return std::make_unique<PipeValidator>(sentence, context);
        case Sentence::Kind::kAssignment:
            return std::make_unique<AssignmentValidator>(sentence, context);
        case Sentence::Kind::kSet:
            return std::make_unique<SetValidator>(sentence, context);
        case Sentence::Kind::kUse:
            return std::make_unique<UseValidator>(sentence, context);
        case Sentence::Kind::kGetSubgraph:
            return std::make_unique<GetSubgraphValidator>(sentence, context);
        default:
            return std::make_unique<ReportError>(sentence, context);
    }
}

Status Validator::appendPlan(PlanNode* node, PlanNode* appended) {
    switch (node->kind()) {
        case PlanNode::Kind::kEnd:
        case PlanNode::Kind::kFilter:
        case PlanNode::Kind::kProject:
        case PlanNode::Kind::kSort:
        case PlanNode::Kind::kLimit:
        case PlanNode::Kind::kAggregate:
        case PlanNode::Kind::kSelector:
        case PlanNode::Kind::kLoop:
        case PlanNode::Kind::kRegisterSpaceToSession: {
            static_cast<SingleInputNode*>(node)->setInput(appended);
            break;
        }
        default: {
            return Status::Error(
                    "%ld not support to append an input.", static_cast<int64_t>(node->kind()));
        }
    }
    return Status::OK();
}

Status Validator::validate() {
    Status status;

    if (!validateContext_) {
        return Status::Error("Validate context was not given.");
    }

    if (!sentence_) {
        return Status::Error("Sentence was not given");
    }

    if (!spaceChosen()) {
        status = Status::Error("Space was not chosen.");
        return status;
    }

    status = validateImpl();
    if (!status.ok()) {
        return status;
    }

    status = toPlan();
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

bool Validator::spaceChosen() {
    return validateContext_->spaceChosen();
}
}  // namespace graph
}  // namespace nebula
