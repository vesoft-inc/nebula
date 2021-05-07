/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_ASSIGNMENTVALIDATOR_H_
#define VALIDATOR_ASSIGNMENTVALIDATOR_H_

#include "validator/Validator.h"

namespace nebula {
namespace graph {
class AssignmentValidator final : public Validator {
public:
    AssignmentValidator(Sentence* sentence, QueryContext* context)
        : Validator(sentence, context) {
        auto* assignSentence = static_cast<AssignmentSentence*>(sentence_);
        validator_ = makeValidator(assignSentence->sentence(), qctx_);

        if (validator_->noSpaceRequired()) {
            setNoSpaceRequired();
        }
    }

private:
    Status validateImpl() override;

    Status toPlan() override;

private:
    std::unique_ptr<Validator>  validator_;
    std::string                 var_;
};
}  // namespace graph
}  // namespace nebula
#endif
